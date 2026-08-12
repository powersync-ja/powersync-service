# V3 Compaction Design

This document explains the decisions behind the V3 MongoDB bucket compactor. It accompanies [the compacting plan](v3-compact-plan.md); the plan states the intended behaviour, while this document explains how the implementation realizes it and the tradeoffs involved.

## Goals

V3 compaction is designed to make background work proportional to modified buckets and modified bucket data, rather than to the entire replication stream. It must support frequent execution, resumption after interruption, concurrent workers, and a bounded interval between full compactions.

The design intentionally does not support the earlier V3 bucket-state format. V3 has not been deployed with that format, so accepting or repairing it would add a second state model and make the normal path less clear.

## Bucket-state ownership

`BucketStateDocumentV3` separates three kinds of information:

1. `bucket_stats` is the current aggregate state of persisted bucket chunks.
2. `compacted_state` is a cache for the prefix covered by the latest lite or full compact.
3. `last_full_compact` records the last full compaction for scheduling heuristics.

Replication writers own `last_op` and `bucket_stats`. Compaction owns the compact caches and scheduling state. This division avoids making writers calculate compaction-specific estimates.

Each writer flush atomically:

- advances `last_op`;
- adds operation count, exact serialized chunk bytes, and chunk count to `bucket_stats`;
- sets `first_uncompacted_write` only if it was absent; and
- sets `next_compact_check` to the earlier of its existing value and the requested lite-check time.

The byte counter uses persisted serialized chunk sizes, rather than an operation-size estimate. Full and chunk compaction also derive their byte totals from persisted chunk metadata, so both sides of the compaction delta use the same unit.

`first_uncompacted_write` is the oldest change which has not received a full compact. It is deliberately independent of `compacted_state`: chunk compaction can reduce chunk fragmentation without declaring changes fully compacted.

## Scheduling work

The `next_compact_check` partial index is the work queue. Scheduled workers select only state rows whose check time is due. This replaces scanning state rows for dirty-count estimates.

A compaction run captures a server-time job start. It claims only rows whose check time was due at or before that start. Consequently, a long “run until completion” job does not continually rediscover work that it scheduled itself during the same run. A later job processes that work.

Claiming one bucket is an atomic `findOneAndUpdate` which requires either no lease or an expired lease. It writes a worker id and server-time expiry. A claim is therefore both a unit of work distribution and the snapshot boundary for the bucket:

- `S` is the server time when the lease was acquired;
- `L` is the bucket `last_op` returned by that atomic claim; and
- the compactor records `C`, the greatest operation it actually covered.

Those values are carried through the compaction call chain as a per-bucket compaction context. A dedicated `CompactionLease` owns the atomic claim result, server-time renewal, owner-fenced finalization/rescheduling, and release. Each claim is immediately bound with `await using`, so every path following a successful claim releases it when the scope ends. It is not mutable compactor-instance state, which keeps the lease, op cap, snapshot, finalization, and renewal logic explicitly tied to the claim that created them.

If a claimed bucket is not yet eligible for either kind of compact, it is rescheduled atomically and its lease is removed. If no chunks exist beyond `compacted_state`, there is no possible chunk-compaction work, so it is scheduled directly for its calculated full-compact check rather than polled at the chunk-compaction interval.

## Choosing full versus chunk compaction

Full compaction is chosen when either condition holds:

1. `first_uncompacted_write` has reached the maximum full-compaction age.
2. The elapsed age multiplied by the uncompacted operation ratio reaches the minimum full-compaction interval.

The sliding rule prevents an isolated small write from causing a large full compact immediately, while allowing a sufficiently large update burst to compact before the maximum interval.

Chunk compaction is chosen when new chunks exist after `compacted_state` and the chunk-compaction interval for that bucket has elapsed. It is intentionally a chunk-layout and checksum-cache operation, not a full logical rewrite.

The relevant intervals are options with V3 defaults:

| Setting                     |    Default | Purpose                                          |
| --------------------------- | ---------: | ------------------------------------------------ |
| `minCompactChunkIntervalMs` |  5 minutes | Avoid frequent tiny tail checks.                 |
| `minCompactFullIntervalMs`  |    2 hours | Controls sliding-scale full-compaction pressure. |
| `maxCompactFullIntervalMs`  |     7 days | Bounds data retention / full-compaction age.     |
| `compactLeaseDurationMs`    | 10 minutes | Lets another worker recover abandoned work.      |

These are operational policy choices rather than correctness constants. They should be tuned from workload and backlog metrics.

When a bucket has no lite work, its full check is scheduled one minute after the calculated eligibility time. This small late margin prevents a worker using a slightly earlier clock from waking before the exact full-compaction condition is true and repeatedly rescheduling the same bucket. It may delay a full compact by up to that margin.

## Chunk compaction

Chunk compaction reads the previous compacted boundary chunk and any later chunks. Including that one previous chunk matters because it may now merge with newly appended data. It does not rescan the older compacted prefix.

The chunk-compaction metadata scan reads chunk fields only. Chunk payloads are hydrated only when a group can actually merge. The checksum cache for the unchanged prefix is seeded from `compacted_state`; the compactor combines that cache with metadata from the processed tail rather than recalculating the whole bucket checksum.

`compactChunksOnly` forces this operation. `compactInitialReplication()` reuses scheduled selection, extending the fixed job-start boundary by one chunk-compaction interval so it includes writes that existed when the pass began, and forces the selected work to chunk rather than full compaction. The fixed boundary means it does not chase writes that arrive while it runs. Scheduled chunk compactions running during initial replication therefore reduce the later pass to the remaining scheduled work. The lease normally prevents the two paths from duplicating work for a bucket; owner-fenced finalization and concurrent-safe operations remain necessary if a lease expires.

## Full compaction

Full compaction scans bucket data backwards through the claimed upper boundary, rewrites superseded operations to MOVE operations where appropriate, performs CLEAR reduction, and may merge resulting chunks. It is bounded by the claimed head and any caller-specified `maxOpId` safe buffer.

The caller cap is never widened: the compactor uses the lower of the claimed `last_op` and the requested maximum. A full compact which cannot cover all operations present at claim time is therefore treated as partial; it must not clear the bucket’s full-compaction debt.

## Aggregate-statistics delta

Compaction must not hold a long transaction on `bucket_state`, because replication writes need to continue. Instead it applies the stat correction at finalization.

At compaction start, let `A` be the aggregate stats for the data being compacted. Let `B` be the corresponding stats after compaction. Replication may concurrently add new chunks, but the final update atomically increments current aggregate state by `B - A`.

This has the desired result whether or not replication wrote during the compact:

- with no concurrent writes, current stats become `B`;
- with concurrent writes, their increments remain and the compacted region is corrected from `A` to `B`.

Chunk compaction calculates `A` and `B` only for its compacted tail plus its single overlap chunk. Full compaction calculates them only for the compacted prefix. Neither needs to rescan a tail beyond `C` merely to update aggregate counters.

## Finalization and concurrent writes

Finalization is one lease-fenced update. It updates cache/stat state, decides scheduling state using the current `last_op` in the same update, and removes the lease.

For a full compact where `C >= L`:

- if current `last_op == L`, there were no concurrent writes, so `first_uncompacted_write` and `next_compact_check` are cleared;
- if current `last_op > L`, writes arrived after `S`, so `first_uncompacted_write` becomes `S` and the next check is `S + minCompactChunkInterval`.

Using `S` can schedule slightly earlier than the actual write time, but it cannot postpone the maximum full-compaction deadline.

For a partial full compact (`C < L`) and for every chunk compact, the original `first_uncompacted_write` is retained. Replacing it with `S` would delay work that was already waiting for a full compact.

## Lease renewal and failure

Long compactions renew the lease with server time. Both renewal and finalization require the same worker id. A worker that loses its lease raises a non-retryable lease-loss error: retrying the same compactor instance would still hold a stale lease identity and could race the new owner.

Transactional replacement conflicts are different. They are retryable because a retry starts the relevant bucket work again, while the lease still belongs to that worker.

## Legacy separation

`MongoCompactorV1` retains V1’s dirty-estimate model, its minimum-change settings, and its legacy checksum-update logic. `MongoCompactorV3` does not inherit those paths:

- V1 owns the legacy `compact()` flow and dirty-bucket discovery.
- V3 owns its own `compact()` flow, scheduled selection, leases, and initial-replication lite path.
- The shared base provides only common configuration, retry handling, and checkpoint-request cleanup.

This separation prevents a V3 change from accidentally using an estimate-based fallback and makes the intended state model visible at the class boundary.

## Deliberate non-goals

- No read, conversion, or repair path exists for an older V3 bucket-state document.
- V3 bucket-data compaction does not use `estimate_since_compact`, `minBucketChanges`, `minChangeRatio`, or dirty-bucket scanning.
- Parameter compaction is separate from V3 bucket-data compaction and currently has no per-collection lease. The V3 compaction lease applies only to bucket-data work.
