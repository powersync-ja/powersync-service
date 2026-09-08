# MongoDB replication throughput proposal

Status: architectural direction. Target: MongoDB source → MongoDB metadata storage + S3 payload storage.

## Objective and constraints

Aim for **30 MB/s through durable checkpoints at both low S3 latency and 100 ms added S3 latency**, with concurrent table snapshots and controlled MongoDB IOPS.

Below saturation, prioritize source-to-checkpoint latency. Preserve the existing marker-driven throttled batching: small, prompt flushes when caught up, and naturally larger batches when durable replication falls behind.

Keep the existing storage structure, S3 object format, placement policy, and client protocol. Write offloaded payloads directly to S3. Compaction remains responsible for its existing work; replication does not stage those payloads in MongoDB.

**Sequence-allocation semantics may change:** operation IDs must remain unique and strictly increase between successive data commits. Different source tables within one commit need no particular relative order. Gaps are allowed. Preserve ordering for dependent changes and the existing sorted per-bucket representation.

The reported baseline is 15 MB/s with low-latency storage and 3 MB/s with added S3 latency. This proposal is based on code inspection; its performance has not been measured.

## Where throughput is lost

The current writer largely executes:

```text
Read → convert → look up memberships → evaluate → upload → write metadata → repeat
```

Input and transaction limits force frequent flushes. Each flush forms separate objects per bucket, so a batch spread across many buckets produces small objects and multiple request waves. Uploads already run concurrently within a flush, but all must finish before subsequent MongoDB writes. S3 waits occur inside the transaction holding the operation-sequence lock.

MongoDB source replication already avoids storing full current rows, but still reads memberships and unconditionally queues membership upserts. Frequent flushes also repeat bucket-state and transaction bookkeeping.

Code anchors: [MongoBucketBatch](../../modules/module-mongodb-storage/src/storage/implementation/MongoBucketBatch.ts), [PersistedBatchV3](../../modules/module-mongodb-storage/src/storage/implementation/v3/PersistedBatchV3.ts), and [S3ObjectStorage](../../modules/module-mongodb-storage/src/storage/implementation/v3/object-storage/S3ObjectStorage.ts).

## Recommended model: concurrent preparation and uploads, ordered commits

Use a bounded pipeline with explicit commit groups. **Run conversion, evaluation, membership loading/reconciliation, serialization, and uploads concurrently wherever their dependencies permit. Reserve IDs before upload and publish groups in reservation order.** The coordinator schedules work and establishes ordering; it does not execute all preparation on one thread. Only allocation ordering, dependency barriers, and the short MongoDB publication transactions remain globally coordinated.

```text
Snapshot table A ─┐                      ┌→ conversion / evaluation workers ─┐
Snapshot table B ─┼→ bounded dispatch ───┤                                    ├→ reconcile by source key
CDC ─────────────┘                      └→ concurrent membership reads ─────┘             ↓
                                                                          operation plans / counts
                                                                                         ↓
                                                                           group / reserve ranges
                                                                                         ↓
                                                                           parallel bucket packing
                                                                                         ↓
                                                                          concurrent S3 upload groups
                                                                                         ↓
                                                                         short ordered MongoDB publish
                                                                                         ↓
                                                                          receipts / safe checkpoints
```

S3 latency now consumes in-flight capacity instead of time holding a MongoDB transaction and sequence lock. One group's uploads can overlap another group's publication and the preparation of later groups.

### Concurrency by stage

Concurrency should exist **within a group and across groups**, rather than only fetching the next source page while one serial writer does everything else.

| Stage                                | Proposed concurrency                                                                  | Required ordering                                                                 |
| ------------------------------------ | ------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Source capture                       | Multiple table snapshot cursors; one ordered CDC reader                               | CDC event order and source boundaries                                             |
| Conversion and query evaluation      | Bounded CPU worker pool over blocks of rows                                           | Results carry input positions and config identity; completion order is irrelevant |
| Membership loading                   | Bounded MongoDB reads across tables/key blocks, overlapping CPU work                  | Reads feed the owning state partition; they cannot overwrite newer prepared state |
| Membership reconciliation            | Independent source-table/key partitions run concurrently                              | Changes touching the same key, including snapshot and CDC, serialize              |
| ID assignment                        | Coordinator reserves group ranges; workers assign local offsets in disjoint subranges | Group publication order and dependent-operation order                             |
| Payload packing                      | Workers encode/merge different bucket chunks concurrently                             | Increasing IDs and valid non-overlapping chunk ranges within each bucket          |
| Lifecycle registration and S3 upload | Bounded independent group/chunk I/O                                                   | Each object's marker must be durable before PUT; publication awaits its uploads   |
| MongoDB write-plan construction      | Workers prepare collection-specific bulk operations and accounting deltas             | Merge shared-key updates correctly before publication                             |
| MongoDB publication and checkpoints  | One ordered publisher                                                                 | Atomic group visibility, closed `last_persisted_op` prefix, source barriers       |

#### Separate pure computation from state transitions

For this MongoDB source, the complete post-image allows data-query and parameter-query evaluation before previous memberships are known. [The current evaluator](../../packages/sync-rules/src/HydratedSyncConfig.ts) consumes the row and selected definitions. [Bucket operation construction](../../modules/module-mongodb-storage/src/storage/implementation/common/PersistedBatch.ts) JSON-encodes the result and computes its PUT checksum before allocating the operation ID. Those CPU-heavy steps can move into workers; they need not wait for either membership lookup or range reservation.

Run membership loading in parallel with that computation. Reconciliation then combines evaluated results with prior state to determine removals, parameter changes, and the new membership state. Snapshot rows still require the existence check before their result is accepted; speculative evaluation may be discarded. Deletes primarily use the membership path. Sources requiring partial-row reconstruction would need different dependencies, so do not impose this MongoDB-specific fast path on every connector.

Use a small shared CPU pool, not one OS thread per source table or per pipeline stage. Each worker owns its evaluator and conversion scratch state; the current evaluator has mutable caches. Combine conversion, evaluation, JSON encoding, and checksumming in one task where possible, and pass bounded blocks/buffers rather than copying full row graphs at every stage. Worker isolation and initialization require implementation work, not just wrapping synchronous calls in `Promise.all`.

Small CDC batches must dispatch immediately. They can use the same computation functions inline when a worker round trip would cost more than the work itself. Markers close dispatch blocks; workers never wait for a minimum task size. Preserve ordered delivery of replication event callbacks, custom-checkpoint collection, and other effects—pure query evaluation is parallelizable, arbitrary callbacks are not automatically so.

#### Partition state without serializing every table

Assign each persisted source table's membership state to a logical owner, initially one ordered queue per table. Snapshot and CDC work for that table use the same owner. Owners for different tables can hydrate and reconcile concurrently. A large table can later be partitioned by source key; an identity change must coordinate both its old and new keys. Table/config changes remain barriers over the affected partitions.

Ownership covers committed state plus earlier prepared changes. A second change to a key waits for its predecessor's **reconciliation**, not its S3 upload or MongoDB commit. It can therefore prepare the next group while the preceding group uploads. Preserve an undo/dependency boundary so aborting a group invalidates its dependent suffix. Keep the same selected logical order through ID assignment and publication; never reorder snapshots and CDC after preparing their before-state assumptions.

This replaces a single coordinator-owned mutable row loop with independently owned state partitions. Shared destination buckets do not force their source-table evaluation to serialize: merge their operation streams and accounting later, at bucket packing/publication.

#### Workers return plans; the coordinator assigns ranges

Workers produce operation plans with local ordinals, operation counts, membership mutations, and bucket/accounting contributions. The coordinator fixes eligible group membership and assigns disjoint ranges using those counts. Workers apply offsets themselves; the coordinator need not visit every row to call `next()`.

For a shared bucket, a packing task merges the group's ordered contributions before choosing chunk boundaries. Different buckets pack concurrently; a single large bucket can have its ordered stream split into chunks and those chunks encoded concurrently. This preserves the existing object format and avoids one tiny object per worker. Final BSON encoding requires assigned IDs, but JSON payloads and checksums can already be prepared. Upload each completed immutable chunk without waiting for other buckets to finish packing.

Prepared bulk-write plans and S3 accounting deltas should likewise be constructed outside the publication transaction. Combine repeated updates to shared bucket/source-table keys rather than letting workers race to overwrite each other's counters. Transaction-dependent validation and updates must still execute against current storage state.

#### Why keep the final database writes serialized?

Separate tables can still update the same bucket state, usage records, and stream head. The current persistence methods use one MongoDB session and transaction, whose operations cannot execute concurrently. Moving each worker's writes to a separate transaction would also expose partial groups and speculative membership state; delaying only `last_persisted_op` is not a complete visibility/recovery protocol.

Keep one short atomic publisher for now, with no evaluation, packing, or S3 wait in it. MongoDB membership reads and lifecycle-marker registration for other groups can still overlap publication on independent operations/sessions. If measured metadata-write capacity then limits throughput, concurrent metadata persistence is a separate design step requiring a proof for readers, compaction, state visibility, and crash recovery. It is not necessary to leave the CPU and read stages serial while investigating that limit.

### Batch structure

| Unit              | Purpose                                                                    | Boundary                                                   |
| ----------------- | -------------------------------------------------------------------------- | ---------------------------------------------------------- |
| Source page       | Efficient fetch and source resume information                              | Cursor batch, not automatically a storage flush            |
| Preparation block | Conversion, evaluation, and membership lookup                              | Small byte-bounded block                                   |
| Commit group      | Reserved ID interval, immutable output plan, atomic publication, and retry | Output bytes, metadata work, age, or consistency barrier   |
| Bucket chunk      | Existing-format inline document or S3 object                               | Per-bucket size target; partial chunks sealed at group end |

A fair coordinator combines ready blocks from one or more tables into a group for one replication stream. Preserve each producer's order and snapshot/CDC semantics. A group need not contain every active table, and must not wait for a slow table to produce data. Size targets are upper targets under load, not minimum fill requirements: a checkpoint marker immediately seals pending work, however small.

Determine the operation count and transaction-size requirements before finalizing the group's range and upload plan. Count bucket operations, parameter entries, and other ID consumers—not just source rows. Split oversized plans before upload. This avoids discovering after upload that one intended commit needs interleaving ID ranges or unsafe transaction sizes.

Within a group, tables can evaluate independently where their source state is independent. Assign table work disjoint subranges, then serialize bucket chunks in increasing operation-ID order. Different tables can share a bucket, so arbitrary worker completion order must not become the persisted chunk order. Compatible operations can be combined before upload to improve object sizes.

### Reserve ranges for groups, not long-lived table writers

Use a short atomic increment of the existing `op_id_sequence` counter to reserve a group interval. Its value becomes the **allocated high-water mark**, not the last committed operation. Allocation must be durable before the IDs are used, and an ambiguously acknowledged reservation must never be guessed or reused.

For example:

```text
Group A reserves 1–1000; uses 1–730
Group B reserves 1001–2000; uses 1001–1650
Group C reserves 2001–3000; uses 2001–2400

Uploads finish: B, C, A
Data commits:   A, B, C
Unused IDs:    permanent gaps
```

A group's first operation is greater than every operation in the preceding committed group. Unused capacity is discarded; a table cannot save the tail of an old range for a later commit. If a plan outgrows its allocation, rebuild/resequence the affected plan and any dependent work rather than extending it past a later group's range.

Prefer exact counts once evaluation is complete; bounded over-reservation is an option when its reduction in coordination justifies it. A coordinator can reserve several upcoming groups with one increment, provided it fixes their order and never returns unused IDs to circulation.

The existing stream `last_persisted_op` must remain a **closed committed prefix** for new replicated operations, not merely the maximum ID among whichever groups have finished. Update it with ordered data publication, never with reservation or upload completion. Existing commit code must stop writing the allocation counter back down to the last used ID. All writers sharing that counter must adopt the new protocol together; mixing the old allocator with reservations is unsafe.

#### Can operations arrive below `last_persisted_op` later?

Not across separately visible transactions under the existing protocol. Once the persisted head is H, every intended new operation at or below H must already be durable, or its ID must be a permanently abandoned gap. Compaction's existing rewrites of historical operations are a separate case.

`MongoBucketBatchV3.commit()` adopts `last_persisted_op` as the next checkpoint, including for a writer with no new data. If group B advances it to 200 while group A's operations 1–100 are still pending, a checkpoint can expose 200. A client can advance its bucket cursor to 200; inserting op 50 afterward does not make that client rewind. Checksum/parameter results for the checkpoint can also change, and checkpoint-diff queries use operation ranges that exclude late lower IDs.

Holding back an explicit checkpoint call is not a sufficient general safeguard: other writers and newly activated configs use the shared head, and initial compaction uses it before the first checkpoint. Advancing it also must not allow checkpoint-based soft-delete cleanup to run ahead of pending work.

It is fine to update the field before inserting lower-ID operations **inside the same MongoDB transaction**, provided all those writes commit atomically and the preceding published prefix is respected. Statement order within the transaction is not the visibility boundary. The existing flush updates the head in the same transaction as its data; retain that guarantee.

Therefore reservation may advance `op_id_sequence` far ahead, and uploads may finish out of order, but group B cannot advance `last_persisted_op` past pending group A. Ordered publication is what makes pre-allocation compatible with the current readers and checkpoints.

### Concurrent writes have an ordered publication point

Keep a bounded window of admitted groups:

```text
Group A: prepare → upload ───────────────→ commit
Group B:     prepare → upload ─────→ ready      → commit
Group C:          prepare → upload ───────→ ready      → commit
```

Once the next group is uploaded, its short MongoDB transaction publishes bucket references, parameter entries, source-record memberships, bucket-state changes, upload-marker removals, and the persisted head. MongoDB operations within that transaction remain sequential. No S3 request or CPU-heavy evaluation belongs in its retry callback.

Do not let B publish ahead of A merely because B uploaded first. This preserves strictly increasing IDs between data commits and avoids introducing durable out-of-order publication state. Where beneficial and within transaction limits, adjacent ready groups may be published together, preserving their order and barriers.

A slow first group can still delay publication. Bound group size and the number of in-flight groups, schedule latency-sensitive CDC before allocating snapshot ranges, and stop admitting work when the window is full. Reservation fixes ordering: CDC cannot later jump ahead of an already reserved snapshot group. More concurrent uploads hide ordinary latency, not an indefinitely stalled predecessor.

### Preserve marker-driven throttled batching

This behavior needs an explicit feedback loop; backpressure alone does not preserve it. In the current [change stream loop](../../modules/module-mongodb/src/replication/ChangeStream.ts), an ordinary batch marker is requested when needed, and handling its event awaits `batch.commit()` before processing subsequent events. Standalone checkpoint requests commit promptly when caught up and are coalesced into the batch-marker flow when work is buffered.

With read-ahead, distinguish **marker observed** from **marker durably handled**:

1. When CDC work needs a boundary and no ordinary batch marker is outstanding, request one as today.
2. Seeing that marker inserts an ordered barrier and immediately seals the preceding partial preparation block, commit group, and bucket chunks. Nothing waits for a size target or batching timer. Only the work through the barrier must complete; later queued work must not delay preparation of that prefix.
3. Complete the barrier after its preceding groups are durable and its checkpoint/resume handling has completed. Keep the ordinary marker outstanding until then. Read-ahead may continue preparing and uploading later work, but merely reading the marker does not authorize another ordinary marker.
4. If post-boundary CDC work is already queued when the barrier completes, request the next marker immediately. Otherwise, request it when the next change arrives. The feedback must notice already-buffered work; waiting only for another source event could strand it.

“Completed” here means the storage barrier has been processed, even if a snapshot/config barrier prevents a client-visible checkpoint. Waiting for snapshot activation before allowing another marker would stall catch-up. Keep source-specific marker classification, foreign-marker filtering, and the existing keepalive/stale-resume recovery paths; their extra markers are not an unrestricted ordinary batching loop.

| Situation | Result                                                                                                                                                                                          |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Caught up | The marker arrives shortly after the change, seals a small group, and publishes as soon as its uploads finish. There is no minimum batch size or mandatory delay.                               |
| Behind    | Durable completion delays the next marker request. More source changes fall between markers; byte/work limits may produce several concurrent upload groups, followed by one checkpoint barrier. |
| Idle      | No empty size-driven groups are generated. Keepalive and standalone checkpoint requests retain their source-specific behavior.                                                                  |

Preserve standalone-request coalescing, but extend the current “buffered changes” check to work beyond the boundary in the CDC queues and unpublished groups, rather than just the current source response. Do not classify the mere existence of a long-running snapshot as CDC lag and indefinitely defer an otherwise ready active-config checkpoint.

At publication, handle a ready checkpoint barrier immediately before advancing the persisted head past its selected prefix. Do not wait to fill a combined transaction or drain all future uploads. Age limits remain safety bounds, not a replacement fixed-interval batching policy.

The concurrency window is a ceiling, not a target to fill. Limit snapshot reservations ahead of CDC and reserve upload capacity for latency-sensitive work. Already reserved predecessors still impose a wait, so this preserves the adaptive batching behavior without promising identical latency under competing snapshots or slow S3. New reservation and lifecycle-marker round trips also belong in the low-load latency budget.

## Concurrent snapshots and state dependencies

Replace the current [serial table snapshot loop](../../modules/module-mongodb/src/replication/MongoSnapshotter.ts) with bounded table workers. They share CPU, memory, upload, and publication budgets. Separate tables can prepare and upload concurrently, while the coordinator gives each producer durable completion receipts for its included work.

The important dependency is source state, not just table-worker identity. Snapshot and CDC can touch the same source record. Preparation outside the transaction must not read stale memberships and blindly publish the result later.

Maintain partition-owned views of committed state plus the effects of earlier prepared groups, keyed by source table and replica ID, as described above. Fetch missing state in bounded concurrent reads. Preserve CDC order, snapshot skip-if-present behavior, and soft-delete protection. A failed or replanned group invalidates dependent prepared state.

At publication, validate that the ownership, config/table context, and relevant source-state assumptions still hold. All replication mutations must participate in the coordination protocol; cleanup or other concurrent mutations require appropriate transactional validation. A failed validation rebuilds affected output. This is real implementation work, potentially including extra reads, whose IOPS cost must be measured; range reservation alone does not solve state consistency.

### Progress and checkpoint ownership

Keep three concepts separate:

- **Allocated:** IDs have been reserved; no data or source progress is implied.
- **Persisted:** the group's payload references and metadata have committed.
- **Checkpointable:** the persisted prefix also satisfies source transaction and snapshot/config barriers.

A producer's flush waits for its own preceding admitted work, including any earlier reserved groups ahead of it—not for other tables to finish their entire snapshots. Persist table page progress only after its data is durable. Overall snapshot completion waits for all required tables and their in-flight work.

The coordinator owns checkpoints and source resume progress. CDC transactions may span groups without an intermediate checkpoint; split events retain valid restart boundaries. Snapshot completion cannot expose partially persisted CDC work. Per-config barriers continue allowing an active config to run while a processing config snapshots.

Process a checkpoint boundary at the appropriate published prefix before publishing later CDC work past it. Later work may already be prepared and uploaded. Upload completion alone never advances a source token or checkpoint.

## Recovery and orphaned S3 objects

Use the existing [ObjectStorageLifecycle](../../modules/module-mongodb-storage/src/storage/implementation/v3/object-storage/ObjectStorageLifecycle.ts) mechanism in replication:

1. **Before PUT:** durably register deletion markers for the planned immutable object paths.
2. **Upload:** freeze bytes and paths once submitted. Identical prepared output can reuse completed uploads across publication-transaction retries.
3. **Publish:** insert references and remove their markers atomically with group metadata. Enforce the existing publication lease.
4. **Abandon:** failed/replanned groups leave markers for delayed cleanup. Changed output uses new paths; never overwrite a published object.

Because the publication transaction starts after preparation and upload, its snapshot can see the pre-existing markers. There is no longer a need to reserve speculative object-path slots before an upload-bearing transaction. Batch marker operations to control the added MongoDB IOPS.

For an ordinary MongoDB transaction retry, keep immutable uploads and retry publication if its assumptions remain valid. If output must change, replan dependent work and leave abandoned uploads tracked. Resolve an ambiguous commit before retrying publication, issuing receipts, or abandoning its objects.

After a process crash, fence the previous coordinator, recover committed progress, discard its unpublished plans, and replay from durable source/snapshot positions with fresh IDs above the allocation high-water mark. Old unused intervals become gaps; orphan markers clean up their objects. Do not persist an in-flight range journal merely to recover unused IDs.

**Ownership must cover the sequence's ordering scope.** The current counter is database-wide. To preserve global commit ordering, use one fenced allocation/publication authority for all participating streams in that storage database; keep each group's data scoped to one stream. Per-stream locks alone do not order independent publishers across streams. Ownership validation must fence publication transactionally, not merely rely on a timer noticing a lost lease. The existing lock machinery is a starting point, but requires integration with this protocol.

This preserves data structure while changing writer coordination. If multiple independent processes must publish concurrently, that needs a separate coordination design; atomic range increments alone guarantee uniqueness, not commit order.

## Backpressure, object sizes, and MongoDB cost

Bound retained bytes across source buffers, prepared results, speculative state, open chunks, uploads, and groups awaiting publication. Reserve capacity for the earliest group to make progress so completed later uploads cannot consume all memory. Window and byte limits propagate backpressure to source fetching.

Each resource has its own bounded concurrency: CPU workers, MongoDB reads, uploads, and the publication window. Raising one must not multiply all the others. Prioritize tasks required by the earliest CDC barrier, including CPU work, while guaranteeing snapshot progress. Worker results may arrive out of order; control boundaries complete from their required input prefix, not from whichever task finishes last globally.

Allow larger groups to accumulate more operations per bucket before forming objects. Keep the existing object format and chunk target initially. Sparse buckets still produce partial chunks; more concurrency does not remove the need for useful bytes per request. Size S3 concurrency and the HTTP connection pool together, with shared capacity for client reads and compaction.

Coalesce redundant membership updates and aggregate bucket bookkeeping within groups, while preserving bucket operations and parameter values. Avoid full-payload MongoDB staging. Measure the new reservation, marker, validation, and fencing work alongside the writes saved; fewer round trips do not automatically mean fewer physical IOPS.

## Validation target

At 30 MB/s, 100 ms represents roughly 3 MB of source data in flight before output amplification, transfer time, and headroom. A bounded window of concurrent uploads can cover that delay without holding MongoDB transactions open.

Recovering the reported low-latency rate only gets to 15 MB/s. Reaching 30 also needs enough source, CPU, and MongoDB capacity. Prototype reserved group IDs, concurrent snapshot uploads, short ordered publication, and orphan tracking together.

Measure durable-checkpoint throughput, publication transaction duration, MongoDB IOPS, object sizes, memory, checkpoint latency, and fairness with concurrent snapshots and CDC. Exercise range gaps, out-of-order upload completion, transaction retries, crashes, stale owners, repeated-key changes, and snapshot/CDC races. The throughput target remains unvalidated.

Measure CPU utilization and queue/service time for each stage to verify actual overlap. Compare one versus several preparation/packing workers, and one large table versus several tables targeting shared buckets. Include repeated updates to one hot key: its state transitions remain serial, even though unrelated work should continue. Additional read concurrency must improve wall-clock time without uncontrolled MongoDB IOPS or cache pressure.

For latency, compare isolated writes and short bursts against the current implementation, including concurrent snapshots. Verify that each observed boundary seals partial work immediately, ordinary marker frequency follows durable completion rather than reader speed, buffered post-boundary work requests another marker without needing a new source event, and checkpoint frequency recovers promptly after a backlog drains. Record p50/p95/p99 source-to-checkpoint latency and checkpoint/group sizes across these load transitions.
