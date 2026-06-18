# Convex snapshot consistency

The current approach should be consistent:

1. pin one global Convex snapshot cursor,
2. snapshot each selected table at that same cursor,
3. persist that cursor as the replication resume point,
4. stream `document_deltas` starting from that original cursor.

The important property is that every table snapshot is read at the same Convex
snapshot cursor, not that all tables are fetched in one HTTP response. Since
`list_snapshot` accepts a `snapshot` parameter, we can snapshot tables
individually while still reading the same database state.

Filtering specific tables should also be consistent as long as filtering happens
inside the PowerSync replicator after reading the unfiltered Convex delta stream.
It would become more delicate if Convex exposed a table-filtered delta stream and
PowerSync used that as the source of truth, because then some source cursor
positions could be skipped by the stream.

## Current initial replication flow

The Convex replicator does not take independent "latest" snapshots per table.
Instead, it resolves a single snapshot boundary and reuses it for every table:

1. `getGlobalSnapshotCursor()` calls `list_snapshot` without a table filter to
   get the latest global snapshot cursor.
2. The replicator stores that cursor as the resume LSN with `setResumeLsn`.
3. Each selected source table is snapshotted with `list_snapshot({ tableName, snapshot: snapshotCursor })`.
4. Each page returned by `list_snapshot` is checked to ensure the returned
   `snapshot` still matches the original `snapshotCursor`.
5. After all selected tables are snapshotted, the replicator marks the snapshot
   done at that same LSN and commits.
6. Streaming replication starts from the same cursor using `document_deltas`.

This is the usual snapshot-then-stream consistency pattern. Rows changed while
the snapshot is being read are allowed to be missed by the snapshot because they
will appear in the delta stream after the pinned cursor.

## Why individual table snapshots are okay

Fetching each table separately would be unsafe if each request implicitly used
"whatever the latest database state is at request time". In that model, table
`A` could be read at time `T1`, table `B` could be read at time `T2`, and the
combined snapshot could represent no real database state.

Convex avoids that problem by allowing a fixed snapshot cursor to be supplied to
`list_snapshot`. Once the replicator has pinned cursor `S`, every table snapshot
request is for the state of that table at `S`.

That means a full "snapshot all tables at once" API is not required for
consistency. It may be convenient, but the consistency boundary is already the
shared snapshot cursor.

## Relationship to document deltas

The second half of the consistency guarantee is the relationship between the
snapshot cursor and `document_deltas`.

For the current design to be correct, the cursor returned by `list_snapshot`
must be usable as the cursor passed to `document_deltas`, and `document_deltas`
must then return all committed changes after that cursor in order. Based on the
behavior tested so far, Convex's streaming export APIs appear to provide this:

- backend mutations are reflected in the snapshot head after the mutation
  succeeds,
- delta pages appear to contain complete Convex transactions, or groups of
  complete transactions,
- the delta cursor is a global source position rather than a per-table position.

With those properties, the snapshot and stream compose cleanly. The snapshot
captures the selected tables at cursor `S`; the delta stream then advances the
replica from `S` onward.

## Table filtering

PowerSync can still filter which tables it stores and applies to sync rules.
The key distinction is where filtering happens.

The safe shape is:

1. read the global Convex delta stream,
2. observe every Convex cursor in order,
3. ignore rows for tables not selected by sync rules,
4. commit or keepalive based on the cursor that was observed.

This is the current model: Convex `document_deltas` is not filtered per table at
the API level for PowerSync. The replicator receives delta pages first and then
decides whether each row matches a selected source table.

That means an unrelated table write can still move the cursor seen by the
replicator, even if PowerSync ignores the row. This is important for checkpoint
progress and avoids the filtered-stream issue seen in sources where the
replication stream itself only includes selected tables.

## What would be risky

Consistency would become trickier if PowerSync used a Convex API mode that
filtered deltas before the replicator saw them.

For example, suppose the source cursor advances for writes to tables `A` and
`B`, but PowerSync only subscribes to a stream of table `A` deltas. If a write to
table `B` advances the global cursor, the source head may move to a position
that the filtered stream never emits. That can make resume points and write
checkpoint heads harder to reason about.

This does not appear to be the current Convex setup. The current setup filters
after receiving the delta page, so the replicator can still observe cursor
movement caused by ignored tables.

## Resume behavior

Because the snapshot cursor is persisted before table snapshotting begins, the
initial snapshot can be resumed against the same global snapshot boundary. Table
progress stores the table page cursor and whether that table's snapshot is
finished.

On restart, the replicator should continue using the original snapshot cursor
for unfinished tables. Completed tables do not need to be snapshotted again. Once
all tables are marked complete, streaming resumes from the same global cursor and
replays any later deltas.

This is why resumable per-table snapshots are compatible with consistency: the
resume point is not a new per-table "latest" snapshot. It is the original global
snapshot cursor.

## Conclusion

There does not appear to be a consistency issue with snapshotting Convex tables
individually, as long as every table is snapshotted at the same pinned
`list_snapshot` cursor and `document_deltas` starts from that cursor.

Likewise, filtering selected tables should be safe when filtering happens in the
PowerSync replicator after reading the unfiltered Convex delta stream. The risky
case would be source-side table-filtered deltas, where the global source cursor
can advance for writes the replication stream never exposes.
