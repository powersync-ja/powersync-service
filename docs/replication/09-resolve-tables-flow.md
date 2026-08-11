# resolveTables Lifecycle: Discovery, Diff, and Snapshot State

This document explains the conceptual flow around `resolveTables`:

1. Discover a table (either from CDC or initial snapshot scan).
2. Resolve it into tracked table records.
3. Detect conflicts/differences with previously tracked records.
4. Persist changes and snapshot state.

_Partially AI-generated, manually reviewed and modified._

## Core concepts

### TablePattern

A `TablePattern` is a sync query selector. This includes:

1. Connection tag. _We don't support multiple connections yet, but this caters for it in theory._
2. Schema name.
3. Table _pattern_.

This is configuration-level only, and not used in persisted state.

### SourceTableRef

A `SourceTableRef` describes the table of a source row as replicated. This includes:

1. Connection tag.
2. Schema name.
3. Table name.

This is similar in structure to `TablePattern`, but uses specific names instead of wildcards.

We can do direct matching of `TablePattern.matches(ref: SourceTableRef)`. This is what drives matching of replicated rows with specific sync queries.

### SourceEntityDescriptor

This is a SourceTableRef with additional metadata used for replication:

1. objectId / relation id: The underlying id of the table/collection in the source database. This is used to track renames.
2. replicaIdColumns: The columns and types representing the "replica identity" for the table.
3. sourceMetadata (nullable): Opaque, source-specific identity metadata. `null` means no metadata has been recorded. Storage persists and hydrates non-null values verbatim and never interprets them.

### SourceTable

A `SourceTable` is a replicated table with state:

1. It has a specific `SourceTableRef`, but the same ref may have multiple `SourceTable`s.
2. It stores the specific metadata from the `SourceEntityDescriptor` - any changes would result in a new `SourceTable`.
3. It tracks snapshot lifecycle state (complete/in-progress, progress markers).
4. It carries resolved sync participation flags (used for data, parameters, events).
5. It tracks which persisted bucket data definitions and parameter indexes are used with it.

There may be multiple `SourceTable`s per `SourceTableRef`. Historically it was generally 1:1, but incremental reprocessing now uses multiple records when a new bucket data source or parameter index is added. Instead of re-snapshotting an existing `SourceTable`, storage creates a new `SourceTable` with the same `SourceTableRef`. The new snapshot then only affects the new definitions, not existing compatible ones.

When multiple records exist for one physical table, their bucket and parameter memberships must be disjoint so each definition receives each source row once. Storage also designates a single event carrier so row-change events are not duplicated.

`SourceTable` is also used to track changes that may require a re-snapshot:

1. Renamed tables (same table name with different relationId or vice versa).
2. Changes in replica identity.

These changes generally require "truncating" the outdated `SourceTable`, then snapshotting the new one.

## High-level flow

### 1. Table discovery

There are two entry paths:

1. CDC path: a relation/change event reveals a table at runtime.
2. Initial snapshot path: the snapshot process scans configured patterns and discovers existing tables before streaming catches up.

These both produce a `SourceEntityDescriptor`, describing the table to replicate.

### 2. Match sync-rule patterns

For each discovered table, the system finds all matching `TablePattern`s.

This may be more than one pattern, for example wildcard and exact-match overlaps, or multiple sync configs in one incremental stream.

Each matching pattern is resolved independently.

### 3. Resolve into tracked tables (`resolveTables`)

`resolveTables` maps the discovered physical table to one or more `SourceTable` records.

Conceptually it does:

1. Find persisted records that overlap by `(schema + name) OR object/relation id`.
2. Let the source connector classify them as compatible or incompatible.
3. Resolve matching sync config definitions and reuse compatible records where possible.
4. Persist source metadata updates and create records for missing definitions.
5. Return the records to replicate into and the incompatible records to drop.

Important: one physical table can resolve to multiple `SourceTable` records when sync config definitions have been added over time.

### Source-owned candidate reconciliation

`resolveTables` delegates compatibility checks to the connector through `reconcileSourceTables`. If none is provided, it uses the default identity comparison.

- Storage hydrates overlapping records into `SourceTable` values, then passes isolated clones to the connector through a read-only `SourceTableCandidate` type.
- The source connector classifies those candidates and provides `sourceMetadata` for updated or new records.
- Storage persists those changes and manages record creation, snapshot state, memberships, and incompatible records.

The reconciler must not perform slow source queries. Candidate state is cloned before the callback; `withSourceMetadata()` is the supported way to return a metadata change. After reconciliation, storage rematerializes results from its original `SourceTable` values, so snapshot state, memberships, and identity remain storage-owned even if callback code mutates a candidate or bypasses the type boundary with a cast. Changes to the table identity still require a new record and snapshot.

### MSSQL: the replicated table set is fixed at deploy

MSSQL rejects table wildcards and requires every configured table to exist with CDC enabled at startup. This keeps the table set fixed for the life of the stream. Polling cannot detect a new table atomically with a commit, so adding a table, enabling CDC, or renaming a table into scope requires a new sync config deploy.

Dropping, renaming, or recreating a replicated table, or otherwise changing its persisted source identity, fails the job with `PSYNC_S1603`. Continuing would risk committing past changes that have not been read from that table. Existing replicated data is retained; it is removed when the replacement sync config is resolved.

### MSSQL capture-instance pinning

SQL Server can keep two capture instances for one table. Each binding is pinned to the capture table's object id:

- New and legacy bindings use the newest available instance.
- A pinned binding keeps using its instance when a newer one appears and logs a warning.
- If the pinned instance is removed, replication fails with `PSYNC_S1601`. A replacement is not adopted automatically because it may capture a different schema.
- Conflicting persisted pins are treated as invalid state.

Every configured table is resolved before streaming starts, which also backfills pins for legacy records. A table that does not exist or does not have CDC enabled fails with `PSYNC_S1602`.

To adopt a new capture instance, deploy a new sync config. The replacement processing binds to the new
instance and snapshots as needed. Keep the old instance available until the new sync config has finished
its snapshots and becomes active.

The parsed sync config set matters here. Source objects, hydration state, and definition mappings are identity-bound; resolving sources from one parse and writing them through a batch created from another parse can point at the wrong persisted ids.

### 4. Detect differences/conflicts

After resolution, the system identifies `SourceTable` records that conflict with the new definition and should be removed.

### 5. Decide whether to snapshot

For each resolved `SourceTable`, snapshotting is needed when:

1. Snapshot is not complete, and
2. The table is relevant to active sync behavior (data/parameters/events).

Newly discovered tables during CDC can trigger an inline or queued snapshot.
Initial snapshot mode enqueues all unresolved tables first, then processes them.

### 6. Persist snapshot progress and completion

During snapshot:

- Rows are written as replicated operations.
- Progress is periodically persisted (estimated total, replicated count, resume key).
- Flushes persist durable operation state before progress moves forward.

When a table snapshot finishes:

- The table is marked snapshot-complete.
- Per-table progress markers are cleared.
- A "do not checkpoint before X" boundary is advanced (LSN/GTID/timestamp equivalent), so final consistency waits for CDC to catch up past the snapshot point.

When all required tables are done in initial snapshot:

- Global snapshot state is marked complete.

### 7. Continue streaming with resolved mappings

Resolved `SourceTable` mappings are cached by relation identity for fast CDC routing.
Subsequent insert/update/delete events use those mappings to write bucket and parameter updates. If multiple `SourceTable` records are returned, the connector saves the row change for each relevant record; storage uses each record's membership ids to route the write to the correct persisted definitions.

If table metadata changes later, the same resolve + diff + drop cycle runs again.
