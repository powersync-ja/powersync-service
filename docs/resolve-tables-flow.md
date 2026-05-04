# resolveTables Lifecycle: Discovery, Diff, and Snapshot State

This document explains the conceptual flow around `resolveTables`:

1. Discover a table (either from CDC or initial snapshot scan).
2. Resolve it into tracked table records.
3. Detect conflicts/differences with previously tracked records.
4. Persist changes and snapshot state.

## Core concepts

### TablePattern

A `TablePattern` is a sync-rule selector:

- It describes which source tables should match (exact name or wildcard prefix).
- It is scoped by connection/schema.
- It is configuration-level and does not represent persisted runtime state.

Think of it as: "which tables should we care about?"

### SourceTable

A `SourceTable` is a concrete tracked runtime table:

- It represents one resolved physical source table identity.
- It carries replication identity metadata (replica-id columns).
- It tracks snapshot lifecycle state (complete/in-progress, progress markers).
- It carries resolved sync participation flags (used for data, parameters, events).

Think of it as: "this exact table instance is now being tracked and replicated."

## High-level flow

### 1. Table discovery

There are two entry paths:

- CDC path: a relation/change event reveals a table at runtime.
- Initial snapshot path: the snapshot process scans configured patterns and discovers existing tables before streaming catches up.

In both paths, discovery produces a physical table descriptor:

- connection
- schema
- table name
- object identity (when available)
- replica-id columns

### 2. Match sync-rule patterns

For each discovered table, the system finds all matching `TablePattern`s.

This may be more than one pattern (for example wildcard + exact match overlaps, or multiple rule sets).

Each matching pattern is resolved independently.

### 3. Resolve into tracked tables (`resolveTables`)

For each matching pattern, `resolveTables` maps the discovered physical table to one or more `SourceTable` records.

Conceptually it does:

- Look up existing tracked records that match the physical identity.
- Determine which sync-rule sources are already covered.
- Create missing tracked records when coverage is incomplete.
- Return the current tracked records that should receive replicated data.

Important: one physical table can resolve to multiple tracked records when different sync-rule source sets are represented separately.

### 4. Detect differences/conflicts (`resolveTablesToDrop`)

After resolution, the system identifies tracked records that conflict with the new definition and should be removed.

Common conflict cases:

- Rename: same physical object identity now has a different name.
- Identity change: same table name but replica-id definition changed.
- Object replacement: same schema/name now points at a different physical object identity.

Those conflicting records are dropped/truncated so old state cannot leak into the new table identity.

### 5. Decide whether to snapshot

For each resolved `SourceTable`, snapshotting is needed when:

- snapshot is not complete, and
- the table is relevant to active sync behavior (data/parameters/events).

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
Subsequent insert/update/delete events use those mappings to write bucket and parameter updates.

If table metadata changes later, the same resolve + diff + drop cycle runs again.

## Why this design exists

This lifecycle guarantees:

- New tables can be onboarded without restarting replication from scratch.
- Renames and identity changes do not preserve stale state.
- Snapshot progress is resumable and durable.
- CDC and snapshot together converge to a consistent state after catch-up.

## Refactor plan: plural table resolution

Reference branch: `incremental-reprocessing`.

The old branch has the core shape we want, but it is outdated around the surrounding writer/storage architecture. Use it as prior art for the `resolveTables` semantics, not as a direct patch source.

### Current mismatch to fix

The current storage API is singular:

- `resolveTable(options)` returns one resolved `SourceTable`.
- The same call also returns `dropTables`.
- Callers cache and snapshot a single table per discovered source row.
- `TablePattern` matching is mostly implicit inside `HydratedSyncRules.tableSyncsData/tableSyncsParameters/tableTriggersEvent`.

The desired model is plural:

- One physical source row/table can resolve to multiple `SourceTable` records.
- In MongoDB v3, each `SourceTable` can represent the subset of sync-rule sources it owns.
- In Postgres and MongoDB v1, we can assume one `SourceTable` per physical source row and keep source ownership implicit.
- Pattern matching should happen before persistence resolution.
- Persistence resolution should work with concrete source-table records, with storage-specific support for source memberships where needed.

### Target concepts

Keep a clear distinction between:

- `TablePattern`: a sync-rule selector. It answers "does this rule source care about this physical table?"
- physical source descriptor: the discovered table/collection identity from CDC or snapshot discovery.
- `SourceTable`: a persisted runtime table record. It answers "which exact source table record should receive rows and snapshot state?"
- source membership: for MongoDB v3 only, the set of bucket data sources and parameter lookup sources covered by a `SourceTable`.

The important MongoDB v3 behavior is that adding a new sync-rule source for an already tracked physical table can create a new `SourceTable` for the uncovered source membership, without reprocessing unrelated existing memberships.

For Postgres and MongoDB v1, the compatibility behavior can stay coarser: a physical source row resolves to one `SourceTable`, and row evaluation uses the active sync rules for that table.

### API direction

Introduce a plural resolver on `BucketStorageBatch`:

- `resolveTables(options): Promise<ResolveTablesResult>`
- `ResolveTablesResult` should contain `tables: SourceTable[]` and `dropTables: SourceTable[]`.
- Options should include the physical `entity_descriptor`, connection metadata, and matching sources when available.
- The caller should compute matching sources from the record table schema/name before calling `resolveTables`.
- If matching sources are not available for the storage version or sync-rule context, the caller should pass `null`.
- Avoid a generic requirement that every storage layer persists source membership IDs.

The old branch passed a single `pattern` to `resolveTables` and used `rowProcessor.getMatchingSources(pattern)` to compute memberships. That is a useful reference, but the new boundary should pass the already-computed matching sources, or `null` when fine-grained memberships are not meaningful.

Passing already computed source objects gives the cleanest split for MongoDB v3: stream/snapshot code handles matching `TablePattern`s from the record schema/name, while storage handles persisted `SourceTable` identity and translates memberships to storage-specific definition IDs. Postgres and MongoDB v1 should receive `null` because they do not split one physical source row across multiple persisted source memberships.

Placing `resolveTables` on `BucketStorageBatch` is feasible and preferred:

- replication and snapshot call sites already have a batch when they resolve discovered tables
- the batch already owns the parsed sync rules used for row evaluation
- MongoDB batches already carry the definition mapping needed by MongoDB v3
- Postgres batches already carry `group_id`, database access, and parsed sync rules
- resolving on the batch keeps table resolution, conflict detection, drops, truncates, snapshots, and row writes in one write-oriented API

`SyncRulesBucketStorage.resolveTable` can either be removed after call sites migrate, or kept temporarily as a compatibility wrapper during the transition.

### Resolution algorithm

For each discovered physical table:

1. Caller computes the matching sources for the physical table from schema/name, or `null` if fine-grained memberships are unavailable.
2. Query existing `SourceTable` records matching the physical identity:
   - same connection
   - same schema/name
   - same object identity when available
   - same replica-id columns
3. If matching sources are `null`, resolve or create one `SourceTable` for the physical identity.
4. If matching sources are present, compare existing source memberships with those desired sources.
5. Return existing records that cover any desired membership.
6. If any desired membership is uncovered, create a new `SourceTable` record for the uncovered subset and return it too.
7. Separately identify conflicting persisted records that should be dropped:
   - same object identity with a different schema/name
   - same schema/name with a different object identity
   - same schema/name with different replica-id columns
8. Snapshot all returned tables that are incomplete and relevant to active sync behavior.

The resolver should use all current matching physical records to determine coverage, even if a record only covers part of the desired membership set.

For Postgres and MongoDB v1, simplify this algorithm:

1. Match the physical table to active patterns.
2. Pass `matchingSources: null` to `resolveTables`.
3. Resolve or create the single `SourceTable` for the physical identity.
4. Detect conflicting persisted records for rename, replacement, or replica-id changes.
5. Return the single table in `tables`, preserving the plural API shape.

### Source membership storage

MongoDB v3 already has explicit membership fields:

- `bucket_data_source_ids`
- `parameter_lookup_source_ids`

Those IDs are used by MongoDB v3 storage and row processing to:

- decide whether an existing `SourceTable` already covers a matched bucket data source or parameter lookup source
- create a new `SourceTable` only for uncovered source memberships
- evaluate only the sources owned by that `SourceTable` when writing source records
- persist current-data/source-record entries with compact definition/index IDs
- fetch bucket data and parameter lookups by those persisted definition/index IDs

Postgres and MongoDB v1 should not add equivalent definition-id tracking as part of this refactor. For those storage versions, source membership is not part of the persisted `SourceTable` identity, and the resolver can keep the one-table-per-physical-source-row assumption.

Event sources should not get persisted definition IDs as part of this refactor. They do not currently have definition IDs, and storage does not persist long-lived data scoped to individual event source queries. Event participation should remain derived from the active sync rules for each resolved `SourceTable`.

Concretely:

- `matchingSources` should cover bucket data sources and parameter lookup sources only.
- `SourceTable.syncEvent` should be set by checking whether active event descriptors match the resolved table.
- event emission should continue to select matching event descriptors at write time from active sync rules.
- an event-only table should still resolve to a `SourceTable` so `syncAny` can trigger the required snapshot/CDC path, but it does not need a stable event membership ID.

### Caller changes

Replication streams and snapshotters should stop assuming one resolved table:

- relation caches need to store multiple `SourceTable` records per physical relation/collection identity.
- insert/update/delete handling must evaluate and save each resolved table independently.
- snapshot decisions should iterate resolved tables.
- mark snapshot done/progress APIs already accept arrays in several places; call sites should preserve per-table state when plural tables are returned.

This should be a generic caller shape even when a storage implementation currently returns one table. That keeps CDC and snapshot code consistent while allowing MongoDB v3 to return multiple records.

Provider-specific notes:

- Postgres and MSSQL have stable object identities, so rename and replacement detection should remain object-id aware.
- Postgres can keep resolving one `SourceTable` per physical source row; no definition-id persistence is needed unless we later want fine-grained incremental reprocessing there too.
- MySQL object ids are synthesized from names in current code, so rename detection remains limited unless a better physical identity is introduced.
- MongoDB v1 can keep resolving one `SourceTable` per physical source row.
- MongoDB v3 is the storage version that uses definition IDs for plural membership resolution and fine-grained reprocessing.

### Migration and compatibility plan

Implement in stages:

1. Keep source membership data model changes scoped to MongoDB v3.
2. Add `resolveTables` to `BucketStorageBatch` with a compatibility path that returns a single table where matching sources are `null`.
3. Move matching logic out of `resolveTable`: callers compute matching sources from schema/name and pass sources or `null`.
4. Implement MongoDB v3 coverage detection and creation of missing `SourceTable` records.
5. Keep conflict detection in `resolveTables` and return `dropTables` in the same result; keep the actual drop/truncate operation in the caller/batch.
6. Update relation caches, snapshot loops, and row processing to handle multiple resolved tables.
7. Remove or deprecate `SyncRulesBucketStorage.resolveTable` once stream and snapshot code resolve through the batch.
8. Add focused tests for overlapping patterns, adding a new source to an existing physical table, replica-id changes, renames, and object replacement.

### Test impact

The expected test impact is moderate but mostly mechanical:

- Storage implementations that implement `BucketStorageBatch` must add `resolveTables`, so TypeScript compilation will force the main changes.
- Existing storage tests mostly create writers and use `save`, `truncate`, `drop`, and snapshot methods; they should not need broad rewrites unless they mock `BucketStorageBatch`.
- Current tests do not appear to call `SyncRulesBucketStorage.resolveTable` directly, so moving call sites to `batch.resolveTables` should mainly affect replication stream tests and compile-time interfaces.
- New tests should cover the resolver behavior directly on a batch, especially MongoDB v3 plural records and Postgres/MongoDB v1 single-record compatibility.
- Existing helper `resolveTestTable(writer, ...)` already accepts a writer, so it can later be changed to call `writer.resolveTables` without changing most test call sites.

### Drop table handling

It is feasible and preferable to keep `dropTables` as part of the `resolveTables` result.

Conflict detection uses the same physical identity inputs as table resolution:

- connection
- schema/name
- object identity when available
- replica-id columns

Keeping this in the same storage step avoids a second query path that would repeat most of the resolver's lookup work. It also lets the storage implementation compute conflicts against the complete set of current resolved tables. This matters for MongoDB v3, where multiple current `SourceTable` records can share the same physical source row but own different definition IDs; those records must not be treated as conflicts with each other.

The resolver should only identify conflicts. The caller or batch should still perform the actual `drop(result.dropTables)` operation, so operation ordering stays explicit next to cache updates and snapshot handling.

Implementation detail: the conflict query should exclude all current valid `SourceTable` records returned by `resolveTables`, not just one table ID. That preserves existing rename/replacement/replica-id-change behavior while supporting plural resolution.
