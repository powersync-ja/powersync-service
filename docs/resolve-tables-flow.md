# resolveTables Lifecycle: Discovery, Diff, and Snapshot State

This document explains the conceptual flow around `resolveTables`:

1. Discover a table (either from CDC or initial snapshot scan).
2. Resolve it into tracked table records.
3. Detect conflicts/differences with previously tracked records.
4. Persist changes and snapshot state.

_Partially AI-generated, manually reviewed and modified._

For the storage-version-3 MongoDB implementation details (membership coverage, narrowing,
drop semantics, invariants), see [resolve-tables-v3.md](./resolve-tables-v3.md).

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

We can do directy matching of `TablePattern.matches(ref: SourceTableRef)`. This is what drives matching of replicated rows with specific sync queries.

### SourceEntityDescriptor

This is a SourceTableRef with additional metadata used for replication:

1. objectId / relation id: The underlying id of the table/collection in the source database. This is used to track renames.
2. replicaIdColumns: The columns and types representing the "replica identity" for the table.

### SourceTable

A `SourceTable` is a replicated table with state:

1. It has a specific `SourceTableRef`, but the same ref may have multiple `SourceTable`s.
2. It stores the specific metadata from the `SourceEntityDescriptor` - any changes would result in a new `SourceTable`.
3. It tracks snapshot lifecycle state (complete/in-progress, progress markers).
4. It carries resolved sync participation flags (used for data, parameters, events).
5. It tracks which sync stream / bucket definitions are used with it.

There may now be multiple `SourceTable`s per `SourceTableRef`. Historically it was generally 1:1, but we now support multiple in preparation for incremental reprocessing: If a new data source or parameter index creator is added, we need to snapshot the source table. Instead of doing a re-snapshot of an existing SourceTable, we create a new SourceTable with the same SourceTableRef. The new snapshot then only affects the new data sources, not existing ones.

`SourceTable` is also used to track changes that may require a re-snapshot:

1. Renamed tables (same table name with different relationId or vice versa).
2. Changes in replica indentity.

These changes generally require "truncating" the outdated `SourceTable`, then snapshotting the new one.

## High-level flow

### 1. Table discovery

There are two entry paths:

1. CDC path: a relation/change event reveals a table at runtime.
2. Initial snapshot path: the snapshot process scans configured patterns and discovers existing tables before streaming catches up.

These both produce a `SourceEntityDescriptor`, describing the table to replicate.

### 2. Match sync-rule patterns

For each discovered table, the system finds all matching `TablePattern`s.

This may be more than one pattern (for example wildcard + exact match overlaps, or multiple rule sets).

Each matching pattern is resolved independently.

### 3. Resolve into tracked tables (`resolveTables`)

`resolveTables` maps the discovered physical table to one or more `SourceTable` records.

Conceptually it does:

1. Look up existing `SourceTable` records that match the physical identity.
2. Determine which sync config sources are already covered.
3. Create missing `SourceTable` records when coverage is incomplete.
4. Return the `SourceTable` records that should receive replicated data.

Important: one physical table can resolve to multiple `SourceTable` records when sync config definitions have been added over time.

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
Subsequent insert/update/delete events use those mappings to write bucket and parameter updates.

If table metadata changes later, the same resolve + diff + drop cycle runs again.
