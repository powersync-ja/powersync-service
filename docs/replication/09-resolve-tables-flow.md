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
3. sourceMetadata (optional): Opaque, source-specific identity metadata. Storage persists and hydrates it verbatim and never interprets it.

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

1. Query every existing `SourceTable` record that _overlaps_ the physical table by `(schema + name) OR object/relation id`.
2. Hand the hydrated candidates to a source-provided reconciler that classifies compatibility and selects the metadata to persist (see below).
3. Resolve matching sources through the parsed sync config set's definition mapping.
4. Determine which persisted bucket and parameter definition ids are already covered by compatible records.
5. Create missing `SourceTable` records when coverage is incomplete, persisting the resolution's finalized `sourceMetadata`.
6. Return the `SourceTable` records that should receive replicated data, plus incompatible overlapping records to drop.

Important: one physical table can resolve to multiple `SourceTable` records when sync config definitions have been added over time.

### Source-owned candidate reconciliation

Compatibility is a source-specific decision, so `resolveTables` delegates it to a `reconcileSourceTables` callback provided by the connector (defaulting to a shared identity comparison when omitted). The split of responsibilities is:

- The **source module** owns compatibility classification and metadata selection. Its reconciler is deterministic and free of storage mutations. It may be asynchronous, but storage awaits it while resolution is in progress and may hold a transaction, so slow or unbounded external I/O should be avoided where possible.
- **Storage** owns transactions, record creation, initial snapshot state, v3 membership coverage/disjointness, event-carrier selection, and returning incompatible records to drop. It persists and hydrates `sourceMetadata` but never interprets it.

All records created in one resolution receive the same `sourceMetadata`, so a physical-table binding never mixes metadata-free and metadata-bearing records. Identity-bearing fields (schema/name, object id, replica id columns, and `sourceMetadata`) are immutable: if populated metadata changes, the reconciler treats the old records as incompatible and storage creates replacements with fresh snapshot state. Legacy records with absent metadata are left absent — they are never backfilled.

### MSSQL capture-instance pinning

SQL Server CDC can have two capture instances for one physical table, each with its own change table and captured schema. The MSSQL reconciler pins new bindings to a specific capture instance:

- **New binding** (no compatible candidates): pin to the newest available capture instance and persist `{ captureTableObjectId }`.
- **Legacy binding** (compatible candidates all lack metadata): keep them compatible, persist no metadata, and retain the previous automatic new-capture behavior.
- **Pinned binding** (compatible candidates share one capture identity): keep them compatible and persist the same identity; fail if that capture instance is no longer available rather than silently switching.
- **Invalid state** (mixed metadata-free + pinned, or multiple pinned identities): fail with a diagnostic error.

At runtime a pinned stream polls its bound capture instance and uses that instance's minimum LSN for retention checks. When a newer capture instance appears it logs a single warning (throttled per newer instance) that a redeploy is required, and keeps polling the bound instance. If the bound instance disappears, replication for that table stops instead of falling forward.

**Deployment procedure for MSSQL capture/schema transitions:** adopting a new capture instance requires deploying a _new replication stream_. Reusing the same stream restores its pinned capture identity and therefore will not adopt the new instance. Promotion of the new stream waits for its snapshots to finish; the old stream keeps its capture instance available until it is retired, at which point the old capture instance can be removed.

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
