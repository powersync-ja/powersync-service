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
2. Hand the hydrated candidates to a source-provided reconciler that returns compatible and incompatible tables, selects values for new records, and may return modified copies of compatible tables (see below).
3. Resolve matching sources through the parsed sync config set's definition mapping.
4. Determine which persisted bucket and parameter definition ids are already covered by compatible records.
5. Diff compatible returned tables against the candidates and persist allowlisted updates without resetting snapshot state.
6. Create missing `SourceTable` records when coverage is incomplete, persisting the resolution's new-record values.
7. Return the `SourceTable` records that should receive replicated data, plus incompatible overlapping records to drop.

Important: one physical table can resolve to multiple `SourceTable` records when sync config definitions have been added over time.

### Source-owned candidate reconciliation

Compatibility is a source-specific decision, so `resolveTables` delegates it to a `reconcileSourceTables` callback provided by the connector (defaulting to a shared identity comparison when omitted). The split of responsibilities is:

- The **source module** owns compatibility classification, source-owned changes on hydrated table copies, and values for newly created records. Its reconciler is deterministic and free of storage mutations. It may be asynchronous, but storage awaits it while resolution is in progress and may hold a transaction, so slow or unbounded external I/O should be avoided where possible.
- **Storage** owns transactions, diffing and applying allowlisted changes, record creation, initial snapshot state, v3 membership coverage/disjointness, event-carrier selection, and returning incompatible records to drop. It persists and hydrates `sourceMetadata` but never interprets it.

All records created in one resolution receive the same `sourceMetadata`. A reconciler may return `table.withSourceMetadata(...)` for compatible records when the source can prove that updating the metadata preserves the existing snapshot. Storage currently persists only this allowlisted difference; changes to schema/name, object id, or replica id columns still require incompatible replacement records with fresh snapshot state.

### MSSQL: the replicated table set is fixed at deploy

Table wildcards are rejected, and every configured table must exist with CDC enabled at startup. Together these fix the replicated table set for the life of a stream.

The reason is that a table can only enter scope by polling for it, and a poll can never be atomic with a commit. Any detection interval therefore leaves a window where checkpoints are committed without a table that belongs in them — and rows already committed may reference it. Such a checkpoint is not a state the source database was ever in, and clients cannot tell an absent row from an unarrived one, so they act on it. Shortening the interval narrows that window; only a fixed table set closes it.

Consequences: no table-create event, and adding a table, renaming one into scope, or enabling CDC on one all require a new sync deploy.

Dropping **or renaming** a replicated table fails the job with `PSYNC_S1603`. Schema checks run before polling within a cycle, so merely dropping the table from the cache would let that same cycle poll the remaining tables and commit the end LSN — permanently skipping any changes for the departed table that had not been read yet.

Its replicated data is **retained**, not deleted. Removing it would propagate a delete to every client's local database, which cannot be undone if the change was a mistake or part of a drop-and-recreate, and would leave rows in other tables referencing it dangling. Both changes are also observed in the catalog rather than the change stream, so there is no LSN at which a delete could be shown to be safe. A redeploy is what actually removes the data.

No runtime schema change deletes replicated data. The only removal is at resolution time, on deploy.

### MSSQL capture-instance pinning

SQL Server CDC can have two capture instances for one physical table, each with its own change table and captured schema. The MSSQL reconciler pins new bindings to a specific capture instance:

The governing rule is that **a sync-config table without a usable capture instance stops the replication job.** Serving checkpoints while one of the configured tables cannot be replicated would emit broken data to clients, and clients cannot detect it: a bucket that is empty because its table is not replicating looks exactly like a bucket whose table has no rows. The client syncs to the checkpoint and treats it as complete. A stopped job is the recoverable failure; a silently incomplete checkpoint is not. Rules, evaluated in order:

- **No capture instance available at all:** fail with `PSYNC_S1601`. Applies both to a table that has never been pinned (CDC not enabled yet) and to one whose only instance was dropped.
- **New binding** (no compatible candidates): pin to the newest available capture instance and persist `{ captureTableObjectId }`.
- **Legacy binding** (compatible candidates all lack metadata): update them in place to pin the newest available capture instance.
- **Pinned binding, instance still available** (compatible candidates share one capture identity): keep them compatible and persist the same identity. A newer instance is never adopted while the bound one is usable.
- **Pinned binding, instance dropped**: fail with `PSYNC_S1601`, whether or not a replacement exists. A replacement may capture a different schema, so adopting it in place would silently change what is replicated; adopting one requires deploying the sync configuration as a new replication stream.
- **Invalid state** (mixed metadata-free + pinned, or multiple pinned identities): fail with a diagnostic error.

At job startup, table-cache population resolves every configured table before streaming, which also ensures legacy records have a capture-instance pin. A sync-config table with no capture instance fails the job with `PSYNC_S1601` rather than being skipped — this deliberately differs from Postgres publication handling, for the reason above. The schema check applies the same rule at runtime: it lists each pattern's tables and fails if one has no capture instance, which is the only way to notice a table created after startup without CDC (wildcard patterns can match one, and new-table detection sees only tables that already have a capture instance). Tables already being replicated are excluded there, so a capture instance lost from under a running table is diagnosed by the more specific path below. At runtime a pinned stream polls its bound capture instance and uses that instance's minimum LSN for retention checks.

Because both cases fail, startup does not need to distinguish a never-pinned table from one whose pin was dropped, and therefore never needs to load persisted records before deciding to skip.

The two capture-instance changes a running stream can see are handled differently:

- **A newer instance appears while the bound one is still available** (`NEW_CAPTURE_INSTANCE`): log a single warning (throttled per newer instance) that a redeploy is required, and keep polling the bound instance. Adopting the new schema mid-stream would silently change what is replicated.
- **The bound instance is gone** (`MISSING_CAPTURE_INSTANCE`): fail the whole replication job with `PSYNC_S1601`, whether or not a replacement exists. The event still carries `newCaptureInstance` when there is one, but only to tell the operator which remedy applies — re-enable CDC, or redeploy to adopt the replacement.

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
