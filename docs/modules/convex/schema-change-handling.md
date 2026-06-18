# Convex schema change handling

This note explains why the Convex replicator does not implement the same
schema-change detection flow used by relational and CDC-based replicators.

## What conventional schema change handling is for

In source modules such as Postgres, MySQL, MongoDB, and SQL Server, schema or
relation change handling is used to keep PowerSync's source table metadata in
step with the source database. Typical reasons include:

1. detecting changed replica identity columns,
2. refreshing cached table or relation metadata,
3. handling table rename, drop, and truncate events,
4. detecting source object identity changes,
5. deciding whether a table must be dropped, truncated, or re-snapshotted,
6. updating column metadata used by row decoding.

Those concerns matter when the replication stream contains DDL, relation
messages, capture-instance changes, table ids, or column metadata that affect how
PowerSync should interpret subsequent row changes.

## Why Convex is different

Convex replication uses the Streaming Export APIs:

- `json_schemas` for table-name discovery and schema preview,
- `list_snapshot` for initial table snapshots,
- `document_deltas` for ongoing changes.

The replicated row shape comes from the actual JSON document returned by
`list_snapshot` and `document_deltas`. It does not come from `json_schemas`.
The Convex row converter iterates over the document payload, skips PowerSync's
internal Convex metadata fields, and converts the JSON-compatible values into
Sync Streams input values.

That means ordinary Convex document shape changes flow through as data changes:

- if a field is added, later snapshot/delta payloads include the field,
- if a field is removed, later payloads omit the field,
- if a field changes type, later payloads carry the new JSON value,
- if a migration updates existing documents, those updates are normal Convex
  writes and should appear in `document_deltas`.

Convex tables also always use `_id` as the replication identity in this module.
There is no source-specific primary key or replica identity definition to track,
so the usual "replica identity changed" path does not apply.

## How table discovery works

Exact table patterns in Sync Streams rules do not need `json_schemas` for
replication. The table name is already present in the sync rule, and the stream
can resolve that table directly.

Wildcard table patterns do need a source of table names during initial
replication. For those patterns, the Convex replicator uses `json_schemas` to
expand the wildcard into concrete table names before snapshotting.

An integration test verifies the key assumption behind that behavior:
`json_schemas` lists schema-defined tables even when those tables contain no
documents. This means initial wildcard expansion can discover empty tables
without waiting for a first row write.

## New tables while streaming

When `document_deltas` contains a row for a table that is not in the relation
cache, the replicator checks whether that table is selected by the current Sync
Streams rules. If it is selected, the table is resolved in storage and marked as
snapshot-complete at the current delta cursor.

The replicator does not run an inline snapshot for that newly observed table.
The reasoning is:

1. existing wildcard-selected tables were already discovered through
   `json_schemas` during initial replication,
2. a table that appears later in `document_deltas` has appeared because a source
   write occurred,
3. the delta payload contains the full document state needed for replication,
4. an empty newly-created table has no data to snapshot.

So the delta stream itself is the source of truth for newly observed table data
after the initial snapshot boundary.

## When a re-snapshot is still needed

Convex does not need a re-snapshot merely because a field was added, removed, or
changed type. Those are document changes and should replicate through normal
deltas.

A re-snapshot is still required for cases that change the selected data set or
invalidate the source cursor model:

- initial replication of tables selected by the current Sync Streams rules,
- a Sync Streams deployment that selects new existing data,
- restarting initial replication from an incomplete snapshot,
- a lost or expired Convex cursor that requires restarting from a fresh
  snapshot boundary.

These are selection or consistency-boundary events, not schema-metadata drift
events.

## Table drops

The Convex stream does not continuously diff `json_schemas` to detect that a
table has disappeared from the source schema.

Validation against the Convex dashboard showed that deleting a table from the
dashboard does not emit per-document `_deleted` rows in `document_deltas`. That
means PowerSync will not automatically remove previously replicated rows for that
table, and clients can continue to see stale synced data.

If a table needs to be decommissioned while preserving replication correctness,
clear the table before deleting it. In the Convex dashboard, use the "Clear
Table" action first, then delete the table after those document removals have
replicated. Deleting documents through Convex mutations is also valid when that
path emits document delete deltas. Otherwise, treat dashboard table deletion or
schema-only table removal as a sync-rule/deployment state change: review the
affected rules and re-replicate or clear affected PowerSync state as needed.

## Role of `json_schemas`

`json_schemas` remains useful for:

- validating that the Convex deployment is reachable,
- checking that required support tables such as `powersync_checkpoints` exist,
- presenting database schema information through API/debug routes,
- expanding wildcard table patterns at initial replication time.

It is intentionally not used for runtime row decoding or schema drift detection.
Convex `json_schemas` can omit field detail until data exists, and using it for
type coercion would make replicated values depend on metadata availability
rather than on the source JSON payload.

## Summary

For Convex, schema changes are best understood as document-shape changes rather
than DDL events. Since PowerSync replicates the JSON payloads directly and `_id`
is always the replica identity, conventional schema-change detection has little
value for correctness.

The important guarantees are instead:

1. initial wildcard expansion can discover all schema-defined tables,
2. snapshot reads use a pinned Convex snapshot cursor,
3. streaming reads the unfiltered global `document_deltas` cursor in order,
4. new selected tables observed in deltas are resolved without an inline
   snapshot,
5. migrations that update data are replicated as normal document changes.
