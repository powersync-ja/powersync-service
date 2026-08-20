# Storage version 3 - Data structure

Storage v3 separates source replication stream state from immutable sync config definitions. That lets compatible sync config updates reuse one MongoDB change stream and the bucket or parameter data that is still valid.

## Replication stream

A replication stream represents one conceptual source replication job. For example, that may correspond to one Postgres logical replication stream, one MongoDB change stream, or generally one source-side process advancing checkpoints from an ordered stream of changes.

This does not refer to concurrency: a future implementation may use multiple underlying database streams while still presenting one replication stream to storage. In storage v3, the replication stream owns one stream-level operation sequence and one stream-level resume position.

The stream state lives in the `sync_rules` collection. Unlike v1 storage, the static sync config content is not stored directly on this document. Instead, `sync_rules.sync_configs` embeds per-config state for one or more sync config definitions.

Common states are:

1. One `ACTIVE` sync config in an `ACTIVE` stream.
2. One `ACTIVE` config plus one `PROCESSING` config in the same active stream during incremental reprocessing.
3. A separate `PROCESSING` stream when incremental reprocessing is not supported or the update is incompatible.

The sync API reads from a single active config. A processing config embedded in the same stream does not affect client checkpoints until activation.

Important stream-level fields:

- `sync_configs`: per-config checkpoint and lifecycle state.
- `last_persisted_op`: highest operation id persisted for the stream, whether or not it is visible through a checkpoint yet.
- `resume_lsn`: stream-level source position used to resume replication.

`resume_lsn` is shared by all configs in the stream. The per-config `last_checkpoint_lsn` values are consistency markers for client-visible checkpoints, not independent restart positions.

Multiple replication streams may still exist concurrently when:

1. Incremental reprocessing is not used, so nothing is shared between sync config versions.
2. The update is incompatible with the active stream.
3. The storage version changes, because each replication stream can only handle one storage version.

## sync_configs

Collection: `sync_configs`

Each document is an immutable sync config definition for storage v3. It stores:

1. The YAML content.
2. The serialized sync plan, when available.
3. The storage version.
4. The replication stream id it was created for.
5. The persisted `rule_mapping`.

A specific sync config definition never moves between replication streams. When a config is stopped or replaced inside a stream, the reference from `sync_rules.sync_configs` can be removed, but the definition document remains historical.

## rule_mapping

Each sync config definition has a `rule_mapping` that maps bucket data sources, parameter lookup sources, and events to stable ids within the replication stream.

Compatible incremental updates reuse ids for equivalent serialized bucket and parameter definitions. Event definitions use a normalized serialized-plan identity that excludes raw SQL and stored hash values while retaining expression ASTs and their external-data bindings. This ignores payload-query order, but otherwise uses the compiler's existing conservative expression equality. Added definitions receive new ids. Historical mappings are included when allocating new ids so dropped ids are not accidentally reused inside the same stream.

### Event mapping and matching

Event ids are opaque, hexadecimal counters scoped to one replication stream. Each sync config persists an event-name-to-id mapping. A new event receives one more than the largest event id in any current or historical mapping. The service does not calculate an id by hashing the event definition: a compatible event reuses the id recorded in an earlier mapping, while an incompatible event receives the next counter value. Historical mappings continue to reserve an id after its event is removed, preventing that value from later being assigned to a different event definition in the same stream.

When deploying a sync config, each event is matched independently against events in compatible active configs using its name and serialized compiled behavior. Matching includes source tables, filters, and projected payloads while ignoring raw SQL formatting, event-definition order, and payload-query order. Expression operands retain the compiler's existing ordered comparison, so reordering operands or clauses is conservatively treated as a change. A match reuses the existing id; otherwise, a new id is allocated. Consequently, reordering unchanged `event_definitions` does not create ids or snapshot work. Ordering only determines which opaque counter values are assigned when multiple genuinely new events are introduced together.

Source-table documents store event ids as memberships. For any matching physical table, one event id belongs to at most one source-table document, although different documents may own and evaluate different events. Evaluating one event may produce multiple payloads when several payload queries match the row. A reused id can reuse existing snapshot-complete source-table coverage. A new or changed event has a new, uncovered id, so reconciliation creates separate source-table snapshot work for it. The in-memory reverse mapping from event id to sync config ids ensures that this work affects only configs using that id. Compatibility matching must remain conservative: failing to match only causes another snapshot, while incorrectly matching could reuse incompatible event state.

These ids are used by:

1. `bucket_data_${stream_id}_${definition_id}` collections.
2. `parameter_index_${stream_id}_${index_id}` collections.
3. `bucket_state_${stream_id}` keys.
4. `source_table_${stream_id}` membership arrays.

## source_table

Scoped to a replication stream.

Collection: `source_table_${stream_id}`

There may be multiple copies per physical table per stream. This is how incremental reprocessing snapshots new bucket, parameter, or event definitions without reprocessing already-compatible definitions.

Each source table document stores:

1. Source identity and replica identity metadata.
2. Snapshot state.
3. `bucket_data_source_ids`: bucket definitions covered by this source table.
4. `parameter_lookup_source_ids`: parameter indexes covered by this source table.
5. `event_definition_ids`: assigned event definition ids covered by this source table.

Memberships are narrowed when stopped configs are cleaned up. New memberships are covered by creating a new source table document rather than expanding an existing one. An event-only source table has empty bucket and parameter memberships and one or more event definition ids.

## source_records (previously current_data)

Scoped to a source_table in a replication stream.

Collection: `source_records_${stream_id}_${source_table_id}`

The `_id` field is now the source row id. Unlike V1 storage model, this does not include `g` (group_id) or `t` (table id), since those are already encapsulated in the collection name.

When a table is dropped, we first create relevant REMOVE operations, then drop the relevant current data collection.

When stopped config cleanup removes definitions, source records are kept only while the source table still has live data or parameter memberships. If a retained source table becomes event-only, or if all definitions for a source table are removed, the corresponding source records collection can be dropped.

## bucket_data

Scoped by replication stream and definition id.

Collection: `bucket_data_${stream_id}_${definition_id}`

`_id.g` is removed, since this is encapsulated in the collection name now.

`definition_id` is not tracked in storage V1.

The collection is dropped when stopped config cleanup determines that no live sync config still uses that definition id.

## parameter_index (previously bucket_parameters)

Scoped by replication stream and index definition.

Collection: `parameter_index_${stream_id}_${index_id}`

_Also_ indexed by compound `key`, which includes {t: source_table_id, k: source_record_key}

The `lookup` array drops the first two fields compared to V1 lookups (lookupName and queryId), since those are encapsulated in `index_id` in the collection name. In-memory, we use lookupName = indexId, queryId = '' (may change in the future).

The collection is dropped when stopped config cleanup determines that no live sync config still uses that parameter index id.

## bucket_state

Scoped by replication stream.

Collection: `bucket_state_${stream_id}`.

`_id` is now compound: `{d: <definition id>, b: <bucket name>}` (previously `{g, b}`)

Bucket state documents for a definition id are removed when stopped config cleanup drops that definition.

## custom checkpoint requests

Scoped by replication stream and compiled event definition.

Collection: `custom_checkpoint_requests_${stream_id}_${event_id}`

Each collection stores one custom checkpoint per user for an event definition. An event id is required for all v3
custom checkpoint reads and writes. Reused event definitions share their
collection across active and processing sync configs, while changed definitions use separate collections until
activation. Collections and their indexes are created lazily when the first custom checkpoint for an event is
flushed; unrelated events do not get a collection. Stopped sync config cleanup drops a collection once no live sync
config uses its event definition.

## Cleanup of stopped sync configs

Incremental streams can contain stopped sync config state while the stream continues serving live config state. Cleanup compares stopped configs with live configs using their persisted mappings. Live means `ACTIVE`, `PROCESSING`, or `ERRORED`.

1. Bucket data collections, parameter index collections, and bucket state are removed only for ids no live config still uses.
2. Source table memberships for unused ids are removed from retained source tables.
3. Unused event definition ids are removed from source table memberships using the same stopped-versus-live comparison.
4. Custom checkpoint request collections for unused event definition ids are dropped.
5. Source tables whose data, parameter, and event memberships become empty are deleted with their source records collections.
6. Source tables kept only by event memberships become event-only; their source records collections are dropped.
7. The stopped sync config entries are pruned from `sync_rules.sync_configs`.
