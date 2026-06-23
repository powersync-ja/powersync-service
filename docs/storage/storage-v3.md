# Storage version 3 - Data structure

Storage v3 separates source replication stream state from immutable sync config definitions. That lets compatible sync config updates reuse one MongoDB change stream and the bucket or parameter data that is still valid.

## Replication stream

A replication stream represents one conceptual source replication job: one ordered stream of source changes, one stream-level operation sequence, and one stream-level resume position.

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

Each sync config definition has a `rule_mapping` that maps bucket data sources and parameter lookup sources to stable ids within the replication stream.

Compatible incremental updates reuse ids for equivalent serialized bucket and parameter definitions. Added definitions receive new ids. Historical mappings are included when allocating new ids so dropped ids are not accidentally reused inside the same stream.

These ids are used by:

1. `bucket_data_${stream_id}_${definition_id}` collections.
2. `parameter_index_${stream_id}_${index_id}` collections.
3. `bucket_state_${stream_id}` keys.
4. `source_table_${stream_id}` membership arrays.

## source_table

Scoped to a replication stream.

Collection: `source_table_${stream_id}`

There may be multiple copies per physical table per stream. This is how incremental reprocessing snapshots new bucket or parameter definitions without reprocessing already-compatible definitions.

Each source table document stores:

1. Source identity and replica identity metadata.
2. Snapshot state.
3. `bucket_data_source_ids`: bucket definitions covered by this source table.
4. `parameter_lookup_source_ids`: parameter indexes covered by this source table.

Memberships are narrowed when stopped configs are cleaned up. New memberships are covered by creating a new source table document rather than expanding an existing one. Empty memberships represent an event-only source table.

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

## Cleanup of stopped sync configs

Incremental streams can contain stopped sync config state while the stream continues serving a live config. Cleanup compares stopped configs with live configs using their persisted mappings:

1. Bucket data collections, parameter index collections, and bucket state are removed only for ids no live config still uses.
2. Source table memberships for unused ids are removed.
3. Source tables with no remaining data, parameter, or live event use are deleted with their source records collections.
4. Event-only source tables are deleted when no live sync config still triggers events for them.
5. The stopped sync config entries are pruned from `sync_rules.sync_configs`.
