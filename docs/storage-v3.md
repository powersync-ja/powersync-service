# Storage version 3 - Data structure

## Replication stream

A replication stream represents one conceptual replication "job":

1. One logical replication stream on the client in Postgres.
2. One change stream in MongoDB.
3. Generally, one entity creating checkpoints from a source database stream.

This does not refer to concurrency - we may add concurrency in each of these streams at a later point, which may use multiple underlying database streams. Instead, it just refers to conceptually having one replication job, advancing checkpoints one at a time.

Right now, each "sync config version", or `sync_rules` document, is one replication stream.

For incremental reprocessing, this will change so that multiple sync config versions can be processed by the same stream.

It is possible to have multiple replication streams running concurrently, for example when:

1. Incremental reprocessing is not used, so nothing is shared between the sync config versions.
2. Changing storage versions - each replication stream can only handle one storage version at a time.

## source_table

Scoped to a replication stream.

Collection: `source_table_${stream_id}`

[FUTURE CHANGE] May have multiple copies per phyisical table per stream, especially when adding definitions to a stream.

[FUTURE CHANGE] We can remove a source definition from a source table, but never add one.

## source_records (previously current_data)

Scoped to a source_table in a replication stream.

Collection: `source_records_${stream_id}_${source_table_id}`

The `_id` field is now the source row id. Unlike V1 storage model, this does not include `g` (group_id) or `t` (table id), since those are already encapsulated in the collection name.

When a table is dropped, we first create relevant REMOVE operations, then drop the relevant current_data collection.

[FUTURE CHANGE] If a _definition_ using a source table is removed:

1. We remove the bucket_data (drop the collection - see below).
2. We _don't_ update the source-records collection - stale records will remain. (Purely because this would be a slow operation, without gaining much)
3. When re-processing a source record, we then check for orphaned references.

When all definitions for a source table is removed, we remove the drop the corresponding source_records collection.

## bucket_data

Scoped by replication stream and definition id.

Collection: `bucket_data_${stream_id}_${definition_id}`

`_id.g` is removed, since this is encapsulated in the collection name now.

`definition_id` is new here - that is not tracked in storage V1.

[FUTURE CHANGE] collection must be dropped when the definition is removed.

## parameter_index (previously bucket_parameters)

Scoped by replication stream and index definition.

Collection: `parameter_index_${stream_id}_${index_id}`

_Also_ indexed by compound `key`, which includes {t: source_table_id, k: source_record_key}

The `lookup` array drops the first two fields compared to V1 lookups (lookupName and queryId), since those are encapsulated in `index_id` in the collection name. In-memory, we use lookupName = indexId, queryId = '' (may change in the future).

## bucket_state

Scoped by replication stream.

Collection: `bucket_state_${stream_id}`.

`_id` is now compound: `{d: <definition id>, b: <bucket name>}` (previously `{g, b}`)
