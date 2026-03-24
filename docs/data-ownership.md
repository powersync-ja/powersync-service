# Storage version 3 - Data structure and "ownership"

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

Belongs to a replication stream.

Scope: `source_table_${stream_id}`

[FUTURE CHANGE] May have multiple copies per table per stream, especially when adding definitions.

[FUTURE CHANGE] We can remove a source definition from a source table, but never add one.

## source_records (previously current_data)

Owned by source table.

Scope: `source_records_${stream_id}_${source_table_id}`

The `_id` field, is the source row id. Unlike V1 storage model, this does not include `g` (group_id) or `t` (table id), since those are already encapsulated in the collection name.

When a table is dropped, we first create relevant REMOVE operations, then drop the relevant current_data collection.

[FUTURE CHANGE] If a _definition_ using a source table is removed:

1. We remove the bucket_data (drop the collection - see below).
2. We _don't_ update the source-records collection - stale records will remain. (Purely because this would be a slow operation, without gaining much)
3. When re-processing a source record, we then check for orphaned references.

When all definitions for a source table is removed, we remove the drop the corresponding source_records collection.

## bucket_data

Owned by replication stream.

Scoped by definition.

Scope: `bucket_data_${stream_id}_${definition_id}`

[FUTURE CHANGE] collection must be dropped when the definition is removed.

## parameter_index (previously bucket_parameters)

Owned by replication stream.

Scoped by definition.

Scope: `parameter_index_${stream_id}_${index_id}`

_Also_ indexed by compound `key`, which includes {t: source_table_id, k: source_record_key}

The `lookup` array drops the first two fields compared to V1 lookups (lookupName and queryId), since those are encapsulated in `index_id` in the collection name. In-memory, we use lookupName = indexId, queryId = '' (may change in the future).
