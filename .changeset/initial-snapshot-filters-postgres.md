---
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
'@powersync/service-module-postgres': minor
'@powersync/service-module-postgres-storage': patch
'@powersync/service-module-mongodb-storage': patch
---

Add `initial_snapshot_filters` support for Postgres sources.

A global `initial_snapshot_filters` section in the sync config maps table patterns
(including schema and table wildcards) to a SQL WHERE clause applied during the initial
snapshot, so the source database skips rows outside the filter instead of the service
reading and discarding them. The filter is applied in all snapshot query types, including
the chunked resume path.

Filters are also persisted alongside the compiled sync plan, so configs using
`edition: 3` restore them correctly when replication rebuilds the config from the stored
plan.
