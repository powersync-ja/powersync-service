---
'@powersync/service-sync-rules': minor
'@powersync/service-module-postgres': minor
---

Add wildcard schema support to Sync Streams (e.g. `SELECT * FROM "%".materials`). A wildcard schema lets a single stream definition span every matching Postgres schema, exposing the per-row schema as the synthetic `_schema` value for use in filters and bucket parameters. This enables schema-per-tenant replication from a single replication slot, with the active schema resolved per client (e.g. from a JWT claim).
