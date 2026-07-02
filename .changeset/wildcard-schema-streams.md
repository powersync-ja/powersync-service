---
'@powersync/service-sync-rules': minor
'@powersync/service-module-postgres': minor
---

Add wildcard schema support to Sync Streams (e.g. `SELECT * FROM "%".materials`). A wildcard schema lets a single stream definition span every matching Postgres schema, with the row's schema available as `"table".schema()` (and the matched wildcard table suffix as `"table".table_suffix()`) for use in filters and bucket parameters. This enables schema-per-tenant replication from a single replication slot, with the active schema resolved per client (e.g. from a JWT claim).
