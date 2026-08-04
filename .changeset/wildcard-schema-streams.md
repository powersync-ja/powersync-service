---
'@powersync/service-sync-rules': minor
'@powersync/service-module-postgres': minor
---

Support wildcard schemas in Sync Streams (e.g. `SELECT * FROM "%".assets`), with the matched schema available as `assets.schema()`, the table name as `assets.table_name()` and the matched wildcard table suffix as `assets.table_suffix()` for use in filters and bucket parameters.
