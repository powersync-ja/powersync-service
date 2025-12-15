---
'@powersync/service-image': patch
'@powersync/service-module-postgres': patch
'@powersync/service-module-mongodb': patch
'@powersync/service-module-mysql': patch
'@powersync/service-module-mssql': patch
'@powersync/service-sync-rules': patch
'@powersync/service-jpgwire': patch
---

Add the `timestamp_max_precision` option for sync rules. It can be set to `seconds`, `milliseconds` or `microseconds` to restrict the precision of synced datetime values.
