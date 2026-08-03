---
'@powersync/service-errors': minor
'@powersync/service-module-mongodb': patch
'@powersync/service-module-mysql': patch
'@powersync/service-module-mssql': patch
'@powersync/service-module-convex': patch
---

Throw a clear error (`PSYNC_R2201`) when a schema wildcard is used in a table pattern with MongoDB, MySQL, SQL Server or Convex connections, instead of silently discovering no tables.
