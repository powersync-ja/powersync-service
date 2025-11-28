---
'@powersync/service-core': minor
'@powersync/service-module-mssql': minor
'@powersync/service-module-postgres-storage': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres': patch
'@powersync/service-errors': patch
'@powersync/service-module-mysql': patch
'@powersync/service-image': patch
---

- First iteration of MSSQL replication using Change Data Capture (CDC). 
- Supports resumable snapshot replication
- Uses CDC polling for replication

