---
'@powersync/service-module-postgres-storage': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-core-tests': patch
'@powersync/service-module-postgres': patch
'@powersync/service-module-mongodb': patch
'@powersync/service-core': patch
'@powersync/service-module-mssql': patch
'@powersync/service-module-mysql': patch
'@powersync/service-sync-rules': patch
---

Store compiled sync plans in bucket storage to deserialize them instead of recompiling on service startup.
