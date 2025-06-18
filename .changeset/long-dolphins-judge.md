---
'@powersync/service-module-postgres-storage': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres': patch
'@powersync/service-module-mongodb': patch
'@powersync/service-core': patch
'@powersync/service-module-mysql': patch
---

Cleanly interrupt clearing of storage when the process is stopped/restarted.
