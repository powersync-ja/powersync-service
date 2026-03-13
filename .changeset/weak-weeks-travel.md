---
'@powersync/service-module-postgres-storage': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-core-tests': patch
'@powersync/service-core': patch
'@powersync/service-types': patch
---

Rename client connection reporting methods and top-level response keys to use clearer current connections, sessions, and summary naming, without changing the underlying storage schema.
