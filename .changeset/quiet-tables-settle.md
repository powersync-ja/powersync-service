---
'@powersync/service-module-postgres-storage': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-sync-rules': patch
---

Prevent unused publication tables from blocking initial replication after an interruption with Postgres storage or MongoDB storage versions 1 and 2. Tables unused by the sync config no longer create persisted source-table records. Existing affected deployments must explicitly redeploy the sync config after upgrading; restarting the service or reloading unchanged configuration does not clear the existing state.
