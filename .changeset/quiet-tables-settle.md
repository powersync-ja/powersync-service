---
'@powersync/service-module-postgres-storage': patch
'@powersync/service-module-postgres': patch
'@powersync/service-core': patch
---

Fix Postgres replication with Postgres storage getting stuck in `PSYNC_S1101` after interrupted initial replication when the publication contains tables that the sync config does not use. Snapshot completion now ignores persisted unused tables for the source connection, allowing existing affected deployments to recover on restart while still requiring snapshots for tables used by the sync config.
