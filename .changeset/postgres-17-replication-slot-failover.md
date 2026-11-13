---
'@powersync/service-module-postgres': patch
'@powersync/service-errors': patch
'@powersync/service-image': minor
---

Add the `slot_failover` boolean option for PostgreSQL sources, allowing replication slots to sync to physical standbys during switchovers.
