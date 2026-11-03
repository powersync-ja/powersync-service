---
'@powersync/service-module-postgres': patch
---

Enable logical replication slot failover automatically on PostgreSQL 17+, allowing replication slots to sync to physical standbys during switchovers. Failover can also be configured explicitly using the `slot_failover` connection option.
