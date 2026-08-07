---
'@powersync/service-module-mysql': minor
---

Improve MySQL GTID consistency and resume safety. PowerSync now derives replication heads from the active server's executed GTID set, validates stored GTIDs and binlog coordinates before resuming, and re-snapshots after source rewinds or server UUID changes. Replica sources and foreign-origin binlog transactions are rejected until multi-origin transaction ordering is supported, while heartbeat keepalives use the last committed position to prevent idle checkpoint stalls.
