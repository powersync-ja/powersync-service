---
'@powersync/service-module-mysql': patch
---

Fix GTID parsing for multi-server-UUID GTID sets. Previously a `gtid_executed` containing multiple server UUIDs (e.g. after a failover or restore) produced a `NaN` transaction id in the comparable LSN, which could permanently block checkpoint creation. GTID sets with multiple intervals per server UUID are now also parsed correctly. LSN ordering now follows the connected server's own transaction counter (`@@server_uuid`) instead of the highest counter in the set, so a stale server UUID with a higher counter can no longer hang checkpoints. A warning is logged when a transaction from a different server UUID appears on the binlog (e.g. when connected to a replica), since those are not reliably ordered yet.
