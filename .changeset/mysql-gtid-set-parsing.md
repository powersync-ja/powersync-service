---
'@powersync/service-module-mysql': patch
---

Fix GTID parsing for multi-server-UUID GTID sets. Previously a `gtid_executed` containing multiple server UUIDs (e.g. after a failover or restore) produced a `NaN` transaction id in the comparable LSN, which could permanently block checkpoint creation. GTID sets with multiple intervals per server UUID are now also parsed correctly. A warning is now logged when multiple server UUIDs are detected in the executed GTID set or on the binlog, since GTID-based LSN ordering across server UUIDs is not reliable.
