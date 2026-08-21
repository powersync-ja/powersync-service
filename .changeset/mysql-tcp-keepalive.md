---
'@powersync/service-module-mysql': patch
---

Enable TCP keepalive on the Zongji binlog and control connections and on the connection pool, so that idle connections are no longer silently dropped by stateful firewalls, which froze replication for ~950 seconds per occurrence with no error or health signal. Also add a periodic liveness probe on the control connection that restarts replication within about a minute when the connection stops responding, and bound the BinLog listener stop sequence so that a dead control connection cannot stall shutdown.
