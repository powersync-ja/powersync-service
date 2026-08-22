---
'@powersync/service-module-postgres': minor
---

Add a `snapshot_socket_timeout` connection option for the idle timeout of snapshot connection sockets. Defaults to the previous fixed 30 seconds. When storage flushes stall the snapshot loop for longer than the timeout (for example when replicating while an active sync rules instance is streaming on the same storage), the source connection is killed mid-snapshot; raising the timeout avoids the reconnect cycle.
