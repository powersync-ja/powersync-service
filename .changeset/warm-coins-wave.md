---
'@powersync/service-core': patch
---

Minor improvement to the HTTP liveness probe. The liveness probe will return an HTTP `200` response code when running in the `API` mode. Any HTTP response in the `API` mode indicates the service is running. When running in the `UNIFIED` mode the HTTP response code is `200` if the last `touched_at` timestamp value is less than 10 seconds ago - this indicates the replication worker is running.
