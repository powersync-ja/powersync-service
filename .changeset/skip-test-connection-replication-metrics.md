---
'@powersync/service-module-postgres': patch
---

Skip the PostgreSQL replication-byte metrics recorder in `test-connection` mode so the command can run without `DATA_REPLICATED_BYTES`.
