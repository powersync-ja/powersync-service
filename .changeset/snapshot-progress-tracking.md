---
'@powersync/service-core': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres-storage': patch
---

Show the replication rate and estimated time remaining in snapshot progress log lines, for example `Replicating public.orders 1.2M/~5.0M (24%, 8.1k records/s, ETA 7m49s)`. Postgres storage now stores snapshot row counts as 64-bit integers, so progress is tracked correctly for tables with more than about 2.1 billion rows.
