---
'@powersync/service-module-postgres': minor
---

Add a `snapshot_concurrency` connection option to snapshot multiple tables in parallel during initial replication, and pipeline chunk flushes so that storage writes overlap with reading the next chunk from the source database. Defaults to 1 (sequential).
