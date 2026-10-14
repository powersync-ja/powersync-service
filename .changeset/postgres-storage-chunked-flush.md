---
'@powersync/service-module-postgres-storage': patch
---

Fix replication failing with `RangeError: Invalid string length` when a single source row with large data is evaluated into many buckets. Bucket data and parameter data are now flushed in size-bounded INSERT statements, configurable with `batch_limits.max_statement_size`.
