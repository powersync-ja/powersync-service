---
'@powersync/service-module-postgres-storage': patch
---

Insert bucket data and parameter data rows while a replication batch accumulates, instead of holding all rows until the end of the transaction. This bounds the memory used by a single source row evaluated into many buckets by `batch_limits.max_estimated_size`.
