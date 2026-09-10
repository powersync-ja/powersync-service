---
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-core': patch
---

Concurrent storage version 4 chunk-merge compaction across buckets during. Configure the shared worker limit with `storage.chunk_compaction_concurrency` (default: 4 with object storage, otherwise 2). Full compactions remain sequential within each job.
