---
'@powersync/service-module-mongodb-storage': patch
---

Overlap V3 chunk compaction across concurrent buckets during initial replication and normal scheduled compaction. Configure the shared worker limit with `storage.chunk_compaction_concurrency` (default: 4 with object storage, otherwise 2). Full compactions remain sequential within each job.
