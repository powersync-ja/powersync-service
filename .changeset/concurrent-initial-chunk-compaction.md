---
'@powersync/service-module-mongodb-storage': patch
---

Overlap V3 initial chunk compaction across concurrent buckets. Configure the shared worker limit with `storage.chunk_compaction_concurrency` (default: 2).
