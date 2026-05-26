---
'@powersync/module-mongodb-storage': minor
'@powersync/service-core': minor
---

Introduce chunked multi-op bucket documents with invariants and read-filtering tests in MongoDB storage.

Streaming V3 compactor — process buckets incrementally with byte-bounded batches, scoped deletes, and bounded transactions. Add `moveBatchByteLimit` option to `CompactOptions`.
