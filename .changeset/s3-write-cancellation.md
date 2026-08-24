---
'@powersync/service-module-mongodb-storage': minor
'@powersync/service-core': minor
'@powersync/service-module-mongodb': patch
'@powersync/service-module-postgres': patch
'@powersync/service-module-mysql': patch
'@powersync/service-module-mssql': patch
'@powersync/service-module-convex': patch
---

Make object storage uploads and deletes cancellable. `ObjectStorage.put()` and `ObjectStorage.delete()` now accept an abort signal, alongside the existing `get()`, `list()` and `deletePrefix()`. Replication passes the replicator's abort signal via the new `CreateWriterOptions.signal`, and compaction passes its own, so in-flight object storage work is cancelled when replication or compaction stops instead of running to its timeout.
