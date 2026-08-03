---
'@powersync/service-module-postgres-storage': patch
---

Delete rows in batches when clearing storage for a replication stream. A single DELETE covering the entire group can run for hours on large deployments and never complete once it exceeds statement or socket timeouts; batched deletes make durable progress and can be safely retried or aborted.
