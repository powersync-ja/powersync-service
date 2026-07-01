---
'@powersync/service-core-tests': patch
'@powersync/service-core': patch
---

Fix edge case where the service can return incomplete data. This could happen when using bucket priorities, and a checkpoint is invalidated by the compact process while syncing.
