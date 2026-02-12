---
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-core-tests': patch
'@powersync/service-core': patch
---

[MongoDB Storage] Optimize the compact job, avoiding re-compacting buckets in the same job.
