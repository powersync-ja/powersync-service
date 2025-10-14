---
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-core': patch
'@powersync/service-image': patch
---

[MongoDB Storage] Only compact modified buckets. Add partial index on bucket_state to handle large numbers of buckets when pre-calculating checksums or compacting.
