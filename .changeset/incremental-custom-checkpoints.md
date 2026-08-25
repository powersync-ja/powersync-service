---
'@powersync/service-core': minor
'@powersync/service-module-mongodb-storage': minor
---

Scope MongoDB v3 custom write checkpoint records and reads to the stream-assigned event definition that created them. Custom checkpoint mode names the event whose id is resolved through the active sync config's persisted mapping, while event handlers copy the assigned id from replication event payloads when writing checkpoints. This keeps active and incrementally processing checkpoint events separate when their definitions change.
