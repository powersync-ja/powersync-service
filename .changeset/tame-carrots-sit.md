---
'@powersync/service-core': patch
'@powersync/service-image': patch
---

Prevent premature `partial_checkpoint_complete` lines for Sync Stream subscriptions with custom priorities. These would cause checksum errors before.
