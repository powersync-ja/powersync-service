---
'@powersync/service-core': patch
'@powersync/service-image': patch
---

Fix websocket auth errors not correctly propagating the details, previously resulting in generic "[PSYNC_S2106] Authentication required" messages.
