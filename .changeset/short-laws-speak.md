---
'@powersync/service-module-mongodb': patch
---

Fix edge case where checkpoints are stalled after an internal MongoDB command retry, leading to large reported replication lag.
