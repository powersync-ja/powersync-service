---
'@powersync/service-module-postgres-storage': patch
---

Fix bug where listening to active checkpoint notifications on an ended connection could cause a crash.
