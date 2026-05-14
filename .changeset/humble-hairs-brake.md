---
'@powersync/service-sync-rules': patch
---

Fix IN / NOT IN against a scalar JSON array so it applies SQLite type for booleans and integers above 2^53
