---
'@powersync/service-sync-rules': patch
---

Fix `CAST(... AS INTEGER)` to truncate REAL values towards zero, matching SQLite. Previously a negative non-integer real was floored (`CAST(-3.7 AS INTEGER)` returned `-4` instead of `-3`), causing sync-rule expressions that cast negative reals to integer to diverge from the source database.
