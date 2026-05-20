---
"@powersync/service-sync-rules": patch
---

Fix Sync Streams JavaScript evaluator handling of `NOT NULL` and `NULL NOT IN (...)` to preserve SQLite `NULL` semantics.
