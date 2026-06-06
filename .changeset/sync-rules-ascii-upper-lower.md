---
'@powersync/service-sync-rules': patch
---

Make `upper()` and `lower()` ASCII-only in the JS evaluator to match SQLite's default behaviour. Previously the server used `String.prototype.toUpperCase()` / `.toLowerCase()`, which are Unicode-aware and perform length-changing case folds (ß -> SS, fi -> FI, İ -> İ̇). The client SQLite uses ASCII-only semantics for the same calls, so server-side bucket keys silently disagreed with client-side parameter values for any source data containing non-ASCII letters.
