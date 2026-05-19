---
'@powersync/service-sync-rules': patch
---

Fix the `&&` (overlap) operator on JSON arrays so it correctly tests intersection. Also compares array elements with SQLite affinity so `&&` handles bigint/number mixes consistently.
