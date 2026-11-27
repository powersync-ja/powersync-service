---
'@powersync/service-module-mongodb': patch
---

Use the highest matching `_id` as an upper bound for prefiltered MongoDB snapshot queries. This can avoid scanning an unrelated tail of the collection on the final page. If the upper-bound lookup times out, the snapshot continues without the bound.
