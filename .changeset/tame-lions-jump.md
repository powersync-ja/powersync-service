---
'@powersync/service-sync-rules': patch
---

Deduplicate inclusion reasons when merging buckets reached through more than one branch (e.g. overlapping subscriptions, or both a static and a dynamic parameter query), avoiding duplicate `subscriptions` entries on the wire.
