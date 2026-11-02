---
'@powersync/service-module-mongodb-storage': patch
---

Evaluate parameter lookups in batches so requests with 996 or more lookups no longer exceed MongoDB's 1000-stage aggregation pipeline limit.
