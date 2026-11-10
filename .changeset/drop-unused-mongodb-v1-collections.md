---
'@powersync/service-module-mongodb-storage': patch
---

Drop unused MongoDB storage v1/v2 collections during stopped-stream cleanup instead of deleting their records individually when no other streams need them. A migration similarly removes existing unused collections. Initialize these collections only when a v1/v2 stream starts, so new v4 instances do not create them.
