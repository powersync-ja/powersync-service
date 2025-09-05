---
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-core': patch
'@powersync/service-image': patch
---

Avoid frequent write checkpoint lookups when the user does not have one.
