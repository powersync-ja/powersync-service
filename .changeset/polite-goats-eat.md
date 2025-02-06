---
'@powersync/service-errors': minor
'@powersync/service-module-mongodb': minor
'@powersync/service-image': minor
---

Added support for MongoDB resume tokens. This should help detect Change Stream error edge cases such as changing the replication connection details after replication has begun.
