---
'@powersync/service-core': minor
'@powersync/service-module-mongodb-storage': minor
---

Scope MongoDB v3 custom write checkpoint records and reads to the compiled event definition that created them. Custom checkpoint mode accepts a resolver that selects the required event id from the active sync config supplied by the client read path. This keeps active and incrementally processing checkpoint events separate when their definitions change.
