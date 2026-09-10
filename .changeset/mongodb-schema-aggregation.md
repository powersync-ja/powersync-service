---
'@powersync/service-module-mongodb': patch
---

Infer MongoDB collection schemas using aggregation so large sampled documents are not transferred into service memory. Query each collection in its own database with up to four aggregations at a time, and limit each aggregation to 30 seconds of server execution time.

Omit the aggregation collation option on Azure DocumentDB, which does not support it.

Cache DocumentDB detection on the route adapter for schema discovery and checkpoint requests.
