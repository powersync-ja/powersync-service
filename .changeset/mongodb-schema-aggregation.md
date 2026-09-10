---
'@powersync/service-module-mongodb': patch
---

Infer MongoDB collection schemas using aggregation so large sampled documents are not transferred into service memory. Query each collection in its own database, process databases sequentially, and limit each aggregation to 30 seconds of server execution time.
