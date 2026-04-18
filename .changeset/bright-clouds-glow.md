---
'@powersync/service-module-mongodb': minor
---

Add experimental Cosmos DB MongoDB vCore support. Auto-detects Cosmos DB via `hello` command and applies workarounds for missing `clusterTime`, `operationTime`, and unsupported change stream features.
