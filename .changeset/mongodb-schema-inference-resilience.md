---
'@powersync/service-module-mongodb': patch
---

Report a MongoDB collection without columns when sampling it exceeds the query memory limit, instead of failing the whole schema request. Sampling a small collection of large documents can hit this limit, which previously blocked sync config validation and deployment for the entire connection.
