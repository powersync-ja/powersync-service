---
'@powersync/service-module-postgres-storage': patch
'@powersync/service-core': patch
---

Improve Postgres sync throughput by reading bucket data with ordered, per-bucket index range scans that stop once the requested batch is full.
