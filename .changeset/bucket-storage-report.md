---
'@powersync/service-core': minor
'@powersync/service-types': minor
'@powersync/service-module-mongodb-storage': minor
'@powersync/service-core-tests': minor
'@powersync/service-client': minor
---

Add a `POST /api/admin/v1/bucket-report` admin endpoint reporting per-bucket operation counts, with rows and fragmentation derived from each bucket's last full compact (MongoDB storage; storage v1/v2 report operation counts only).
