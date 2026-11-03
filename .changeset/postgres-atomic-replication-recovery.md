---
'@powersync/service-module-postgres-storage': patch
---

Prevent replication recovery from cancelling a newer sync config deployment. Check active and processing configs and create any recovery replacement in one transaction, using a table lock to serialize concurrent sync config state changes. This preserves valid deployments submitted while recovery is starting, including after an invalid configured sync config cancels pending processing.
