---
'@powersync/service-module-mssql': minor
---

Pin new MSSQL source-table bindings to a specific CDC capture instance.

New bindings persist the capture-table object ID and restore that binding across restarts. A running
pinned stream warns when a newer capture instance appears but continues polling its bound instance;
adopting the new instance requires a new replication stream. Existing metadata-free bindings retain
their previous automatic capture-instance behavior and are not backfilled.
