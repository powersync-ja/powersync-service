---
'@powersync/service-module-mssql': minor
---

Pin MSSQL source-table bindings to a specific CDC capture instance.

Source tables now persist the capture-table object ID and restore that binding across restarts. A running
pinned stream warns when a newer capture instance appears but continues polling its bound instance;
PowerSync no longer automatically adopts CDC capture-instance changes. Schema changes that create a
replacement capture instance therefore require redeploying the sync configuration as a new replication
stream. Existing metadata-free bindings are backfilled at job startup with the same capture instance
the previous streaming logic would have selected.
