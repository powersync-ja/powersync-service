---
'@powersync/service-module-postgres': patch
'@powersync/service-module-mongodb': patch
'@powersync/service-module-mysql': patch
'@powersync/service-module-mssql': patch
'@powersync/service-module-convex': patch
---

Use the context-owned `SyncConfigEngine` for configured sync config loading and validation, sharing source-specific parsing defaults and capability checks with diagnostics. Reject new sync configs with fatal errors before storing them for replication, while retaining diagnostics when `exit_on_error` is disabled.
