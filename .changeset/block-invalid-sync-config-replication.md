---
'@powersync/service-core': patch
'@powersync/service-module-mongodb': patch
'@powersync/service-module-postgres': patch
'@powersync/service-module-mysql': patch
'@powersync/service-module-mssql': patch
'@powersync/service-module-convex': patch
---

Prevent replication from starting when a persisted sync config contains fatal parsing or compilation errors. Previously, a config loaded with `exit_on_error: false` could replicate using a partially compiled plan despite those errors.

With `exit_on_error: false`, invalid configs remain available through diagnostics, but replication is blocked until the fatal errors are corrected. Replication also checks source capabilities at startup and reports failures through storage for diagnostics. Warnings do not block replication.
