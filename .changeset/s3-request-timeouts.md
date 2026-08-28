---
'@powersync/service-module-mongodb-storage': minor
'@powersync/service-core': minor
'@powersync/service-module-mongodb': patch
'@powersync/service-module-postgres': patch
'@powersync/service-module-mysql': patch
'@powersync/service-module-mssql': patch
'@powersync/service-module-convex': patch
---

Apply timeouts and abortSignal handling to all S3 operations. The broad timeouts can be configured using the new `storage.object_storage.defaults_mode` option, or the `AWS_DEFAULTS_MODE` environment variable. Upgrade S3 SDK to fix further timeout issues.
