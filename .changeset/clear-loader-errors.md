---
'@powersync/service-core': patch
---

Improve the error raised when loading an unrecognized connection or storage type: the message now lists the supported types, and is raised as a `ServiceError` with code `PSYNC_S3201` instead of a plain `Error`.
