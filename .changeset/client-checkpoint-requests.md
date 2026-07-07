---
'@powersync/lib-services-framework': minor
'@powersync/service-core': minor
'@powersync/service-core-tests': minor
'@powersync/service-module-convex': minor
'@powersync/service-module-mongodb': minor
'@powersync/service-module-mongodb-storage': minor
'@powersync/service-module-mssql': minor
'@powersync/service-module-mysql': minor
'@powersync/service-module-postgres': minor
'@powersync/service-module-postgres-storage': minor
'@powersync/service-types': minor
---

Add `/sync/checkpoint-request` for client-supplied checkpoint request ids, previously called write checkpoint ids. The route returns the stored `checkpoint_request_id`, storage now treats managed request ids as monotonic per user/client, custom checkpoint request ids continue to use the existing `checkpoint` field for backwards compatibility, and `checkpoint_requested_at` metadata lets compact jobs remove expired request-derived checkpoint records.

This release includes storage migrations for the checkpoint request metadata. Self-hosters should run migrations as part of the upgrade.
