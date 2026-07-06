---
'@powersync/lib-services-framework': minor
'@powersync/service-core': minor
'@powersync/service-core-tests': minor
'@powersync/service-module-mongodb-storage': minor
'@powersync/service-module-postgres-storage': minor
'@powersync/service-types': minor
---

Add `/sync/checkpoint-request` for client-supplied write checkpoint request ids. The route returns the stored `checkpoint_request_id`, storage now treats managed request ids as monotonic per user/client, custom write checkpoint ids continue to use the existing `checkpoint` field, and `checkpoint_requested_at` metadata lets compact jobs remove expired request-derived checkpoint records.
