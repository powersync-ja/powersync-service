---
'@powersync/service-sync-rules': patch
---

Report a non-fatal deprecation warning when a sync config uses legacy Sync Rules (`bucket_definitions:`). It is attached to the `bucket_definitions` key and surfaces in the `/api/admin/v1/diagnostics` and `/api/admin/v1/validate` responses with `level: 'warning'`.
