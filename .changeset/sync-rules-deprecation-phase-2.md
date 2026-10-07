---
'@powersync/service-sync-rules': patch
'@powersync/service-core': patch
---

Report a non-fatal deprecation warning when a sync config uses legacy Sync Rules (`bucket_definitions:`). It appears in the `/api/admin/v1/diagnostics` and `/api/admin/v1/validate` responses with `level: 'warning'`, and the service now logs sync config errors and warnings when it loads or deploys a sync config.
