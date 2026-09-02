---
'@powersync/service-core': minor
'@powersync/service-types': minor
'@powersync/service-rsocket-router': minor
'@powersync/service-module-core': minor
---

Add a `powersync_sync_connections_total` counter with bounded `outcome`, `close_reason`, `error_code`, and `transport` labels. Normal disconnects and server-initiated closes count as successes, stream failures as errors, and pre-stream service-unavailable, missing-sync-config, and storage-query failures as rejections on both transports. RSocket concurrency-limit failures also count as rejections.
