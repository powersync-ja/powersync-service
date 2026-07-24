---
'@powersync/lib-service-postgres': patch
'@powersync/service-image': patch
'@powersync/service-jpgwire': patch
'@powersync/service-module-postgres-storage': patch
---

Reconnect PostgreSQL notification connections and restore `LISTEN` subscriptions when the underlying connection is terminated. Harden connection-slot retries and lease handling, and publish checkpoint notifications atomically with checkpoint updates.
