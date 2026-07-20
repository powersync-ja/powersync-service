---
'@powersync/lib-service-postgres': patch
'@powersync/service-core': patch
'@powersync/service-jpgwire': patch
'@powersync/service-module-postgres-storage': patch
---

Reconnect PostgreSQL notification connections and restore `LISTEN` subscriptions when the underlying connection is terminated.
