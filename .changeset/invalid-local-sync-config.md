---
'@powersync/service-core': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres-storage': patch
---

Reject sync configs with fatal parsing or source-capability errors before they enter replication. A context-owned `SyncConfigEngine` shares one source-aware parsed config and validation result between replication startup, diagnostics, and manual reprocess guards. The engine loads filesystem, Base64, or inline input on first use and caches the result.

With `exit_on_error: false`, a rejected startup submission stops pending processing work while preserving active replication, including when active and processing configs share a replication stream. This also stops invalid pending configs persisted by older services. Rejected content is exposed through diagnostics `deploying_sync_rules`. With `exit_on_error: true`, validation failures stop startup before storage changes. Warnings remain advisory.

API-managed deployments share validation and continue rejecting fatal submissions before storage changes. Rejected API requests do not cancel accepted pending work or create a local diagnostic overlay.
