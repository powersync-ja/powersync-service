---
'@powersync/service-core': patch
'@powersync/service-types': patch
'@powersync/service-schema': patch
---

Skip persisting and processing sync configs supplied through service configuration (file, Base64, or inline YAML) with fatal validation errors when `exit_on_error: false`. Existing active replication continues, including recovery and manual reprocessing of the active config.

Expose invalid local config and validation errors through diagnostics `deploying_sync_rules` without changing storage states. For filesystem deployments, the API and replicators should use the same sync config file or identical mounted copies. The core module reads and validates the local config during initialization. Replication and diagnostics share that snapshot until restart. It cannot report validation errors from a file unavailable to that instance. Existing pending deployments are retained in storage.

Add optional `sync_config.source: config | api` to declare deployment ownership explicitly. Explicit `source: config` reserves configuration-managed deployment before content is supplied, keeping API deployment disabled. Existing file, inline, and Base64 configurations remain supported without an explicit source; an omitted or empty section retains API deployment when no Base64 content is supplied.
