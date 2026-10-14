---
'@powersync/service-core': patch
---

Skip persisting and processing sync configs supplied through service configuration (file, Base64, or inline YAML) with fatal validation errors when `exit_on_error: false`. Existing active replication continues, including recovery and manual reprocessing of the active config.

Expose invalid local config and validation errors through diagnostics `deploying_sync_rules`. For filesystem deployments, the API and replicators should use the same sync config file or identical mounted copies. Service configuration resolution reads and validates the local config once. Replication and diagnostics share that snapshot until restart. It cannot report validation errors from a file unavailable to that instance. Existing pending deployments are retained in storage.
