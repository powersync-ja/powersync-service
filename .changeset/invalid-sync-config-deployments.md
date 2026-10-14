---
'@powersync/service-core': patch
'@powersync/service-core-tests': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres-storage': patch
---

Add an `INVALID` state for sync configs with fatal validation errors. Postgres and MongoDB bucket storage persist these configs on all storage versions without replicating them or serving them to clients.

Deployment and processing are now distinct: a deployed sync config can be persisted for diagnostics without being processed. Rename `getDeployingSyncConfig()` to `getProcessingSyncConfig()` to reflect that it returns only configs undergoing replication processing, excluding invalid deployments.

Add a storage `getSyncConfigStatus()` API returning `{ active, next }`, where `next` is the latest processing or invalid deployment and becomes `null` once the replacement activates. Diagnostics exposes the latest invalid deployment and its fatal errors through the existing `deploying_sync_rules` field; the HTTP response format is unchanged. Every newer deployment marks previous invalid configs as `STOP`, removing them from current diagnostics while retaining their config content and validation errors as history.

Repeated loads of an unchanged invalid config do not create duplicate deployments, and restoring the active config supersedes the invalid attempt. An unchanged invalid config that now parses without fatal errors, for example after a service upgrade, is deployed again.

Every new sync config deployment supersedes pending processing work, including an invalid deployment. For example, if config A is active and config B is still processing, deploying invalid config C stops B. A continues replicating and serving clients until a subsequent valid config becomes active. If there is no active config, cancelling the pending config leaves no config progressing toward activation until a valid config is deployed.

Manual reprocessing is rejected while a processing or invalid deployment is pending. An invalid deployment must be replaced by deploying a new sync config before requesting reprocessing.
