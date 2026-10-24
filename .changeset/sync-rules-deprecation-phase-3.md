---
'@powersync/service-sync-rules': patch
---

Rename sync config error and schema exports: `SyncConfigErrors`, `SyncConfigProcessingError`, `SyncConfigErrorCode`, `syncConfigSchema` and `validateSyncConfigSchema`. The previous names remain as deprecated aliases, so existing imports and `instanceof` checks keep working.

This also renames new schema helpers from the separate PR in this release to `createSyncConfigSchema`, `compileSyncConfigSchemaValidator` and `SyncConfigSchemaValidator`. These have not been in a stable release, so they have no aliases.
