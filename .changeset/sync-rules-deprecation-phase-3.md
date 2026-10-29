---
'@powersync/service-sync-rules': minor
---

Rename sync config error and schema exports. The previous names are removed, so update any direct imports:

- `SyncRulesErrors` → `SyncConfigErrors`
- `SyncRuleProcessingError` → `SyncConfigProcessingError`
- `SyncRulesErrorCode` → `SyncConfigErrorCode`
- `syncRulesSchema` → `syncConfigSchema`
- `validateSyncRulesSchema` → `validateSyncConfigSchema`
- `createSyncRulesSchema` → `createSyncConfigSchema`
- `compileSyncRulesSchemaValidator` → `compileSyncConfigSchemaValidator`
- `SyncRulesSchemaValidator` → `SyncConfigSchemaValidator`

Parsing and validation behavior is unchanged. The published `schema/sync_config.json` and `schema/sync_rules.json` files are unchanged.
