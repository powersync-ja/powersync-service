---
'@powersync/service-sync-rules': patch
---

Rename sync config error and schema exports: `SyncConfigErrors`, `SyncConfigProcessingError`, `SyncConfigErrorCode`, `syncConfigSchema` and `validateSyncConfigSchema`. The previous names (`SyncRulesErrors`, `SyncRuleProcessingError`, `SyncRulesErrorCode`, `syncRulesSchema` and `validateSyncRulesSchema`) remain as deprecated aliases, so existing imports and `instanceof` checks keep working.
