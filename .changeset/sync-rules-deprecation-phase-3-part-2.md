---
'@powersync/service-sync-rules': patch
---

Add `parseSyncConfig()` and `validateSyncConfig()` to parse and validate a sync config for either engine. `SqlSyncRules.fromYaml()` and `SqlSyncRules.validate()` remain as deprecated wrappers, and `SqlSyncRules` itself is now marked deprecated as the legacy Sync Rules implementation. When the YAML can't be parsed, `parseSyncConfig()` returns an empty `InvalidSyncConfig` instead of an empty `SqlSyncRules`.

Rename `SyncRulesOptions` to `SyncConfigParseOptions`, and `toSyncRulesValue`, `toSyncRulesRow` and `toSyncRulesParameters` to `toSyncConfigValue`, `toSyncConfigRow` and `toSyncConfigParameters`. The previous names remain as deprecated aliases.
