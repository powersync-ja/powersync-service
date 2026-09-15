---
'@powersync/service-types': minor
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres-storage': patch
'@powersync/lib-services-framework': patch
---

Add `config.connections` to edition 3 sync configs and a shared service parser with generic module validation
hooks. This accepts the common connection and table structure, rejecting module-specific options unless their schema is
registered. Validate saved options before hydration and require replacement processing when connection options change
on sync config deployment. Configs without connection options retain their existing saved-plan format.
