---
'@powersync/service-types': minor
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres-storage': patch
'@powersync/lib-services-framework': patch
---

- Add `config.connections` to edition 3 sync configs. Empty connection config falls back to a Sync Plan version 3 - specifying connection config uses a new Sync Plan version 4. This is a generic connection and table structure for now. External/additional modules may declare module specific attributes/config in the future. The generic structure does not allow any specific configuration at this stage, specifying config without an additional module being loaded will result in a validation error.

```yaml
config:
  connections: # connections is added to the existing config entry
    tables:
      my_table: {} # no config is allowed here, unless an external module has registered it
```

- And a shared service context Sync Config parser with generic module validation hooks. This allows external modules to register additional configuration and validation. The shared parser is now used for all Sync Config parsing.
