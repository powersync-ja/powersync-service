---
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres-storage': patch
'@powersync/lib-services-framework': patch
---

Add `config.connections` to edition 3 sync configs. The common structure allows a connection type and empty table options; module-specific options require a registered parser that declares and parses them.

```yaml
config:
  edition: 3
  connections:
    default:
      type: mongodb
      tables:
        my_table: {}
streams:
  my_table:
    query: SELECT * FROM my_table
```

Plans with parsed connection options or required module IDs use sync-plan format 3. Plans without either retain formats 1 and 2. Changed connection options require replacement processing when a new sync config is deployed.

Add a shared service-context parser with module registration hooks for schema extension, parsing, and persisted validation. Validation routes, deployments, replication, and saved-config loading use this parser. Saved plans fail to load when a required module is unavailable.
