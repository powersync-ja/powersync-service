---
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres-storage': patch
'@powersync/lib-services-framework': patch
---

Add `config.source_tables` to edition 3 sync configs. This flat map uses `table`, `database.table`, or `connection.database.table` keys. Core accepts empty table options; module-specific options require a registered parser that declares and parses them.

Connection prefixes are parsed and stored, but explicitly qualified patterns continue to use the `default` connection tag during hydration. This change does not enable selecting another replication connection through a prefix.

```yaml
config:
  edition: 3
  source_tables:
    my_table: {}
streams:
  my_table:
    query: SELECT * FROM my_table
```

Plans with parsed source-table options or required module IDs use sync-plan format 3. Plans without either retain formats 1 and 2. Changed source-table options require replacement processing when a new sync config is deployed.

Add a shared service-context parser with module registration hooks for schema extension, parsing, and persisted validation. Validation routes, deployments, replication, and saved-config loading use this parser. Saved plans fail to load when a required module is unavailable.
