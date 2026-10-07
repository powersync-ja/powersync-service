---
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
'@powersync/service-types': minor
'@powersync/service-module-mongodb': minor
'@powersync/service-module-postgres': patch
'@powersync/service-module-mysql': patch
'@powersync/service-module-mssql': patch
'@powersync/service-module-convex': patch
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-postgres-storage': patch
'@powersync/lib-services-framework': patch
---

Add `config.source_table_options` to edition 3 sync configs, including MongoDB pre-filtering expressions. Generate editor schemas with examples and report validation errors at their YAML source locations.

MongoDB replication pre-filtering is only available in the Team and Enterprise editions. For example:

```yaml
config:
  edition: 3
  source_table_options:
    orders:
      mongodb_filter_expression:
        $and:
          - $eq: ['$$doc.active', true]
          - $in: ['$$doc.status', ['pending', 'shipped']]
streams:
  orders:
    query: SELECT * FROM orders
```

Validation and deployment reject filters when the feature is unavailable. Postgres, MySQL, SQL Server, and Convex sources reject MongoDB pre-filtering expressions. Filesystem-loaded sync configs also run source-capability checks. With `exit_on_error` disabled, configs remain available to diagnostics while fatal source errors block replication. Changing source-table options requires replacement processing when deploying a new sync config.

Source capability validation returns structured `ValidationDiagnostic` entries, allowing multiple fatal issues and
advisory warnings. Deployment and reprocessing reject fatal diagnostics while validation continues collecting
independent findings. Existing `ReplicationError` consumers retain the same diagnostic shape.
