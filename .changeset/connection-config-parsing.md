---
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
'@powersync/service-module-mongodb': minor
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

Validation and deployment reject filters when the feature is unavailable. Changing source-table options requires replacement processing when deploying a new sync config.
