---
'@powersync/service-sync-rules': minor
'@powersync/service-core': minor
'@powersync/service-module-postgres-storage': patch
'@powersync/service-module-mysql': minor
---

Add snapshot_filter support for initial table replication

Users can now configure snapshot filters in sync_rules.yaml to apply WHERE clauses during initial table snapshots. This reduces storage and bandwidth for large tables where only a subset of rows match sync rules.

Features:

- Configure global filters for initial replication
- CDC changes continue to work normally, only affecting rows in storage
- Supports MySQL source with PostgreSQL storage

Example:

```yaml
# EXPLICIT: "I only want data from the last 90 days, period"
initial_snapshot_filters:
  orders:
    sql: 'created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)'

  logs:
    sql: "timestamp > NOW() - INTERVAL '7 days'"

bucket_definitions:
  # This works - queries recent orders
  recent_orders:
    data:
      - SELECT * FROM orders WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)

  # This will be EMPTY initially - and that's OK for logs
  all_logs:
    data:
      - SELECT * FROM logs
```
