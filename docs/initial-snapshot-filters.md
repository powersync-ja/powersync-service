# Initial Snapshot Filters

Initial snapshot filters allow you to limit which rows are replicated during the initial snapshot phase. This is useful for large tables where you only need to sync a subset of data.

## Overview

Without snapshot filters, PowerSync replicates **all rows** from source tables during initial replication, even if your sync rules only match a small subset. Initial snapshot filters solve this by allowing you to specify which rows to replicate upfront.

**Important**: Filters are configured **globally** at the top level of your sync rules and apply to **all buckets** using that table. This means some buckets may end up empty if their queries don't align with the global filter.

## Syntax

Initial snapshot filters are defined at the **top level** of your `sync_rules.yaml` using an **object format** with database-specific filter syntax:

```yaml
initial_snapshot_filters:
  users:
    sql: "status = 'active'" # MySQL, PostgreSQL, SQL Server
    mongo: { status: 'active' } # MongoDB (BSON/EJSON format)

bucket_definitions:
  active_users:
    data:
      - SELECT id, name, status FROM users
```

You can specify just `sql` or just `mongo` depending on your database:

```yaml
initial_snapshot_filters:
  todos:
    sql: 'archived = false'

bucket_definitions:
  my_todos:
    data:
      - SELECT id, title FROM todos
```

## Basic Examples

### SQL Databases (MySQL, PostgreSQL, SQL Server)

```yaml
initial_snapshot_filters:
  users:
    sql: "status = 'active'"

bucket_definitions:
  active_users:
    data:
      - SELECT id, name, status FROM users
```

### MongoDB

```yaml
initial_snapshot_filters:
  users:
    mongo: { status: 'active' }

bucket_definitions:
  active_users:
    data:
      - SELECT id, name, status FROM users
```

### Multi-Database Support

Specify filters for both SQL and MongoDB sources:

```yaml
initial_snapshot_filters:
  users:
    sql: "status = 'active'"
    mongo: { status: 'active' }

bucket_definitions:
  active_users:
    data:
      - SELECT id, name, status FROM users
```

## Global Filter Behavior

⚠️ **Critical**: Filters are **global** and apply to **all buckets** using that table. This can result in empty buckets if the bucket query doesn't match the filter.

### Example: Misaligned Bucket Query

```yaml
initial_snapshot_filters:
  users:
    sql: "status = 'active'" # Only active users are replicated

bucket_definitions:
  active_users:
    data:
      - SELECT * FROM users WHERE status = 'active' # ✅ Will have data

  admin_users:
    data:
      - SELECT * FROM users WHERE is_admin = true # ⚠️ Will be EMPTY if admin users aren't active
```

In this example:

- Only users with `status = 'active'` are replicated (due to the global filter)
- The `admin_users` bucket will be **empty** because no admin users are included in the initial snapshot
- This is a **deliberate trade-off** between snapshot performance and bucket completeness

### When to Use Global Filters

✅ **Good use case**: All your buckets can work with a single filter

```yaml
initial_snapshot_filters:
  orders:
    sql: 'created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)' # Only recent orders

bucket_definitions:
  my_orders:
    data:
      - SELECT * FROM orders WHERE user_id = token_parameters.user_id
  pending_orders:
    data:
      - SELECT * FROM orders WHERE status = 'pending'
```

Both buckets work with recent orders only - the filter just improves snapshot performance.

❌ **Bad use case**: Different buckets need different data

```yaml
initial_snapshot_filters:
  users:
    sql: "status = 'active'" # Only active users

bucket_definitions:
  active_users:
    data:
      - SELECT * FROM users WHERE status = 'active' # ✅ Works
  archived_users:
    data:
      - SELECT * FROM users WHERE status = 'archived' # ⚠️ Will be EMPTY!
```

The `archived_users` bucket will never have data because archived users aren't included in the snapshot.

## Complex Filters

You can use any valid SQL WHERE clause syntax:

```yaml
initial_snapshot_filters:
  orders:
    sql: "created_at > DATE_SUB(NOW(), INTERVAL 30 DAY) AND status != 'cancelled'"

bucket_definitions:
  my_orders:
    data:
      - SELECT * FROM orders WHERE user_id = token_parameters.user_id
```

## Wildcard Table Names

You can use wildcards (%) in table names to match multiple tables:

```yaml
initial_snapshot_filters:
  logs_%:
    sql: 'created_at > DATE_SUB(NOW(), INTERVAL 7 DAY)'

bucket_definitions:
  recent_logs:
    data:
      - SELECT * FROM logs_2024
      - SELECT * FROM logs_2025
```

This applies the filter to `logs_2024`, `logs_2025`, etc.

## Schema-Qualified Names

You can specify schema-qualified table names:

```yaml
initial_snapshot_filters:
  public.users:
    sql: "status = 'active'"
  analytics.events:
    sql: "timestamp > NOW() - INTERVAL '1 day'"

bucket_definitions:
  active_users:
    data:
      - SELECT id, name FROM public.users
  recent_events:
    data:
      - SELECT * FROM analytics.events
```

## How It Works

1. **Initial Snapshot**: Only rows matching the filter are replicated during the initial snapshot phase
2. **CDC Replication**: After the snapshot, Change Data Capture (CDC) continues to replicate changes to **only the rows that were included in the snapshot**
3. **Storage**: Only filtered rows are stored in PostgreSQL/MongoDB storage
4. **Buckets**: All buckets using the filtered table will only see the filtered data

### Changing Filters Later

If you widen a filter after the initial snapshot, CDC will **not** backfill older rows that were previously excluded because those rows did not change. To include them, run a **targeted resnapshot** of the affected tables or perform a **one-time backfill** job, then let CDC keep the data up to date.

## Database-Specific Syntax

### MySQL

```yaml
initial_snapshot_filters:
  orders:
    sql: 'DATE(created_at) > DATE_SUB(CURDATE(), INTERVAL 30 DAY)'
```

### PostgreSQL

```yaml
initial_snapshot_filters:
  orders:
    sql: "created_at > NOW() - INTERVAL '30 days'"
```

### SQL Server

```yaml
initial_snapshot_filters:
  orders:
    sql: 'created_at > DATEADD(day, -30, GETDATE())'
```

### MongoDB

```yaml
initial_snapshot_filters:
  orders:
    mongo:
      created_at: { $gt: { $date: '2024-01-01T00:00:00Z' } }
      status: { $in: ['active', 'pending'] }
```

## Use Cases

### Large Historical Tables

```yaml
# Only sync recent orders instead of entire order history
initial_snapshot_filters:
  orders:
    sql: 'created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)'

bucket_definitions:
  my_orders:
    data:
      - SELECT * FROM orders WHERE user_id = token_parameters.user_id
```

### Active Records Only

```yaml
# Skip archived/deleted records
initial_snapshot_filters:
  users:
    sql: 'archived = false AND deleted_at IS NULL'

bucket_definitions:
  all_users:
    data:
      - SELECT id, name FROM users
```

### Tenant/Organization Filtering

```yaml
# Multi-tenant app - only sync specific tenants
initial_snapshot_filters:
  records:
    sql: "tenant_id IN ('tenant-a', 'tenant-b')"

bucket_definitions:
  my_records:
    data:
      - SELECT * FROM records WHERE user_id = token_parameters.user_id
```

### Partitioned Tables

```yaml
# Apply same filter to all partitions
initial_snapshot_filters:
  events_%:
    sql: 'created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)'

bucket_definitions:
  recent_events:
    data:
      - SELECT * FROM events_2024
      - SELECT * FROM events_2025
```

## Special Characters and Identifiers

⚠️ **Important**: Filter expressions are embedded directly into SQL/MongoDB queries. You must properly quote identifiers and escape string literals according to your database's syntax rules.

### SQL Identifier Quoting

**PostgreSQL** - Use double quotes for identifiers with spaces or special characters:

```yaml
initial_snapshot_filters:
  users:
    sql: '"User Status" = ''active'' AND "created-at" > NOW() - INTERVAL ''30 days'''
```

**MySQL** - Use backticks for identifiers with spaces or special characters:

```yaml
initial_snapshot_filters:
  users:
    sql: "`User Status` = 'active' AND `created-at` > NOW() - INTERVAL 30 DAY"
```

**SQL Server** - Use square brackets for identifiers with spaces or special characters:

```yaml
initial_snapshot_filters:
  users:
    sql: "[User Status] = 'active' AND [created-at] > DATEADD(day, -30, GETDATE())"
```

### String Literal Escaping

Always use proper escaping for string literals containing quotes:

```yaml
initial_snapshot_filters:
  comments:
    # Single quotes must be escaped as '' in SQL
    sql: "content NOT LIKE '%can''t%' AND status = 'approved'"
```

### Complex Expressions with OR Operators

PowerSync automatically wraps your filter in parentheses to prevent operator precedence issues:

```yaml
initial_snapshot_filters:
  users:
    # This is wrapped as: WHERE (status = 'active' OR status = 'pending')
    sql: "status = 'active' OR status = 'pending'"
```

### MongoDB Filters

MongoDB filters use native BSON query syntax, which is safer than string concatenation:

```yaml
initial_snapshot_filters:
  users:
    mongo:
      $or:
        - status: 'active'
        - status: 'pending'
      'special field': { $exists: true }
```

### Security Considerations

- Filters are defined in `sync_rules.yaml` by administrators, not by end users
- Filters are static configuration, not dynamic user input
- Still follow security best practices:
  - Avoid including sensitive data in filters
  - Test filters in development before production
  - Review filter changes carefully during deployment
- PowerSync does not parameterize filters since they are arbitrary SQL expressions, similar to bucket query definitions

## Limitations

- Filters are applied **globally** across all buckets using that table
- CDC changes only affect rows that were **initially snapshotted**
- Widening filters requires a **resnapshot or backfill** to pick up previously excluded rows
- Filter syntax must be valid for your **source database**
- Some buckets may be **empty** if their queries don't align with the global filter

## Best Practices

1. **Design filters carefully** - ensure all buckets can work with the same filter
2. **Test filters** on your source database before deploying
3. **Use indexes** on filtered columns for better snapshot performance
4. **Be conservative** - if unsure whether a row is needed, include it
5. **Document filters** in your sync_rules.yaml comments
6. **Monitor snapshot progress** to ensure reasonable replication times
7. **Verify bucket data** after applying filters to ensure no buckets are unexpectedly empty

## Migration from Per-Bucket Filters

If you're using the old per-bucket `source_tables` configuration, move filters to the top level:

```yaml
# Old (no longer supported)
bucket_definitions:
  active_users:
    data:
      - SELECT id, name FROM users
    source_tables:
      users:
        initial_snapshot_filter:
          sql: "status = 'active'"

# New (global configuration)
initial_snapshot_filters:
  users:
    sql: "status = 'active'"

bucket_definitions:
  active_users:
    data:
      - SELECT id, name FROM users
```

**Important**: With the old configuration, if multiple buckets specified different filters for the same table, they were ORed together. With the new configuration, there is **one global filter per table** that applies to all buckets.
