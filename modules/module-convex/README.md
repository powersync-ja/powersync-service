# PowerSync Convex Module

Convex replication module for PowerSync.

## Configuration

```yaml
replication:
  connections:
    - type: convex
      id: default
      tag: default
      deployment_url: https://<your-deployment>.convex.cloud
      deploy_key: <your-deploy-key>
      polling_interval_ms: 1000
      request_timeout_ms: 30000
```

## Manual smoke test

1. Start PowerSync with a Convex source config and valid sync rules that reference `convex.<table_name>`.
2. Confirm diagnostics reports the Convex source as connected.
3. Verify initial snapshot data appears in the expected bucket(s).
4. Insert/update/delete rows in Convex and verify bucket changes are replicated.

## Snapshot behavior

- Initial replication pins a global Convex snapshot boundary using `list_snapshot` without `tableName`.
- Table hydration then runs per selected sync-rule table using that fixed snapshot boundary.
- Each table snapshot starts from the first page (cursor omitted on the first call), and only uses cursor pagination within the current run.
- If initial replication is interrupted, restart resumes from the stored snapshot boundary but restarts table paging from page 1.
