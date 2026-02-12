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

## Write checkpoint behavior

- Route write checkpoints create a source marker row using Convex streaming import in `_powersync_checkpoints` (fallback: `powersync_checkpoints`).
- Convex may materialize system/invalid source table names with a `source_` prefix (for example `source_powersync_checkpoints`).
- Delta polling is global, so marker rows are observed even when not referenced in sync rules.
- Checkpoint marker tables are always ignored during snapshot and delta replication.
- Marker-only pages trigger immediate keepalive checkpoint advancement (not minute-throttled), reducing write-checkpoint acknowledgement latency.
