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
