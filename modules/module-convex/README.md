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

## Convex Source Setup

PowerSync requires a `powersync_checkpoints` table and a mutation function deployed to your Convex project. This is analogous to setting up a replication publication on Postgres — it enables PowerSync to write checkpoint markers that ensure causal consistency.

### 1. Add the table to your schema

In your Convex project's `convex/schema.ts`, add the `powersync_checkpoints` table:

```typescript
import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  // ... your existing tables ...

  powersync_checkpoints: defineTable({
    last_updated: v.float64(),
  }),
});
```

### 2. Add the checkpoint mutation

Create `convex/powersync_checkpoints.ts`:

```typescript
import { mutation } from "./_generated/server";

export const createCheckpoint = mutation({
  args: {},
  handler: async (ctx) => {
    const existing = await ctx.db.query("powersync_checkpoints").first();

    if (existing) {
      await ctx.db.patch(existing._id, { last_updated: Date.now() });
    } else {
      await ctx.db.insert("powersync_checkpoints", { last_updated: Date.now() });
    }
  },
});
```

### 3. Deploy

```bash
npx convex dev --once
```

### How it works

PowerSync calls this mutation via the [Convex HTTP API](https://docs.convex.dev/http-api):

```
POST <deployment_url>/api/mutation
Authorization: Convex <deploy_key>
Content-Type: application/json

{
  "path": "powersync_checkpoints:createCheckpoint",
  "args": {},
  "format": "json"
}
```

The mutation upserts a single row (insert on first call, patch thereafter), so the table stays bounded to one row. This write generates a delta event that PowerSync observes to confirm write-checkpoint consistency.

> **Note:** The `powersync_checkpoints` table is automatically excluded from sync rules — it is never replicated to client devices.

## Write checkpoint behavior

- Route write checkpoints create a source marker row in `powersync_checkpoints` via the deployed mutation.
- Delta polling is global, so marker rows are observed even when not referenced in sync rules.
- Checkpoint marker tables are always ignored during snapshot and delta replication.
- Marker-only pages trigger immediate keepalive checkpoint advancement (not minute-throttled), reducing write-checkpoint acknowledgement latency.
