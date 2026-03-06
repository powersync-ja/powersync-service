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
```

## Manual smoke test

1. Start PowerSync with a Convex source config and valid sync rules that reference `convex.<table_name>`.
2. Confirm diagnostics reports the Convex source as connected.
3. Verify initial snapshot data appears in the expected bucket(s).
4. Insert/update/delete rows in Convex and verify bucket changes are replicated.

## Snapshot behavior

- Initial replication pins a global Convex snapshot boundary using `list_snapshot` without `tableName`.
- Table hydration then runs per selected sync-rule table using that fixed snapshot boundary.
- Each table snapshot starts from the first page when there is no persisted progress, and otherwise resumes from the stored page cursor.
- If initial replication is interrupted, restart resumes from the stored snapshot boundary and any persisted per-table snapshot progress.

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


# Technical notes

The content below is written in an agents.md style describing the behavior of `module-convex`.

## 1) Scope
- This module replicates Convex data into PowerSync bucket storage.
- Source API is Convex Streaming Export (`json_schemas`, `list_snapshot`, `document_deltas`).
- Initial scope is default Convex component only.

## 2) Canonical Behavior
- Initial replication:
  1. Pin one **global snapshot** via `list_snapshot` **without** `tableName` and `cursor`.
  2. Snapshot each selected sync-rules table with that fixed `snapshot`.
  3. First per-table snapshot call omits `cursor`; pagination cursor is only for later pages in the same run.
  4. Commit snapshot LSN, then switch to deltas.
- Streaming replication:
  - Start from persisted resume LSN.
  - Poll `document_deltas`.
  - Always stream globally (no `tableName` filter), then filter locally by selected sync-rules tables.
  - If a table is first seen in a `document_deltas` page and matches sync rules, snapshot it inline at that page boundary and skip that table's delta rows from the same page, because the snapshot already includes them.

## 3) Hard Invariants (Do Not Break)
- `snapshot` is the consistency boundary; page `cursor` is pagination state.
- All table snapshots in a run must use the same pinned `snapshot`; if response snapshot differs, fail fast.
- On restart during initial replication:
  - Reuse persisted snapshot LSN boundary.
  - Resume table page walk from the persisted per-table `lastKey` cursor when available.
  - If the last page was already flushed before interruption, mark the table snapshot done without re-reading rows.
- Delta streaming starts from resume LSN (snapshot boundary), not from table page cursor.
- `tablePattern.connectionTag` and schema must match before table selection.
- Source table replica identity is `_id`.
- The overall system must ensure causal consistency of replicated data in bucket storage.

## 4) LSN and Cursor Rules
- Convex snapshot and delta cursors are always `i64` timestamps (serialized as decimal numeric strings in JSON).
- Convex LSN helpers reject non-numeric cursors.
- The `list_snapshot` pagination cursor is a separate JSON-serialized `{tablet, id}` string — it is pagination state, not a replication cursor.
- Persist LSNs as the raw decimal cursor string returned by Convex.
- Persisted Convex LSNs must be canonical numeric cursor strings.

## 5) API Client Contract
- Auth header: `Authorization: Convex <deploy_key>`.
- Always request `format=json`.
- Fallback path support: `/api/streaming_export/...` when `/api/...` returns `404`.
- Parse large numeric JSON using `JSONBig`.
- `json_schemas` must support:
  - array/object under `tables`,
  - self-host top-level table map shape.
- Retry classification:
  - retryable: network, timeout, 429, 5xx.
  - non-retryable: malformed responses, auth/config issues.

## 6) Sync Rules and Connection Semantics
- Default schema is `convex`.
- SQL literal reminder for sync rules: string literals must use single quotes (`'sally'`), not double quotes.

## 7) Schema Change Caveat
- Convex `json_schemas` does not provide a schema change token or revision cursor that can be checkpointed.
- Current behavior uses `json_schemas` for discovery/debug, but does not continuously diff source schema versions.
- Operational caveat: if Convex schema changes (tables or columns), developers must review and redeploy sync rules manually.
- Future improvement: cache a canonicalized `json_schemas` hash, poll periodically, and raise diagnostics when schema drift is detected.

## 8) Datatype Mapping
- Current runtime mapping in stream writer:
  - `number` integer -> `BigInt` (SQLite INTEGER),
  - `number` float -> REAL,
  - `boolean` -> `1n`/`0n`,
  - bytes over the JSON API -> base64 `string`,
  - `object/array` -> JSON TEXT.
  NOTE:
  - Convex does not expose a native `Date` wire type here; timestamps arrive as `number` or `string`.

## 9) Checkpointing and Consistency
- `createReplicationHead` must:
  1. resolve global head cursor,
  2. write a Convex checkpoint marker via `POST /api/mutation` (calls `powersync_checkpoints:createCheckpoint`),
  3. then pass the head to callback.
- Source marker table: `powersync_checkpoints`
  - Convex rejects table names starting with `_`, so no leading-underscore variant is used.
  - The table has a single `last_updated` field; the mutation upserts one row (bounded to one row total).
  - The developer must deploy the `powersync_checkpoints` schema and mutation to their Convex project (see README).
- Stream handling requirement:
  - checkpoint marker tables must always be excluded from replicated source tables and ignored in delta row application.
  - marker-only delta pages must trigger immediate `keepalive` checkpoint advancement (do not wait for 60s throttle).

## 10) Logging Policy
- Keep logs high-signal and bounded.
- Required snapshot logs:
  - pinned or reused global snapshot,
  - table snapshot start,
  - snapshot completion and resume LSN.
- Avoid noisy per-row debug logs unless behind explicit debug gating.

## 11) Known Non-Goals (For Now)
- Convex component targeting beyond default component.
- A true push stream transport (module is polling deltas).
- Read-only Convex deploy key model (Convex deploy key is full access; use network isolation and secret controls).
- Cross-source transactional guarantees beyond Convex stream ordering.

## 12) Conformance Suite (Must Stay Green)
- Unit tests:
  - `test/src/ConvexApiClient.test.ts`
  - `test/src/ConvexCheckpoints.test.ts`
  - `test/src/ConvexLSN.test.ts`
  - `test/src/ConvexRouteAPIAdapter.test.ts`
  - `test/src/ConvexStream.test.ts`
  - `test/src/types.test.ts`
- Required assertions include:
  - global snapshot pin call is unfiltered,
  - first per-table snapshot call omits cursor,
  - pagination cursor used only for subsequent pages,
  - snapshot mismatch fails fast,
  - route adapter head resolves global snapshot cursor and writes a source marker,
  - marker-only pages advance LSN via immediate keepalive.
- Manual smoke test (self-host acceptable):
  - initial snapshot visible in buckets,
  - inserts/updates/deletes propagate via deltas,
  - no table-not-found due to schema/tag mismatch.

## 13) Change Checklist for Agents
- If changing snapshot/delta flow:
  - update `ConvexStream` tests first.
- If changing cursor/LSN encoding:
  - update the Convex LSN helper tests and any format-compat coverage.
- If changing API response parsing:
  - include self-host payload fixtures in `ConvexApiClient` tests.
- If changing public behavior:
  - update `README.md` and this `AGENTS.md` in the same PR.
