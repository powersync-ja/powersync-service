# PowerSync Convex Module

Convex replication module for PowerSync.

## Configuration

```yaml
replication:
  connections:
    - type: convex
      id: default
      deployment_url: https://<your-deployment>.convex.cloud
      deploy_key: <your-deploy-key>
      polling_interval_ms: 1000
      request_timeout_ms: 30000
```

## Manual smoke test

1. Simplest is to run the convex demo in the self-host-demo [repo](https://github.com/powersync-ja/self-host-demo)

# Technical notes

The content below is written in an agents.md style describing the behavior of `module-convex`.

## 1) Scope

- This module replicates Convex data into PowerSync bucket storage.
- Source APIs used are Convex [Streaming Export](https://docs.convex.dev/streaming-export-api): (`json_schemas`, `list_snapshot`, `document_deltas`).
- Initial scope is default Convex component only, but we could consider support for custom components in the future if we can figure out consistency.

## 2) Canonical Behavior

- Initial replication:
  - Initial replication pins a global Convex snapshot boundary using `list_snapshot`. If this is omitted, it provides the global snapshot boundary [ref](https://docs.convex.dev/streaming-export-api#get-apilist_snapshot).
  - Snapshot each selected Sync Streams table with that fixed `snapshot`.
  - First per-table snapshot call omits `cursor`; pagination cursor is only for later pages in the same run.
  - Commit snapshot LSN, then switch to deltas.
- Streaming replication:
  - Start from persisted resume LSN.
  - Poll `document_deltas` using frequency configured in `polling_interval_ms`
  - Always stream globally (no `tableName` filter), then filter locally by selected Sync Streams tables.
  - If a table is first seen in a `document_deltas` page and matches Sync Streams, snapshot it inline at that page boundary and skip that table's delta rows from the same page, because the snapshot already includes them.

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
- The `list_snapshot` pagination cursor is a separate JSON-serialized `{tablet, id}` string — it is pagination state, not a replication cursor.
- Persisted Convex LSNs must be canonical 19-digit numeric cursor strings. `ZERO_LSN = "0"` remains the internal sentinel.

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

## 6) Schema Change Caveat

- Convex `json_schemas` does not provide a schema change token or revision cursor that can be checkpointed.
- Current behavior uses `json_schemas` for discovery/debug, but does not continuously diff source schema versions.
- Operational caveat: if Convex schema changes (tables or columns), developers must review and redeploy Sync Streams manually.
- Future improvement: cache a canonicalized `json_schemas` hash, poll periodically, and raise diagnostics when schema drift is detected.

## 7) Datatype Mapping

- Current runtime mapping in stream writer:

| Convex Type | TS/JS Type  | SQLite type                        |
| ----------- | ----------- | ---------------------------------- |
| Id          | string      | text                               |
| Null        | null        | null                               |
| Int64       | bigint      | integer                            |
| Float64     | number      | real                               |
| Boolean     | boolean     | Up to developer - string or number |
| String      | string      | text                               |
| Bytes       | ArrayBuffer | text                               |
| Array       | Array       | text                               |
| Object      | Object      | text                               |
| Record      | Record      | text                               |

- Convex does not expose a native `Date` wire type; timestamps arrive as `number` or `string`.
- BLOB values are valid row values but are not valid bucket parameter values.

## 8) Checkpointing and Consistency

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

## 9) Convex-specific notes

- The default schema is `convex`

- **Mutation Transaction Atomicity in** `document_deltas`
  - The `cursor` in `/api/document_deltas` is a Convex commit **timestamp** (`i64`), not a per-operation counter.
  - Every Convex mutation is an ACID transaction that commits with a single timestamp; all writes within that mutation share the same `_ts` value in the delta stream.
  - Therefore, the cursor advances **once per mutation**, not once per individual CRUD operation inside it.
  - Example: a mutation that deletes 5 documents and updates 3 produces 8 entries in `document_deltas`, all with identical `_ts`.
  - The Convex backend enforces this by never splitting a page mid-timestamp: when the row limit is reached mid-transaction, the page extends until all rows at that `_ts` are included before stopping.
  - Consequence for replication: all writes from a single mutation always appear in the same `document_deltas` page and are committed to bucket storage atomically as one batch.
