# PowerSync Convex Module

Convex replication module for PowerSync.

> [!WARNING]
> The Convex replicator is currently released as an [experimental feature](https://docs.powersync.com/resources/feature-status). APIs and
> behavior may change, and the developer experience isn't yet on par with our other backend database connectors.
> We also can't yet guarantee continued support or long-term stability.
> This release is intended for early testing and to invite feedback. Your feedback will directly influence whether, and how,
> this integration evolves.

## Configuration

```yaml
replication:
  connections:
    - type: convex
      deployment_url: https://<your-deployment>.convex.cloud
      deploy_key: <your-deploy-key>
      polling_interval_ms: 1000
      request_timeout_ms: 30000
```

## Manual smoke test

1. Simplest is to run the react-convex-todolist demo in the powersync-js[repo](https://github.com/powersync-ja/powersync-js)

## Development

Run the `dev:convex` script to start the local Convex development backend used by the module tests.

```bash
# In the modules/module-convex folder
pnpm run dev:convex

# OR
# From the repo root
pnpm run -C modules/module-convex dev:convex
```

The local backend listens on `http://127.0.0.1:3210` by default. The integration tests read the local deploy key from
`modules/module-convex/.convex/local/default/config.json`, which is created by `dev:convex`.

To run the Convex module tests locally:

```bash
# Terminal 1
pnpm run -C modules/module-convex dev:convex

# Terminal 2, from the repo root
pnpm --filter='./modules/module-convex' test
```

Some integration tests are gated behind `CI=true` or `SLOW_TESTS=true`. To run them locally, keep `dev:convex` running
and start the required storage backends (MongoDB and Postgres storage), then run:

```bash
CI=true pnpm --filter='./modules/module-convex' test
```

## Technical notes

The content below is written in an agents.md style describing the behavior of `module-convex`.

## 1) Scope

- This module replicates Convex data into PowerSync bucket storage.
- Source APIs used are Convex [Streaming Export](https://docs.convex.dev/streaming-export-api): (`json_schemas`, `list_snapshot`, `document_deltas`).
- Initial scope is default Convex component only, but we could consider support for custom components in the future if we can figure out consistency.
- Deploy keys grant root access (read/write on all tables), components could address this later.

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
  - If a table is first seen in a `document_deltas` page and matches Sync Streams, resolve it and apply the delta row directly. Do not snapshot it inline; initial wildcard expansion already discovers schema-defined tables through `json_schemas`, and the delta payload is the source of truth for later writes.

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
- The `list_snapshot` pagination cursor is a separate JSON-serialized `{table, id}` string — it is pagination state, not a replication cursor.
- Persisted Convex LSNs must be canonical 19-digit numeric cursor strings. `ZERO_LSN = "0"` remains the internal sentinel.

## 5) API Client Contract

- Auth header: `Authorization: Convex <deploy_key>`.
- Always request `format=json`.
- Parse large numeric JSON using `JSONBig`.
- Convex API response shape validation is disabled by default. Set `POWERSYNC_DEV_CHECK_CONVEX_RESPONSES` before service startup to validate responses with the shared ts-codec JSON schema validator while debugging API compatibility.
- Retry classification:
  - retryable: network, timeout, 429, 5xx.
  - non-retryable: malformed responses, auth/config issues.

## 6) Schema Changes

- Conventional schema change handling is mainly used to detect changed replica identity columns, update cached table metadata, drop/rename tables, and trigger a table re-snapshot when DDL changed storage semantics.
- Convex tables always use `_id` as the replication identity, so there is no equivalent replica identity drift to detect.
- Stream row conversion uses the JSON document returned by `list_snapshot` and `document_deltas`, not Convex `json_schemas` metadata. Added fields, removed fields, and type changes are therefore replicated through normal document mutations.
- Convex data migrations are expected to run as writes/mutations over live documents. Those updates should appear in `document_deltas` and be replicated without a schema-driven re-snapshot.
- Exact table patterns are resolved directly from Sync Streams rules. `json_schemas` is only used for initial wildcard table expansion and API/debug schema previews.
- A re-snapshot is still required for initial replication, a sync-rule deployment that selects new existing data, or a lost/expired cursor, but not merely because a Convex field was added, removed, or changed type.
- Table drops are not (yet) detected by continuously diffing `json_schemas`. Validation showed that deleting a table from the Convex dashboard does not emit per-document `_deleted` rows in `document_deltas`, so previously replicated rows can remain synced to clients. Use the dashboard "Clear Table" action before deleting a table, or delete documents through mutation paths that emit document deltas. Otherwise, handle dashboard/schema-only table removal as a sync-rule/deployment state change and clear/re-replicate affected PowerSync state.
- See [Convex schema change handling](../../docs/modules/convex/schema-change-handling.md) for the detailed rationale and limitations.

## 7) Datatype Mapping

- Current runtime mapping in stream writer:

| Convex Type | JSON wire / Sync Streams value | SQLite type |
| ----------- | ------------------------------ | ----------- |
| Id          | string                         | text        |
| Null        | null                           | null        |
| Int64       | base10 string                  | text        |
| Float64     | number                         | real        |
| Boolean     | boolean                        | integer     |
| String      | string                         | text        |
| Bytes       | base64 string                  | text        |
| Array       | Array                          | text        |
| Object      | Object                         | text        |
| Record      | Record                         | text        |

- Convex does not expose a native `Date` wire type; timestamps arrive as `number` or `string`.
- Value conversion flow:
  1. PowerSync requests snapshots or document deltas from Convex's Streaming Export APIs.
  2. The JSON response is parsed into raw Convex documents, where row columns are represented as JSON object fields.
  3. PowerSync converts the raw JSON-compatible values to SQLite-compatible values without using `json_schemas` metadata for datatype coercion.
- Convex's `json_schemas` endpoint appears to infer table schemas from table summaries instead of using the TypeScript schema as a complete source of truth. This means fields can be absent from `json_schemas` until populated data exists. To avoid changing replicated value types based on whether schema metadata was available, Sync Streams see the stable JSON wire representation. For `Int64` columns, users should explicitly cast those values in Sync Streams rules using `CAST(value AS INTEGER)`.

## 8) Checkpointing and Consistency

- `createReplicationHead` must:
  1. resolve global head cursor,
  2. pass that head to the callback so PowerSync stores the managed write checkpoint mapping,
  3. then write a Convex checkpoint marker via `POST /api/mutation` (calls `powersync_checkpoints:createCheckpoint`).
- The marker must be written after the callback. If the marker is replicated before the managed write checkpoint mapping exists, an idle source can still leave the client waiting for a later observable checkpoint update.
- Source marker table: `powersync_checkpoints`
  - Convex rejects table names starting with `_`, so no leading-underscore variant is used.
  - The table has a single `last_updated` field; the mutation upserts one row (bounded to one row total).
  - The developer must deploy the `powersync_checkpoints` schema and mutation to their Convex project.
- Stream handling requirement:
  - checkpoint marker tables must always be excluded from replicated source tables and ignored in delta row application.
  - marker-only delta pages must trigger immediate `keepalive` checkpoint advancement (do not wait for 60s throttle).

## 9) Other Convex-specific notes

- The default schema is `convex`
- On an idle system, multiple successive calls to `/api/document_deltas` will return the same cursor value i.e. the cursor is not wall clock based.

- **Mutation Transaction Atomicity in** `document_deltas`

  - The `cursor` in `/api/document_deltas` is a Convex commit **timestamp** (`i64`), not a per-operation counter.
  - Every Convex mutation is an ACID transaction that commits with a single timestamp; all writes within that mutation share the same `_ts` value in the delta stream.
  - Therefore, the cursor advances **once per mutation**, not once per individual CRUD operation inside it.
  - Example: a mutation that deletes 5 documents and updates 3 produces 8 entries in `document_deltas`, all with identical `_ts`.
  - The Convex backend enforces this by never splitting a page mid-timestamp: when the row limit is reached mid-transaction, the page extends until all rows at that `_ts` are included before stopping.
  - Consequence for replication: all writes from a single mutation always appear in the same `document_deltas` page and are committed to bucket storage atomically as one batch.
  - `TRANSACTIONS_REPLICATED` is counted from distinct `_ts` values among replicated changes, not from `document_deltas` pages. A single page can contain multiple Convex mutations, so a committed page may increase the transaction metric by more than one.
  - The stream relies on Convex returning `document_deltas` in mutation order by `_ts`. Row order within the same `_ts` is not significant: those rows belong to the same Convex mutation and are committed atomically.
  - The stream asserts that observed `_ts` values are non-decreasing across pages. Equal `_ts` values are allowed because they represent rows from the same Convex mutation.

- **Replication metrics**
  - Implemented:
    - `ROWS_REPLICATED`: incremented for each row written from snapshots and deltas.
    - `TRANSACTIONS_REPLICATED`: incremented by the number of distinct Convex `_ts` mutation timestamps replicated from `document_deltas`.
  - Not implemented yet:
    - `DATA_REPLICATED_BYTES`: Convex does not currently report source bytes replicated into PowerSync. This would need explicit accounting in the Convex replication/client path.
    - `CHUNKS_REPLICATED`: Convex does not currently report replication chunks.
  - Bucket storage size gauges (`REPLICATION_SIZE_BYTES`, `OPERATION_SIZE_BYTES`, `PARAMETER_SIZE_BYTES`) are reported by the configured bucket storage backend, not by this replication module.
