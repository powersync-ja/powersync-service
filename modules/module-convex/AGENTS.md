# Convex Module Agent Guide

This file is the working contract for agents modifying `module-convex`.

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

## 3) Hard Invariants (Do Not Break)
- `snapshot` is the consistency boundary; page `cursor` is pagination state.
- All table snapshots in a run must use the same pinned `snapshot`; if response snapshot differs, fail fast.
- On restart during initial replication:
  - Reuse persisted snapshot LSN boundary.
  - Restart table page walk from first page (do not resume per-table `lastKey`).
- Delta streaming starts from resume LSN (snapshot boundary), not from table page cursor.
- `tablePattern.connectionTag` and schema must match before table selection.
- Source table replica identity is `_id`.
- The overall system must ensure causal consistency of replicated data in bucket storage.

## 4) LSN and Cursor Rules
- Never assume Convex cursors are numeric-only.
- Supported cursor shapes include:
  - decimal numeric timestamp strings,
  - opaque strings.
- Persist LSNs using `ConvexLSN` comparable format (`mode + sortable key + encoded cursor`).
- Keep raw cursor round-trip safe.

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
  - `Uint8Array` -> BLOB,
  - `object/array` -> JSON TEXT.
  NOTE:
  - Preserve bytes as BLOB in sync rules; use explicit base64 conversion in rules only when needed by consumers.

## 9) Checkpointing and Consistency
- `createReplicationHead` must:
  1. resolve global head cursor,
  2. write a Convex source marker via streaming import,
  3. then pass the head to callback.
- Source marker table: `powersync_checkpoints`
  - Convex rejects table names starting with `_`, so no leading-underscore variant is used.
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
  - update `ConvexLSN` tests and backward-compat coverage.
- If changing API response parsing:
  - include self-host payload fixtures in `ConvexApiClient` tests.
- If changing public behavior:
  - update `README.md` and this `AGENTS.md` in the same PR.
