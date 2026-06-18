# AI Agent Plans For New Replication Modules

This page gives example implementation plans for an AI coding agent adding a new PowerSync replication module.

The plans are intentionally phased. A complete source module touches config validation, source metadata extraction, replication lifecycle, snapshots, streaming, checkpoints, and tests. For most sources, this should be delivered as a series of small changes rather than one large patch.

Integration tests should move with the implementation phases. Do not defer the first real source test until the final hardening pass. Each phase should add or extend the smallest integration test that proves the new contract boundary works against a real or high-fidelity source fixture. The final hardening phase is for broad failure coverage, not for discovering whether the basic phase behavior works.

Most replication modules use a stream test context to make those integration tests manageable. Existing examples include `WalStreamTestContext`, `ChangeStreamTestContext`, `BinlogStreamTestContext`, `CDCStreamTestContext`, and `ConvexStreamTestContext`. A new module should add the equivalent helper early, before later snapshot, streaming, and write-checkpoint tests depend on it.

Replication integration tests should prefer real bucket storage implementations for real-world verification. Existing modules import test storage factories from storage modules, such as `@powersync/service-module-mongodb-storage` and `@powersync/service-module-postgres-storage`, and run the same source tests through `describeWithStorage` where practical. For when to use spies versus mocks, follow the General Workflow testing guidance in [`AGENTS.md`](../../../AGENTS.md).

Use these plans together with:

- [Core concepts](./01-core-concepts.md)
- [Source connector contract](./03-source-connector-contract.md)
- [Replication lifecycle](./02-replication-lifecycle.md)
- [Snapshots and streaming](./05-snapshots-and-streaming.md)
- [Storage writer contract](./04-storage-writer-contract.md)
- [Checkpoints](./06-checkpoints.md)
- [Source modules](./07-source-modules.md)

## Before Starting

For a new source named `ExampleDB`, first identify the closest existing module:

- Postgres-like: ordered log, source-managed replication slot, transaction boundaries.
- MongoDB-like: change stream, resume token, full-document or partial-document updates.
- MySQL-like: binlog/GTID stream, schema metadata from relational tables.
- SQL Server-like: CDC polling over bounded history.
- Convex-like: API cursor, snapshot API, document delta stream.

Then write down source-specific decisions before editing code:

- Data source config shape, optional input forms, and normalized runtime config.
- How to test a connection.
- How to extract schema for sync config validation and client schema generation.
- Source entity identity, including stable object ids, schema/table names, and replica identity columns.
- Replication stream capability. Confirm that the source has an ordered stream, CDC feed, operation log, export cursor, polling boundary, or equivalent positional mechanism.
- Replication position encoding. The stored `lsn` must be a string and must preserve ordering wherever the module compares source heads or write checkpoint positions.
- Snapshot boundary. Decide how the module records a source position before or at snapshot start.
- Resume state ownership. Define where the restart cursor lives. If the source owns it, describe the source-side state, such as a replication slot or server cursor, and how reconnect resumes from it. If PowerSync owns it, define the token stored in `resumeFromLsn`, when the stream calls `setResumeLsn(lsn)`, and how that differs from committing a visible checkpoint.
- Schema change policy. Decide whether streaming detects metadata changes automatically or whether users must deploy a new sync config after source schema changes.
- History loss behavior. Decide what source error means the module must call `restartReplication(replicationStreamId)`.
- Managed write checkpoint behavior. Decide how `createReplicationHead()` reads a comparable source head and forces a later observable source event when the source is idle.
- Checkpoint marker strategy. Decide whether the source needs a `_powersync_checkpoints` or equivalent marker table/collection, logical message, heartbeat, or no marker at all.
- Integration test context. Decide what helper will own source setup, storage setup, sync config deployment, stream startup, checkpoint waits, aborts, and cleanup.
- Test storage coverage. Decide which real storage factories the integration tests will import and run against, and which environment flags will enable or skip each backend.
- Cleanup behavior for source-side slots, marker tables, cursors, or temporary state.

## Plan 1: Feasibility And Design Notes

Use this plan when the source has not been integrated before.

Goal: produce a short design note that maps the source database or source API onto the PowerSync replication contract.

Steps:

1. Inspect [source-modules.md](./07-source-modules.md) and choose the closest existing source module as the implementation reference.
2. Read that module's `ReplicationModule`, `RouteAPI` adapter, `AbstractReplicator`, `AbstractReplicationJob`, and stream helper.
3. Confirm that the source can provide an ordered change feed, CDC table, operation log, export cursor, or equivalent stream with positional information.
4. Define resume state ownership: either the source stores the stream cursor, or PowerSync stores a resume token in bucket storage.
5. If resume is source-managed, define the source-side state, how reconnect resumes from it, and what error means that state is missing or unusable.
6. If resume is PowerSync-managed, define the exact token stored in `resumeFromLsn`, when the stream calls `setResumeLsn(lsn)`, and why that token is safe to resume from after a crash.
7. Confirm that the stream contains all changes relevant to PowerSync inside the chosen boundary. If the stream is filtered, identify source-head changes that might not be visible in the stream.
8. Confirm that the source can either snapshot existing data or replay enough history to rebuild current state.
9. Define the source position string format and compare semantics. This does not have to be a native LSN, but it must satisfy the module's resume, snapshot, and write-checkpoint requirements.
10. Define how the source detects missing or expired history.
11. Define how managed write checkpoints will work on an idle database.
12. Decide whether a checkpoint marker is required, such as a `_powersync_checkpoints` collection, support table, logical replication message, heartbeat, or source API barrier.
13. Define whether schema changes are detected during streaming or require a new sync config deploy.
14. Define the integration test fixture and test context strategy for this source, including how tests will start the source, create schema/data, reset state, configure storage, deploy sync rules, run the stream, wait for checkpoints, and skip when the fixture is unavailable.
15. Define which real bucket storage factory implementations the integration tests will import, such as MongoDB storage and Postgres storage, and whether any source-specific storage backend is required.
16. List source limitations, such as missing transaction boundaries, partial update images, bounded retention, unsupported schema changes, or unavailable replica identity metadata.

Acceptance checks:

- The design names the closest existing module and why.
- The design proves the source has a positional replication stream that can resume from a known point or from documented source-managed stream state.
- The design explains how the connector knows that all relevant changes inside a boundary are present in the stream.
- The design states whether `commit(lsn)` can happen at true source transaction boundaries or only at page/poll boundaries.
- The design explains the initial snapshot consistency boundary.
- The design explains resume state ownership, including when `setResumeLsn(lsn)` is used and how history loss is detected.
- The design explains managed write checkpoint behavior.
- The design explains when a source-side checkpoint marker is required and what stream event it produces.
- The design documents the schema change policy and operator action required after source schema changes.
- The design describes the test context API that later integration tests will use.
- The design states which integration tests use real storage factory implementations, and where any spies or mocks are justified.
- The design lists the integration tests expected in each later phase, including any fixture setup or environment requirements.

## Plan 2: Module Skeleton And Test Context

Use this plan to add the first compiling source module without implementing full replication.

Goal: register a new replication module with config decoding, connection tests, route API creation, empty replication lifecycle classes, and the test context needed by later integration tests.

Steps:

1. Create a new module package under `modules/module-exampledb` using the closest existing source module as a template.
2. Define a unique source `type` string and a config codec that extends the generic data source config.
3. Add a resolved config type and `resolveConfig()` helper for optional input forms such as URI, host, port, database, schema, credentials, TLS, polling intervals, and timeouts.
4. Implement the `ReplicationModule<TConfig>` subclass with `createRouteAPIAdapter()`, `createReplicator(context)`, `testConnection(config)`, and any source-specific `onInitialized(context)` setup.
5. Implement a `RouteAPI` adapter skeleton with connection status, connection close, source query stubs if supported, and schema endpoint placeholders.
6. Implement an `AbstractReplicator` subclass that can create jobs, clean up stopped streams idempotently, and run `testConnection()`.
7. Implement an `AbstractReplicationJob` subclass whose `replicate()` creates an `ExampleDBStream` helper and whose `keepAlive()` and `getReplicationLagMillis()` are explicit about what is supported.
8. Add module registration and package metadata following the existing module patterns.
9. Add config codec and normalization unit tests.
10. Add an `ExampleDBStreamTestContext` or equivalent helper based on the closest existing module's test context.
11. Make the test context responsible for opening the bucket storage factory, creating the source connection manager/client, clearing source state unless `doNotClear` is requested, configuring sync rules, exposing the stream helper, starting and aborting replication, waiting for initial snapshot or checkpoint progress when supported, and disposing all resources.
12. Import real test storage factories from the storage modules the source should support, for example `mongo_storage.test_utils.mongoTestStorageFactoryGenerator()` from `@powersync/service-module-mongodb-storage` and `postgres_storage.test_utils.postgresTestSetup()` from `@powersync/service-module-postgres-storage`.
13. Add storage-version and storage-backend coverage helpers when the module should run against multiple bucket storage implementations, following existing `describeWithStorage` patterns and environment flags such as `TEST_MONGO_STORAGE` and `TEST_POSTGRES_STORAGE`.
14. Add an integration test that uses the source fixture and test context to verify `testConnection()` succeeds for a valid config and fails cleanly for an invalid or unreachable config.

Acceptance checks:

- The package builds.
- A config with the new `type` is decoded by the module and normalized once.
- Invalid configs fail at the codec or normalization boundary.
- The source fixture can be reached by the module's connection test in CI or in an explicitly documented optional test mode.
- The test context can open and dispose a source connection plus bucket storage without leaking background replication work.
- Later phase tests can use the test context to deploy sync rules, access the source client, start or abort the stream, and wait for client-visible storage output.
- Integration tests use imported real storage factories for storage-facing behavior. Spies may observe calls, but mocks are limited to focused unit tests or impractical real dependencies.
- The module can be registered without starting a working stream.
- Cleanup is safe to call when no source-side state exists.

## Plan 3: Schema And Sync Config API

Use this plan to make the source usable by admin, validation, and schema generation routes where the source can provide schema metadata. For schemaless sources, this phase should define the best available collection/table metadata and make sure snapshot and streaming code can discover source entities correctly.

Goal: translate source metadata into PowerSync's common schema and table formats when available, and define the source entity discovery behavior when strict schema is not available.

Steps:

1. Classify the source as strict-schema, partially schema-aware, or schemaless for the purposes of validation and client schema generation.
2. Implement source metadata queries or API calls for tables, collections, columns, primary keys, replica identity, and source object ids where available.
3. Convert source metadata into `DatabaseSchema[]` for `getConnectionSchema()` where meaningful. For schemaless sources, return the best available collection/table list and type metadata, or document which strict schema fields are unavailable.
4. Implement `getDebugTablesInfo()` so sync config table patterns resolve into concrete source tables or collections with useful warnings.
5. Implement `getParseSyncRulesOptions()` for default database, default schema, identifier behavior, schemaless behavior, or source-specific parsing options.
6. Implement `SourceEntityDescriptor` creation for source entities discovered during snapshot and streaming.
7. Decide how to handle tables or collections without stable replica identity, strict column metadata, or complete update images.
8. Decide whether streaming will refresh these descriptors after schema changes or whether source metadata refresh only happens during sync config deploy.
9. Add unit tests for source metadata conversion, schema defaults, table pattern expansion, schemaless metadata behavior, and unsupported table warnings.
10. Add an integration test using the module test context that creates representative source schema or collections and verifies `getConnectionSchema()`, `getDebugTablesInfo()`, and sync config validation expose the available metadata and warnings.

Acceptance checks:

- Sync config validation can inspect actual source tables, collections, and columns where the source exposes them.
- Client schema generation has enough source metadata to infer usable schemas, or the source documentation clearly states which schema details cannot be inferred.
- `SourceEntityDescriptor` values are stable across restarts and detect renames or identity changes where the source supports that.
- Schemaless sources correctly create source table/collection descriptors as data is snapshotted or streamed, even without strict column metadata.
- Unsupported entities fail with actionable validation messages rather than failing later in streaming.
- The integration test proves metadata extraction works against the source fixture, not only against hand-written metadata objects.
- If automatic schema change detection is not implemented, docs and validation messages make the sync config deploy requirement clear.

## Plan 4: Initial Snapshot

Use this plan once schema discovery and table descriptors are available.

Goal: copy existing source data through `BucketStorageBatch` while preserving the initial consistency boundary.

Steps:

1. Open a writer with the correct `zeroLSN`, `storeCurrentData`, `skipExistingRows`, hooks, and tracer options.
2. Record the source position that bounds the snapshot before or at snapshot start.
3. Discover selected source entities from the hydrated sync config.
4. Call `resolveTables()` for each discovered entity.
5. Apply `drop()` for outdated mappings when `resolveTables()` returns `dropTables`.
6. Copy source rows in chunks, pages, or cursor batches.
7. Normalize each row into insert-like `save()` calls for the resolved `SourceTable` records.
8. Persist progress with `updateTableProgress()` after chunks so interrupted snapshots can resume.
9. Mark each table snapshot complete with `markTableSnapshotDone(tables, no_checkpoint_before_lsn)`.
10. Mark the overall snapshot complete with `markSnapshotDone(no_checkpoint_before_lsn)` once every required table is complete.
11. `commit(lsn)` or `keepalive(lsn)` at a source-safe boundary so storage can unblock checkpoints after streaming reaches the boundary.
12. Add integration tests through the module test context for initial snapshot of at least one data bucket and one parameter lookup path.
13. Add an integration test through the module test context that interrupts or resumes snapshot work when the source module supports resumable snapshot progress.

Acceptance checks:

- A new stream starts in `PROCESSING`, snapshots selected data, and becomes eligible for `ACTIVE` only after a consistent checkpoint exists.
- Snapshot progress survives process restart.
- Re-running a chunk is safe when `skipExistingRows` is used.
- Updates, inserts, and deletes that occur during the snapshot are later replayed by streaming from the recorded source position.
- Checkpoints remain blocked until all table and stream snapshot boundaries have been satisfied.
- The snapshot integration tests assert client-visible bucket data or storage checkpoint output, not only internal helper calls.

## Plan 5: Ongoing Streaming

Use this plan after the module can snapshot.

Goal: consume source changes continuously and write bucket operations, parameter lookup rows, and checkpoints in source order.

Steps:

1. Decide whether streaming starts immediately alongside initial snapshot work or after snapshot completion from the recorded source position.
2. Resume from the source-managed stream state or from storage `resumeFromLsn`, according to the ownership model defined in Plan 1.
3. Open a long-running writer for the streaming attempt unless the source workflow is clearer with page-scoped or poll-iteration writers.
4. Parse relation, table, schema, or collection metadata events.
5. Call `resolveTables()` when a source entity is first seen or when metadata may have changed.
6. Use `truncate()` or `drop()` when source table mappings are superseded, dropped, renamed, or require a fresh snapshot.
7. Normalize inserts, updates, and deletes into `save()` calls.
8. Use `flush()` inside large source transactions when needed, but call `commit(lsn)` only outside source transaction boundaries.
9. Use `keepalive(lsn)` for heartbeats, checkpoint markers, or position advances with no user data.
10. Use `setResumeLsn(lsn)` only for PowerSync-managed resume tokens. This persists the restart cursor without making that source position visible as a checkpoint.
11. Report replication lag from source commit timestamps or the closest reliable source metadata.
12. Detect missing or expired history and call `restartReplication(replicationStreamId)` rather than skipping changes.
13. Add integration tests through the module test context for insert, update, delete, and idle keepalive or marker behavior after the initial snapshot.
14. Add an integration test through the module test context for restart from persisted resume state when PowerSync owns the source resume token.
15. Add schema-change integration tests in this phase if the module claims automatic schema change handling.

Acceptance checks:

- Row changes are applied in source order.
- `commit(lsn)` never splits a source transaction across visible PowerSync checkpoints.
- Idle source marker events can advance checkpoints without writing user data.
- Restart from the source-managed state or persisted `resumeFromLsn` does not duplicate visible effects incorrectly or skip changes.
- Missing source history creates a new replication stream and fresh snapshot.
- Streaming integration tests cover the storage or sync API outputs that clients rely on.
- If schema changes are not detected during streaming, the stream behavior and source docs clearly require a new sync config deploy after source schema changes.

## Plan 6: Managed And Custom Write Checkpoints

Use this plan once streaming can advance checkpoints.

Goal: allow clients to wait until their backend write has round-tripped through replication.

Steps:

1. Implement `RouteAPI.createReplicationHead(callback)`.
2. Read the current comparable source head after the application's write should be visible to the source.
3. Call the callback with that source head so bucket storage can persist the managed write checkpoint mapping.
4. After the callback completes, force a later observable source event when the source may otherwise be idle.
5. Ensure the source stream calls `commit(lsn)` or `keepalive(lsn)` when it observes the marker or a later source position.
6. Add a custom write checkpoint path only if the source or integration needs a backend-owned increasing checkpoint id instead of a source-position-based acknowledgement.
7. Add an integration test through the module test context for managed write checkpoints on an active source.
8. Add an integration test through the module test context for managed write checkpoints on an idle source where the marker is required.
9. Add custom write checkpoint integration tests in the same phase if custom mode is implemented.

Acceptance checks:

- On an idle source, `/write-checkpoint2.json` eventually produces a checkpoint line containing the write checkpoint id.
- The marker used to advance the stream is not exposed as replicated user data.
- The source head ordering matches the stored `lsn` ordering used by checkpoint comparisons.
- Custom write checkpoints, if implemented, are monotonic per user/client contract and are emitted only after the source stream observes them.
- Write checkpoint tests observe the acknowledgement through the same checkpoint path a client uses.

## Plan 7: Cross-Cutting Hardening

Use this plan before treating the source as production-ready.

Goal: fill the gaps left after each earlier phase has already added its own focused integration tests.

Steps:

1. Review the tests added in Plans 2 through 6 and list uncovered correctness risks.
2. Add regression tests for writes during snapshot, including primary-key or replica-identity changes if the source supports them.
3. Add regression tests for truncates, drops, renames, and schema changes according to source capabilities and documented schema-change policy.
4. Test large source transactions and confirm `flush()` does not expose partial transaction results before `commit(lsn)`.
5. Test missing or expired source history and verify replication restarts with a fresh stream.
6. Test cleanup of stopped streams and idempotent cleanup after source-side state is already gone.
7. Test lag metrics, connection status, retry/backoff behavior, and shutdown through `abortController.signal`.
8. Add docs in [source-modules.md](./07-source-modules.md) describing the source position, stream mechanism, snapshot approach, write checkpoint barrier, schema-change policy, and history loss handling.

Acceptance checks:

- Phase tests already cover the basic happy paths before this hardening phase starts.
- New regression tests fail before the relevant fix or guard and pass after it.
- The source has coverage for at least one full client-visible read path: source write, replication checkpoint, sync API checkpoint diff, bucket data, and checkpoint complete.
- Failure tests show that the module restarts or errors explicitly instead of silently skipping data.
- Storage-facing tests run against real storage modules, so checkpoint, parameter lookup, current-data, and bucket operation behavior are covered by the actual storage contract.
- The implementation follows existing module patterns unless the source requires a documented exception.
- The module either has tests for automatic schema change handling or documents that users must deploy a new sync config after source schema changes.

## Prompt Template

Use this prompt shape when asking an AI coding agent to implement a new source module:

```text
Add a new PowerSync replication module for <source>.

Follow docs/specs/replication/08-ai-agent-plans.md. Start with Plan <n> only.

Use <existing module> as the closest reference because <reason>.

Source-specific decisions:
- Config fields:
- Snapshot boundary:
- Source position encoding:
- Resume state ownership and token:
- Schema change policy:
- Transaction/page boundary:
- Managed write checkpoint marker:
- Positional stream and history retention:
- History loss signal:
- Integration test fixture:
- Test context helper:
- Real storage factories:

Acceptance criteria:
- <specific build/test/doc checks>
- <phase-specific integration tests to add or extend>
- <specific behavior checks>

Keep the change scoped to this phase. Do not implement later phases unless required for this phase to compile.
```

The most useful prompts name a single phase, a reference module, and the source-specific decisions that are already known. That keeps the agent focused on one contract boundary at a time.
