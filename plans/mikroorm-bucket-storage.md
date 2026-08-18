# MikroORM Bucket Storage Module Plan

## Goal

Build an experimental bucket storage module backed by MikroORM, with SQLite as the first supported driver and a clean path for future SQL drivers. The module must share entity definitions and storage algorithms by default, while isolating database-specific behavior behind small dialect and migration-lock abstractions.

The first public storage identifier is:

```yaml
storage:
  type: mikroorm:sqlite
```

SQLite storage is intended for single-process unified PowerSync service deployments only. Future drivers can add cross-process notifications and split-runner support through the dialect layer.

## Architecture Decisions

- Start from the PowerSync bucket storage contract, not from a database-first schema design.
- Use MikroORM v7 and its SQL driver stack.
- Use common `defineEntity` schema constants and common TypeScript entity classes for all entities that do not actually vary by driver.
- Do not create empty driver-specific entity subclasses. Add subclasses only when a driver needs methods, hooks, or different behavior.
- Use MikroORM built-in property types for the first SQLite implementation. Avoid custom bigint/blob/json builders unless a later driver proves they are necessary.
- Keep storage algorithms common. Add dialect methods only for query streaming, notification behavior, lock behavior, or raw SQL that cannot be expressed cleanly through typed ORM calls.
- Split high-volume writes into persisted chunks so one `BucketBatch` does not keep every operation in one MikroORM unit of work.
- Stream hot read paths and compaction inputs. Do not load full bucket or compaction result sets into memory.
- Run the shared core storage/sync suites and selected module-postgres replication suites against the MikroORM SQLite storage.

## Module Layout

```text
modules/module-mikroorm-storage/
  package.json
  tsconfig.json
  vitest.config.ts
  README.md
  src/
    index.ts
    mikro-orm.config.ts
    module/
      MikroOrmStorageModule.ts
    types/
      types.ts
    entities/
      entities-index.ts
      common/
        bucket-data.schema.ts
        bucket-parameters.schema.ts
        current-data.schema.ts
        instance.schema.ts
        source-table.schema.ts
        sync-rules.schema.ts
        write-checkpoint.schema.ts
    drivers/
      sqlite/
        SqliteMikroOrmStorageFactory.ts
        SqliteMigrationLockManager.ts
        sqlite-config.ts
        sqlite-dialect.ts
    migrations/
      AbstractMikroOrmMigrationLockManager.ts
      MikroOrmMigrationAgent.ts
      NoOpMigrationStore.ts
      sqlite/
        Migration*_InitialSqliteStorage.ts
    storage/
      MikroOrmBucketBatch.ts
      MikroOrmPersistedBatch.ts
      MikroOrmBucketStorageFactory.ts
      MikroOrmCompactor.ts
      MikroOrmPersistedReplicationStream.ts
      MikroOrmReportStorage.ts
      MikroOrmStorageDialect.ts
      MikroOrmStorageProvider.ts
      MikroOrmSyncRulesStorage.ts
      storage-index.ts
      unsupported.ts
  test/
    src/
      migrations.test.ts
      storage.test.ts
      storage-provider.test.ts
      storage_sync.test.ts
      sync-rules-storage.test.ts
      util.ts
```

## Entity Model

Use common entity classes with MikroORM `defineEntity` schema constants:

```ts
export const BucketDataSchema = defineEntity({
  name: 'BucketData',
  tableName: 'bucket_data',
  properties: {
    id: p.string().primary(),
    groupId: p.integer().fieldName('group_id'),
    bucketName: p.string().fieldName('bucket_name'),
    opId: p.bigint('bigint')
  }
});

export class BucketData extends BucketDataSchema.class {}
BucketDataSchema.setClass(BucketData);
```

The common entity set is:

- `bucket_data`
- `bucket_parameters`
- `current_data`
- `instance`
- `source_tables`
- `sync_rules`
- `write_checkpoints`

Indexes belong in the common entity schema when they represent logical access patterns shared by drivers. SQLite must include indexes for bucket reads, parameter lookup reads, source-table resolution, current-data lookups, write checkpoints, and sync-rule state queries.

Source-table metadata must be JSON-safe. Normalize replica id column type ids to `number` before storing them so MikroORM JSON serialization never sees `bigint` values.

Future driver subclasses should follow the documented MikroORM pattern only when the subclass adds real value:

```ts
export class PostgresBucketData extends BucketDataSchema.class {
  // driver-specific methods or hooks
}
BucketDataSchema.setClass(PostgresBucketData);
```

## Dialect Interface

Common storage code depends on `MikroOrmStorageDialect`, not SQLite imports.

```ts
export interface MikroOrmStorageDialect {
  readonly type: string;
  readonly entityClasses: EntityClass<unknown>[];
  readonly bucketDataEntity: EntityClass<BucketData>;
  readonly bucketParametersEntity: EntityClass<BucketParameters>;
  readonly currentDataEntity: EntityClass<CurrentData>;
  readonly instanceEntity: EntityClass<Instance>;
  readonly sourceTableEntity: EntityClass<SourceTable>;
  readonly syncRulesEntity: EntityClass<SyncRules>;
  readonly writeCheckpointEntity: EntityClass<WriteCheckpoint>;
  streamBucketDataRows(options: MikroOrmBucketDataStreamOptions): AsyncIterable<BucketData>;
  createCheckpointWatcher(): MikroOrmCheckpointWatcher;
}
```

The dialect owns:

- public storage identifier, for example `mikroorm:sqlite`
- entity class registration for MikroORM and migration tooling
- streamed bucket-data reads
- checkpoint watch/notify implementation
- future database-specific raw SQL helpers

SQLite bucket-data streaming should use MikroORM query builder streaming. It must also yield to the event loop before the query so tight single-process polling loops cannot starve replication/checkpoint work.

## SQLite Configuration

SQLite config should be small and explicit:

```yaml
storage:
  type: mikroorm:sqlite
  filename: ./powersync-storage.sqlite
```

Rules:

- `filename` controls the SQLite database file. SQLite creates the file if it does not exist.
- `:memory:` is supported for focused unit tests, but file-backed databases should be used for integration tests that reopen storage with `doNotClear`.
- Do not expose a migrations path in public config. The module owns its bundled migration path.
- Register the module with the service so `mikroorm:sqlite` can be used in normal self-hosted config.
- Include the module in the service Docker build.
- Ensure native `better-sqlite3` bindings are built in the production Docker image.

SQLite must reject split API/sync runner modes. It can run in unified service mode and command/tooling contexts.

## Migrations

Use MikroORM migrations internally and expose them through the standard PowerSync migration surface.

Implementation requirements:

- `MikroOrmMigrationAgent` extends the service migration agent and overrides `run()`.
- The service migration surface triggers the agent.
- MikroORM owns migration discovery and migration state.
- `NoOpMigrationStore` prevents duplicate PowerSync migration bookkeeping.
- `AbstractMikroOrmMigrationLockManager` defines the DB-backed lock contract.
- `SqliteMigrationLockManager` bootstraps its lock table with raw SQLite.

The raw SQLite lock bootstrap is required. Migration locking has a classic chicken-and-egg problem: migrations normally create tables, but a distributed-safe migration trigger needs a table-backed lock before migrations run. For SQLite, use `CREATE TABLE IF NOT EXISTS` in the lock manager before invoking MikroORM migrations.

Generate SQLite migration scripts with the MikroORM CLI. Do not hand-write the initial schema migration except for intentional lock-manager bootstrap SQL.

Migration tests must verify that representative storage tables and indexes are created.

## Storage Implementation

Implement the common storage classes in this order:

1. `MikroOrmStorageProvider`
2. `MikroOrmBucketStorageFactory`
3. `MikroOrmPersistedReplicationStream`
4. `MikroOrmSyncRulesStorage`
5. `MikroOrmBucketBatch`
6. `MikroOrmPersistedBatch`
7. `MikroOrmCompactor`
8. `MikroOrmReportStorage`

`MikroOrmBucketBatch` owns:

- listener notifications
- `resolveTables`
- `save`
- `flush`
- `truncate`
- `drop`
- checkpoint commits
- snapshot state
- write checkpoints

`MikroOrmPersistedBatch` owns:

- per-transaction `current_data` preload
- bucket data evaluation
- parameter data evaluation
- `bucket_data` inserts
- `bucket_parameters` inserts
- `current_data` upserts and pending-delete markers

Flush pending operations in bounded chunks, currently `2_000` source operations per persisted transaction. Log successful flushes with source-operation counts and resulting storage-op ranges.

## Source Tables and Schema Changes

`resolveTables()` must detect both same-name changes and relation-id changes:

- Match the active source table by connection, schema, table name, relation id, and normalized replica id columns.
- When a table is renamed, return old source-table rows in `dropTables` if they share the same `relation_id.object_id`.
- When a table changes replica identity or relevant column type metadata, return old source-table rows in `dropTables`.
- New source-table rows must start with `snapshotDone: false` so initial and triggered snapshots are explicit.

This behavior is required for table recreate, rename, replica identity, and publication-change replication tests.

## Large Rows and Current Data

Current-data persistence must tolerate rows that cannot be serialized to BSON or exceed the maximum persisted row size.

Rules:

- Use a `15 MiB` current-data row limit, matching existing storage behavior.
- If full-row BSON serialization fails or exceeds the limit, log a warning and store a BSON row where each field value is `undefined`.
- Evaluate sync rules against the truncated row so future TOAST-style updates can still be marked unavailable rather than crashing replication.
- Suppress evaluated bucket rows produced only from a truncated row with no usable object id. This prevents placeholder blank-id bucket ops and follow-up blank-id removes.
- Keep legitimate empty-string ids valid when `data.id` is explicitly present.

## Reads and Checkpoints

`MikroOrmSyncRulesStorage.getBucketDataBatch()` should consume dialect-streamed rows and chunk output without buffering the full query result.

Bucket read output must include subkeys whenever `source_table` and `source_key` are present.

`MikroOrmBucketStorageFactory.getReplicatingReplicationStreams()` must return both processing and active streams. Active streams still replicate after initial snapshot completion and must remain visible to replication management.

SQLite checkpoint watching is process-local:

- writes should notify the in-process watcher
- reads should remain cooperative with the event loop
- split-service deployments must be rejected at provider startup

## Compaction

Compaction should stream candidate rows and avoid loading full result sets into memory.

Use typed ORM calls where they are readable and efficient. Use raw SQL for database-specific operations that are awkward or inefficient through MikroORM, and isolate that SQL behind common methods or dialect helpers.

Validate compaction through the shared storage sync tests, especially checkpoint invalidation and bucket batch cases.

## Tests

The module must import shared PowerSync storage tests instead of relying only on bespoke SQLite tests.

Required module-level coverage:

- migration agent tests
- storage provider tests, including SQLite unified-mode guard
- shared storage tests
- shared sync tests across storage protocol versions
- sync-rules storage tests
- compaction tests

The module-postgres replication tests should be runnable against MikroORM SQLite storage with:

```sh
TEST_MONGO_STORAGE=false
TEST_POSTGRES_STORAGE=false
TEST_MIKROORM_SQLITE_STORAGE=true
```

module-postgres integration tests should use a file-backed SQLite storage filename by default. Allow `MIKROORM_SQLITE_STORAGE_TEST_FILENAME` to override it, but do not default those tests to `:memory:` because resume and `doNotClear` flows need storage state to survive factory reopen.

Sequential module-postgres files to run against MikroORM SQLite storage:

- `checkpoints.test.ts`
- `chunked_snapshots.test.ts`
- `large_batch.test.ts`
- `pg_test.test.ts`
- `replica_identity_full.test.ts`
- `replication_retry.test.ts`
- `resuming_snapshots.test.ts`
- `route_api_adapter.test.ts`
- `schema_changes.test.ts`
- `slow_tests.test.ts`
- `storage_combination.test.ts`
- `types/registry.test.ts`
- `validation.test.ts`
- `wal_budget_api.test.ts`
- `wal_budget.test.ts`
- `wal_stream.test.ts`

Some files are skipped unless their suite-specific environment flags are enabled.

## Verification Commands

```sh
source ~/.nvm/nvm.sh && nvm use && corepack pnpm --filter @powersync/service-module-mikroorm-storage build
source ~/.nvm/nvm.sh && nvm use && corepack pnpm --filter @powersync/service-module-mikroorm-storage build:tests
source ~/.nvm/nvm.sh && nvm use && corepack pnpm --filter @powersync/service-module-mikroorm-storage test --run
source ~/.nvm/nvm.sh && nvm use && corepack pnpm --filter @powersync/service-module-postgres build:tests
source ~/.nvm/nvm.sh && nvm use && TEST_MONGO_STORAGE=false TEST_POSTGRES_STORAGE=false TEST_MIKROORM_SQLITE_STORAGE=true corepack pnpm --filter @powersync/service-module-postgres test test/src/wal_stream.test.ts --run
source ~/.nvm/nvm.sh && nvm use && TEST_MONGO_STORAGE=false TEST_POSTGRES_STORAGE=false TEST_MIKROORM_SQLITE_STORAGE=true corepack pnpm --filter @powersync/service-module-postgres test test/src/schema_changes.test.ts --run
```

Expected baseline:

- MikroORM storage source build passes.
- MikroORM storage test build passes.
- Full MikroORM storage suite passes with the shared storage/sync tests.
- module-postgres `wal_stream.test.ts` passes against MikroORM SQLite storage.
- module-postgres `schema_changes.test.ts` passes against MikroORM SQLite storage.

## Build Order From Scratch

1. Create the package, TypeScript config, Vitest config, exports, README, and workspace wiring.
2. Add common MikroORM entity schemas and classes.
3. Add SQLite config and dialect with entity class registration.
4. Configure MikroORM migrations and generate the SQLite initial migration with the MikroORM CLI.
5. Add the DB-backed migration lock and migration agent override.
6. Add provider config for `mikroorm:sqlite`.
7. Register the module with the service module loader and Docker build.
8. Implement factory and sync-rule persistence.
9. Implement writer creation and `MikroOrmBucketBatch`.
10. Split persisted operation writes into `MikroOrmPersistedBatch`.
11. Implement oversized current-data handling.
12. Implement source-table rename and replica-identity drop detection.
13. Implement streamed bucket reads through the dialect.
14. Implement write checkpoints and checkpoint watching.
15. Implement compaction with streamed reads.
16. Add the SQLite unified-runner guard.
17. Import shared storage and sync tests.
18. Add module-postgres MikroORM SQLite storage test wiring.
19. Run the verification commands and the sequential module-postgres files.

## Future Driver Guidance

For Postgres or another SQL driver:

- reuse common entity definitions first
- add concrete entity subclasses only for real driver-specific behavior
- add a new dialect object with its own entity class list
- implement cross-process checkpoint notification in `createCheckpointWatcher()`
- move raw SQL into dialect methods when query shape differs by database
- generate migrations with the MikroORM CLI for that driver
- add a DB-backed migration lock before invoking the migrator
- run the shared storage/sync suites and relevant replication-module integration tests against the new driver

This keeps the module abstract without over-abstracting the entity model before another driver proves what varies.
