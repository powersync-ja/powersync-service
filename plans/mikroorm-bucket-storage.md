# MikroORM Bucket Storage Module Plan

## Goal

Build a bucket storage module backed by MikroORM, with SQLite as the first supported driver and a path for future SQL drivers. The module should use common storage logic and common entity definitions wherever possible, while isolating driver-specific concerns behind a small dialect interface.

The current first target is `mikroorm:sqlite`.

## Lessons Learned

- Start from the shared PowerSync storage contract, not from the database schema alone. The quickest validation came from importing the standard core storage and sync tests into the new module.
- MikroORM already handles the primitive types we need well enough for the SQLite first pass. We do not need custom bigint/blob/json property builders for the initial implementation.
- Do not redeclare entities per driver until the driver needs a real difference. Common `defineEntity` schema constants and common classes are enough for SQLite today.
- Driver-specific classes are still useful in a typical MikroORM project when a concrete entity needs methods, hooks, or driver-only behavior. For this module, keep that option open, but do not create empty SQLite subclasses just for ceremony.
- The dialect abstraction should expose concrete entity classes directly, not a nested `MikroOrmStorageEntities` registry. The extra registry layer did not buy much once the common entity classes became stable.
- Advanced read paths should stream. This matters for bucket reads and compaction, where loading all rows into memory is the wrong shape.
- Batch flushes must be split into smaller persisted chunks. A long-lived `BucketBatch` should not keep every record in one MikroORM unit of work.
- SQLite checkpoint notification is process-local only. The provider should reject split service modes for SQLite and require the unified runner, while future drivers can expose cross-process notification.
- Migration locking is a chicken-and-egg problem. The lock must be created before generated MikroORM migrations run, so the lock manager needs a tiny amount of driver-specific raw SQL.

## Module Layout

```text
modules/module-mikroorm-storage/
  package.json
  tsconfig.json
  vitest.config.ts
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

## Entity Strategy

Use common MikroORM `defineEntity` schema constants plus common TypeScript classes:

```ts
export class BucketData {
  declare id: string;
  declare groupId: number;
  declare bucketName: string;
  declare opId: bigint;
}

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
BucketDataSchema.setClass(BucketData);
```

The common entities currently cover:

- `bucket_data`
- `bucket_parameters`
- `current_data`
- `instance`
- `source_tables`
- `sync_rules`
- `write_checkpoints`

Keep indexes in the common schema when they are part of the logical access pattern and valid for SQLite. Add driver-specific indexes only when a later driver needs a different shape.

Avoid schema factories like `defineBucketDataEntity(options)` and avoid `property-builders` for now. They made sense when we thought every driver would need custom bigint/blob/json storage, but MikroORM's built-in `p.bigint('bigint')`, `p.blob()`, `p.json()`, `p.string()`, and date handling are enough for the current module.

Future drivers can still introduce concrete subclasses using the documented MikroORM pattern:

```ts
export class PostgresBucketData extends BucketDataSchema.class {
  // driver-specific methods or hooks
}
BucketDataSchema.setClass(PostgresBucketData);
```

Only do that when the subclass has a real purpose.

## Dialect Strategy

Storage code should depend on `MikroOrmStorageDialect`, not on SQLite imports. The dialect should expose the small set of driver-specific things the common layer needs:

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

- the public storage identifier, such as `mikroorm:sqlite`
- the entity classes passed to MikroORM and migration tooling
- streaming query implementation for hot bucket reads
- checkpoint watch/notify implementation
- future raw SQL escape hatches where an operation cannot be expressed well through typed ORM calls

Keep common storage classes responsible for the high-level algorithm. Pull database-specific behavior down into dialect methods only when the common implementation would otherwise need driver-specific SQL or notification behavior.

## SQLite First Pass

SQLite-specific choices:

- Use `type: 'mikroorm:sqlite'` as the public storage identifier.
- Use a `db_filename` config value, with `:memory:` supported for fast tests.
- Use MikroORM v7 and its current driver stack. Do not introduce Knex-based helpers.
- Use MikroORM's query builder streaming for bucket reads.
- Use an in-process checkpoint watcher for local wakeups.
- Reject split `api`/`sync` service runners for SQLite storage. SQLite cannot notify sharded pods, so it should only run in unified mode, while command-style contexts used by tooling remain allowed.
- Generate SQLite migrations with the MikroORM CLI instead of hand-writing schema migrations.
- Include indexes needed by the read, write, current-data, source-table, and checkpoint paths in the generated migration.

## Migrations

Migrations should use MikroORM migrations internally and be exposed through the standard PowerSync service migration surface.

Implementation shape:

- `MikroOrmMigrationAgent` extends the service migration agent and overrides `run()`.
- The service migration layer triggers the agent, but MikroORM owns migration discovery and migration state.
- `NoOpMigrationStore` exists because PowerSync should not duplicate MikroORM migration bookkeeping.
- `AbstractMikroOrmMigrationLockManager` defines the DB-backed lock contract.
- `SqliteMigrationLockManager` bootstraps its own lock table with raw SQLite.

The raw SQLite lock bootstrap is intentional. We need a DB-backed lock before migrations run, but migrations are what normally create tables. For SQLite, the lock manager solves that classic chicken-and-egg problem with `CREATE TABLE IF NOT EXISTS` against MikroORM's connection before invoking the migrator.

Tests should verify that the migration agent creates representative storage tables, not just that the migrator returns successfully.

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

`MikroOrmBucketBatch` should own the public writer lifecycle:

- listener notifications
- `resolveTables`
- `save`
- `flush`
- `truncate`
- `drop`
- checkpoint commits
- snapshot state
- write checkpoints

`MikroOrmPersistedBatch` should own per-transaction write state:

- preload `current_data` for a chunk
- evaluate bucket data and parameter rows
- insert `bucket_data`
- insert `bucket_parameters`
- upsert or mark-delete `current_data`

Flush pending operations in chunks, currently `2_000` operations per persisted transaction. This keeps high-volume sync tests fast and avoids growing a single MikroORM unit of work across tens of thousands of rows.

## Reads, Streaming, and Compaction

Bucket reads should stream rows from the dialect through `streamBucketDataRows()`. The common sync path should consume the async iterable and batch output without buffering the entire query result.

Compaction should also stream candidate rows. Use typed ORM calls when they express the operation cleanly. Use raw SQL only for database-specific operations that are awkward or inefficient through MikroORM, and isolate those operations behind common methods or dialect-level helpers.

The compactor should be validated with the shared sync tests, especially checkpoint invalidation cases.

## Test Strategy

The module should import and run the shared core tests instead of building only bespoke SQLite tests.

Current expected coverage:

- shared storage tests
- shared sync tests across storage protocol versions
- migration agent tests
- sync-rules storage tests
- provider/service-mode tests

Known verification commands:

```sh
source ~/.nvm/nvm.sh && nvm use && corepack pnpm --filter @powersync/service-module-mikroorm-storage build
source ~/.nvm/nvm.sh && nvm use && corepack pnpm --filter @powersync/service-module-mikroorm-storage build:tests
source ~/.nvm/nvm.sh && nvm use && corepack pnpm --filter @powersync/service-module-mikroorm-storage test test/src/storage_sync.test.ts --run
source ~/.nvm/nvm.sh && nvm use && corepack pnpm --filter @powersync/service-module-mikroorm-storage test --run
```

Current observed status after implementing the SQLite vertical slice:

- source build passes
- test build passes
- sync suite passes with `51 passed`
- full module suite passes with `223 passed | 2 skipped`

There has been an intermittent native SQLite/Vitest `139` exit during full-suite startup. Rerunning has passed cleanly, so treat this as runner instability unless a failure includes test-level assertions.

## Build Order From Scratch

1. Create the package, TypeScript config, Vitest config, exports, and workspace wiring.
2. Add the common entity schemas and classes.
3. Add SQLite config and dialect with entity class registration.
4. Configure MikroORM migrations and generate the SQLite initial migration with the MikroORM CLI.
5. Add the DB-backed migration lock and migration agent override.
6. Add provider config for `mikroorm:sqlite` and register it with the service storage engine.
7. Implement factory and sync-rule persistence.
8. Implement writer creation and `MikroOrmBucketBatch`.
9. Split persisted operation writes into `MikroOrmPersistedBatch`.
10. Implement streamed bucket reads through the dialect.
11. Implement write checkpoints and checkpoint watching.
12. Implement compaction with streaming.
13. Add the SQLite unified-runner guard.
14. Import the shared storage and sync tests, then fill gaps until they pass.
15. Run focused sync tests and the full module suite.

## Future Driver Guidance

For Postgres or another SQL driver:

- reuse the common entity definitions first
- add concrete entity subclasses only when methods/hooks/driver behavior require them
- add a new dialect object with its own entity class list
- implement cross-process checkpoint notification in `createCheckpointWatcher()`
- move raw SQL into dialect methods when query shape differs by database
- keep migrations generated by MikroORM for that driver
- add driver-specific migration locking before invoking the migrator

This keeps the module abstract without over-abstracting the entity model before a second driver proves what actually varies.
