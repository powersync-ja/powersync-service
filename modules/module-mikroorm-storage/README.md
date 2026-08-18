# MikroORM Bucket Storage

Experimental MikroORM-backed bucket storage for the PowerSync service.

This module currently supports SQLite through the `mikroorm:sqlite` storage type and has an initial MySQL dialect through `mikroorm:mysql`. It is intended to prove out a shared MikroORM storage layer that can support more database drivers while keeping most bucket storage behavior in common code.

## Status

This module is experimental. The SQLite implementation is useful for local development, tests, and validating the MikroORM storage architecture. It is not yet intended as the default production storage backend.

SQLite storage should only be used when running PowerSync in unified mode. Split API and sync runners are rejected because SQLite checkpoint notifications are process-local and cannot notify separate service processes or pods.

## How It Works

The module stores PowerSync bucket state in MikroORM entities:

- `sync_rules` stores deployed sync rule versions and replication stream state.
- `source_tables` stores source table metadata and snapshot progress.
- `bucket_data` stores bucket operations.
- `bucket_parameters` stores materialized parameter lookups.
- `current_data` stores the latest known source-row state used by write batching and compaction.
- `write_checkpoints` stores custom write checkpoint state.
- `instance` stores the PowerSync storage instance id.

Common entity definitions live in `src/entities/common`. The SQLite and MySQL drivers reuse those definitions directly instead of redeclaring driver-specific entity classes.

Common storage classes implement most behavior:

- `MikroOrmBucketStorageFactory` manages sync rule versions and storage instances.
- `MikroOrmSyncRulesStorage` implements the sync-rule-specific storage surface.
- `MikroOrmBucketBatch` owns the writer lifecycle, checkpoints, truncates, and snapshot state.
- `MikroOrmPersistedBatch` persists write chunks in smaller transactions.
- `MikroOrmCompactor` compacts bucket history.
- `MikroOrmStorageDialect` isolates driver-specific streaming and checkpoint notification behavior.

Migrations are generated and run by MikroORM, but exposed through the standard PowerSync migration surface. Migration execution is guarded by a database-backed lock. For SQLite, that lock table is bootstrapped with raw SQLite before MikroORM migrations run, because migration locking has to work before the normal storage tables exist.

## Self-Hosted Configuration

Use `storage.type: mikroorm:sqlite` and provide a SQLite `filename`.

File-backed SQLite example:

```yaml
replication:
  connections:
    - type: postgresql
      uri: !env PS_DATA_SOURCE_URI
      sslmode: disable

storage:
  type: mikroorm:sqlite
  filename: ./powersync-storage.sqlite

port: 8080

sync_rules:
  path: sync-rules.yaml

client_auth:
  jwks_uri: !env PS_JWKS_URI
  audience: ['powersync']
```

In-memory SQLite example for local tests and throwaway development:

```yaml
replication:
  connections:
    - type: postgresql
      uri: postgres://postgres:mypassword@localhost:5432/postgres
      sslmode: disable

storage:
  type: mikroorm:sqlite
  filename: ':memory:'

port: 8080

sync_rules:
  path: sync-rules.yaml
```

Only use this storage type when starting PowerSync in unified mode. Do not run separate API and sync runners against the same SQLite storage file.

MySQL example:

```yaml
replication:
  connections:
    - type: postgresql
      uri: !env PS_DATA_SOURCE_URI
      sslmode: disable

storage:
  type: mikroorm:mysql
  uri: !env PS_STORAGE_MYSQL_URI

port: 8080

sync_rules:
  path: sync-rules.yaml
```

The MySQL dialect is newer than SQLite and should be treated as experimental. It currently uses the common in-process checkpoint watcher; database-backed notifications can be added behind `MikroOrmStorageDialect` when needed.

## SQLite Concurrency

For file-backed SQLite, the module enables WAL mode and uses MikroORM read replicas to open separate SQLite handles when `storage.max_pool_size` is greater than `1`. This allows SQLite-level read-while-write behavior for API reads during replication writes.

The current MikroORM SQLite stack uses Kysely's `better-sqlite3` dialect. Those query calls are asynchronous in shape, but they execute on synchronous `better-sqlite3` handles rather than being delegated to worker threads. This means WAL and read replicas improve database handle concurrency, but a long-running SQLite statement can still occupy the Node.js event loop for the process executing it.

The PowerSync SDK has a Node SQLite dialect that delegates SQLite work to workers. That may be a future workaround if this module needs stronger read concurrency while retaining SQLite storage.

## Service Registration

The service image registers this module dynamically under the storage keys `mikroorm:sqlite` and `mikroorm:mysql`. The service package depends on `@powersync/service-module-mikroorm-storage`, and `service/src/util/modules.ts` loads `MikroOrmStorageModule` when the config uses one of these storage types.

The module also contributes its config type to the generated PowerSync config schema.

## Development

Use pnpm through Corepack:

```sh
corepack pnpm --filter @powersync/service-module-mikroorm-storage build
corepack pnpm --filter @powersync/service-module-mikroorm-storage build:tests
corepack pnpm --filter @powersync/service-module-mikroorm-storage test --run
```

Focused sync suite:

```sh
corepack pnpm --filter @powersync/service-module-mikroorm-storage test test/src/storage_sync.test.ts --run
```

Opt into MySQL storage tests by setting a test database URI:

```sh
MIKROORM_MYSQL_STORAGE_TEST_URI=mysql://repl_user:good_password@localhost:3306/powersync \
  corepack pnpm --filter @powersync/service-module-mikroorm-storage test test/src/mysql-storage.test.ts --run
```

Generate a new SQLite migration from entity changes:

```sh
corepack pnpm --filter @powersync/service-module-mikroorm-storage mikroorm:generate-migration:sqlite
```

Generate a MySQL migration from entity changes:

```sh
MIKRO_ORM_MYSQL_URI=mysql://repl_user:good_password@localhost:3306/powersync \
  corepack pnpm --filter @powersync/service-module-mikroorm-storage mikroorm:generate-migration:mysql
```

Generate migrations for all supported dialects:

```sh
corepack pnpm --filter @powersync/service-module-mikroorm-storage mikroorm:generate-migrations
```

Generate initial migrations for all supported dialects:

```sh
corepack pnpm --filter @powersync/service-module-mikroorm-storage mikroorm:generate-initial-migrations
```

Review generated migrations before committing them.
