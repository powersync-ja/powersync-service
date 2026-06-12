# MikroORM Bucket Storage

Experimental MikroORM-backed bucket storage for the PowerSync service.

This module currently supports SQLite through the `mikroorm:sqlite` storage type. It is intended to prove out a shared MikroORM storage layer that can support more database drivers later, while keeping most bucket storage behavior in common code.

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

Common entity definitions live in `src/entities/common`. The SQLite driver reuses those definitions directly instead of redeclaring SQLite-specific entity classes.

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

## Service Registration

The service image registers this module dynamically under the storage key `mikroorm:sqlite`. The service package depends on `@powersync/service-module-mikroorm-storage`, and `service/src/util/modules.ts` loads `MikroOrmStorageModule` when the config uses this storage type.

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

Generate a new SQLite migration from entity changes:

```sh
corepack pnpm --filter @powersync/service-module-mikroorm-storage mikroorm:generate-migration:sqlite
```

Review generated migrations before committing them.
