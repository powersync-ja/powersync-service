# Drizzle Bucket Storage

Experimental Drizzle-backed bucket storage for the PowerSync service.

The first implementation uses SQLite through `better-sqlite3`:

```yaml
storage:
  type: drizzle:sqlite
  filename: ./powersync-storage.sqlite
```

SQLite storage is restricted to the unified runner because checkpoint
notifications are process-local. `:memory:` is supported for tests and
throwaway development.

## Design

The SQLite driver owns its Drizzle table declarations. Column codecs map
database values to the shared runtime shapes used by storage code, including
lossless `bigint`, `Buffer`, JSON, boolean, and `Date` values. Compile-time
assertions verify that the inferred SQLite query models match the canonical
storage records.

Storage classes use Drizzle queries directly. Database-specific behavior is
kept behind the small dialect surface only where required: transactions,
bucket reads, compaction/raw SQL, and checkpoint notification.

## Migrations

Drizzle Kit owns schema generation and migration metadata:

```sh
corepack pnpm --filter @powersync/service-module-drizzle-storage drizzle:generate:sqlite
corepack pnpm --filter @powersync/service-module-drizzle-storage drizzle:check:sqlite
```

Generated SQL and metadata are committed under `src/migrations/sqlite` and
copied into the built package. Runtime migrations are exposed through the
normal PowerSync migration agent and guarded by a SQLite-backed lock.

## Development

```sh
corepack pnpm --filter @powersync/service-module-drizzle-storage build
corepack pnpm --filter @powersync/service-module-drizzle-storage build:tests
corepack pnpm --filter @powersync/service-module-drizzle-storage test --run
```

PostgreSQL replication tests can opt into this storage with:

```sh
TEST_MONGO_STORAGE=false \
TEST_POSTGRES_STORAGE=false \
TEST_MIKROORM_SQLITE_STORAGE=false \
TEST_DRIZZLE_SQLITE_STORAGE=true \
corepack pnpm --filter @powersync/service-module-postgres test test/src/wal_stream.test.ts --run
```

Set `DRIZZLE_SQLITE_STORAGE_TEST_FILENAME` to override the file-backed test
database path.
