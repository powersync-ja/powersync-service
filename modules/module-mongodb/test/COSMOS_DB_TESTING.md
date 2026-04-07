# Running Tests Against Cosmos DB

These instructions cover running the `module-mongodb` test suite against an Azure Cosmos DB for MongoDB vCore cluster.

## Prerequisites

- A Cosmos DB for MongoDB vCore cluster with change stream support
- Local PostgreSQL for PowerSync's internal storage (not the source database)
- The connection URI for the Cosmos DB cluster

## Environment Variables

| Variable              | Required | Description                                                                                                                      |
| --------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `COSMOS_DB_TEST`      | Yes      | Set to `true` to enable Cosmos DB integration tests. Without this, all tests in `cosmosdb_mode.test.ts` are skipped.             |
| `MONGO_TEST_DATA_URL` | Yes      | Cosmos DB connection URI. Must include a database name in the path (see below).                                                  |
| `PG_STORAGE_TEST_URL` | No       | PostgreSQL connection for PowerSync storage. Defaults to `postgres://postgres:postgres@localhost:5432/powersync_storage_test`.   |
| `TEST_MONGO_STORAGE`  | No       | Set to `false` to skip MongoDB storage tests. Recommended when testing against Cosmos DB to avoid using it as a storage backend. |

### Connection URI format

The `MONGO_TEST_DATA_URL` must include a database name in the path. Cosmos DB URIs typically don't have one, so you need to add it before the query string:

```
# Original URI (no database):
mongodb+srv://user:pass@cluster.mongocluster.cosmos.azure.com/?tls=true

# With database added:
mongodb+srv://user:pass@cluster.mongocluster.cosmos.azure.com/powersync_test?tls=true
```

If your password contains special characters (`=`, `@`, `+`, `/`), they must be URL-encoded in the URI (e.g., `=` becomes `%3D`). Cosmos DB auto-generated passwords often contain `=` (base64).

## Commands

All commands run from the module directory: `modules/module-mongodb/`

```bash
# Run all Cosmos DB tests (integration + unit helpers):
COSMOS_DB_TEST=true \
MONGO_TEST_DATA_URL="mongodb+srv://user:pass@cluster.mongocluster.cosmos.azure.com/powersync_test?tls=true" \
TEST_MONGO_STORAGE=false \
npx vitest run cosmosdb --reporter=verbose

# Run only integration tests:
COSMOS_DB_TEST=true \
MONGO_TEST_DATA_URL="<uri>" \
TEST_MONGO_STORAGE=false \
npx vitest run cosmosdb_mode --reporter=verbose

# Run only unit helper tests (no Cosmos DB cluster needed):
npx vitest run cosmosdb_helpers --reporter=verbose

# Run a specific test by name:
COSMOS_DB_TEST=true \
MONGO_TEST_DATA_URL="<uri>" \
TEST_MONGO_STORAGE=false \
npx vitest run cosmosdb_mode -t "resume after restart" --reporter=verbose
```

If you have the URI in an environment variable (e.g., `$COSMOSDB_URI`), you can construct the test URL inline:

```bash
COSMOS_TEST_URL=$(echo "$COSMOSDB_URI" | sed 's|\?|powersync_test?|')
COSMOS_DB_TEST=true \
MONGO_TEST_DATA_URL="$COSMOS_TEST_URL" \
TEST_MONGO_STORAGE=false \
npx vitest run cosmosdb --reporter=verbose
```

## Test Files

| File                       | Requires Cosmos DB        | Description                                                                                                                       |
| -------------------------- | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `cosmosdb_mode.test.ts`    | Yes                       | Integration tests: replication, sentinel checkpoints, write checkpoints, keepalive, resume. Skipped unless `COSMOS_DB_TEST=true`. |
| `cosmosdb_helpers.test.ts` | No (1 test needs MongoDB) | Unit tests: `getEventTimestamp`, sentinel parsing/matching, detection logic. Runs against any MongoDB or standalone.              |

## What the Integration Tests Cover

| Test                 | What it validates                                                                               |
| -------------------- | ----------------------------------------------------------------------------------------------- |
| basic replication    | Insert, update, delete through change stream with wallTime timestamps                           |
| sentinel checkpoint  | Checkpoint created with `mode: 'sentinel'`, resolved by matching document content in the stream |
| keepalive            | Stream idles past the keepalive interval without crashing on Cosmos DB resume tokens            |
| write checkpoint     | Full `createReplicationHead` → sentinel → polling flow for client write consistency             |
| resume after restart | Stop streaming, create new context, resume from stored token                                    |

## Known Issues

- **Resume on storage v2**: The "resume after restart" test intermittently fails on storage v2 only (v1 and v3 pass). This appears to be a storage-version-specific issue, not a Cosmos DB detection or resume token problem.
