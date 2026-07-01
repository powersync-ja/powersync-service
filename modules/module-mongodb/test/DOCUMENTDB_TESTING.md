# Running Tests Against DocumentDB

These instructions cover running the `module-mongodb` test suite against an Azure DocumentDB (formerly Cosmos DB for MongoDB vCore) cluster.

## Prerequisites

- An Azure DocumentDB (Cosmos DB for MongoDB vCore) cluster with change stream support
- Local PostgreSQL for PowerSync's internal storage (not the source database)
- The connection URI for the DocumentDB cluster

> **The open-source `documentdb-local` Docker image cannot be used for these tests.** The
> open-source DocumentDB engine does not implement change streams (`$changeStream is not
supported yet in native pipeline`), does not report the `documentdb_versions` `hello` field
> the suite uses for detection, and presents as `msg: isdbgrid`. An Azure-managed DocumentDB
> (vCore) cluster is required.

## Environment Variables

DocumentDB is detected automatically from the server: the test suite runs
`detectDocumentDb()` once at startup (see `DatabaseType.ts`) and gates the
DocumentDB-specific tests on the result. There is no separate enable flag — pointing
`MONGO_TEST_DATA_URL` at a DocumentDB cluster is what activates the DocumentDB tests.

| Variable                  | Required | Description                                                                                                                                                              |
| ------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `MONGO_TEST_DATA_URL`     | Yes      | DocumentDB connection URI. Must include a database name in the path (see below). Pointing this at a DocumentDB cluster enables the DocumentDB tests.                     |
| `PG_STORAGE_TEST_URL`     | No       | PostgreSQL connection for PowerSync storage. Defaults to `postgres://postgres:postgres@localhost:5432/powersync_storage_test`.                                           |
| `TEST_MONGO_STORAGE`      | No       | Set to `false` to skip MongoDB storage tests. Recommended when testing against DocumentDB to avoid using it as a storage backend.                                        |
| `TEST_TIMEOUT_MULTIPLIER` | No       | Scale factor `testTimeout()` applies to a default timeout on DocumentDB when no explicit cloud override is given. Defaults to **6**. Increase for a slow/remote cluster. |

### Connection URI format

The `MONGO_TEST_DATA_URL` must include a database name in the path. DocumentDB URIs typically don't have one, so you need to add it before the query string:

```
# Original URI (no database):
mongodb+srv://user:pass@cluster.mongocluster.cosmos.azure.com/

# With database added:
mongodb+srv://user:pass@cluster.mongocluster.cosmos.azure.com/powersync_test
```

If your password contains special characters (`=`, `@`, `+`, `/`), they must be URL-encoded in the URI (e.g., `=` becomes `%3D`). DocumentDB auto-generated passwords often contain `=` (base64).

## Commands

All commands run from the module directory: `modules/module-mongodb/`

```bash
# Run all DocumentDB tests (integration + unit helpers):
MONGO_TEST_DATA_URL="mongodb+srv://user:pass@cluster.mongocluster.cosmos.azure.com/powersync_test" \
TEST_MONGO_STORAGE=false \
npx vitest run documentdb --reporter=verbose

# Run only integration tests:
MONGO_TEST_DATA_URL="<uri>" \
TEST_MONGO_STORAGE=false \
npx vitest run documentdb_mode --reporter=verbose

# Run only unit helper tests (no DocumentDB cluster needed):
npx vitest run documentdb_helpers --reporter=verbose

# Run a specific test by name:
MONGO_TEST_DATA_URL="<uri>" \
TEST_MONGO_STORAGE=false \
npx vitest run documentdb_mode -t "resume after restart" --reporter=verbose
```

If you have the URI in an environment variable (e.g., `$DOCUMENTDB_URI`), you can construct the test URL inline:

```bash
DOCUMENTDB_TEST_URL=$(echo "$DOCUMENTDB_URI" | sed 's|\?|powersync_test?|')
MONGO_TEST_DATA_URL="$DOCUMENTDB_TEST_URL" \
TEST_MONGO_STORAGE=false \
npx vitest run documentdb --reporter=verbose
```

## GitHub Actions

The `.github/workflows/documentdb-integration.yml` workflow runs these tests manually via `workflow_dispatch` only. Add a repository or organization secret named `MONGO_TEST_DATA_URL`, then dispatch the workflow.

The URI must include a database name in the path. The tests clear/drop this database as part of setup, so use a dedicated test database and cluster.

The workflow starts a local MongoDB storage service, then runs the complete `modules/module-mongodb` `test` script against that storage backend. `MONGO_TEST_DATA_URL` is the remote DocumentDB source; `MONGO_TEST_URL` points at the local MongoDB storage test instance.

```bash
TEST_MONGO_STORAGE=true
TEST_POSTGRES_STORAGE=false
```

## Test Files

| File                         | Requires DocumentDB       | Description                                                                                                                                                                    |
| ---------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `documentdb_mode.test.ts`    | Yes                       | Integration tests: replication, sentinel checkpoints, write checkpoints, keepalive, resume. Skipped automatically unless `MONGO_TEST_DATA_URL` points at a DocumentDB cluster. |
| `documentdb_helpers.test.ts` | No (1 test needs MongoDB) | Unit tests: `getEventTimestamp`, sentinel parsing/matching, detection logic. Runs against any MongoDB or standalone.                                                           |

## What the Integration Tests Cover

Each integration test runs against 3 storage versions (v1, v2, v3) = 15 integration tests. Plus 15 unit tests in helpers = 30 total.

| Test                    | What it validates                                                                               |
| ----------------------- | ----------------------------------------------------------------------------------------------- |
| basic replication       | Insert, update, delete through change stream with wallTime timestamps                           |
| sentinel checkpoint     | Checkpoint created with `mode: 'sentinel'`, resolved by matching document content in the stream |
| keepalive               | Stream idles past the keepalive interval without crashing on DocumentDB resume tokens           |
| write checkpoint        | Full `createReplicationHead` → sentinel → polling flow for client write consistency             |
| data events not dropped | Verifies `.lte()` dedup guard is skipped — events in the same wall-clock second are not lost    |
| resume after restart    | Stop streaming, create new context, resume from stored token                                    |

There is also a **characterization test**, `does not report collection drop and rename events`,
that documents the current limitation: it writes DDL plus a post-DDL marker, waits for the marker
(proving the stream caught up), then asserts no `drop` / `rename` events were delivered. It passes
today and **fails if a future DocumentDB engine starts delivering DDL events** — a signal to add
real drop/rename replication support.
