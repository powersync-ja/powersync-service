# PowerSync Service Core Benchmarks

Adds benchmarks for the areas of the PowerSync service.

This is generally used to test changes to different areas of the service, allowing for implementation comparisons that help improve the service as a whole.

## Usage

The benchmarks use environment variables to set the location of running instances of databases, which are used within the benchmarks:

| Environment Variable           | Usage                                                         | Default Value                                                      |
| ------------------------------ | ------------------------------------------------------------- | ------------------------------------------------------------------ |
| PG_STORAGE_TEST_URL            | A Postgres URL for storage benchmarks                         | postgres://postgres:postgres@localhost:5432/powersync_storage_test |
| MONGO_TEST_URL                 | A MongoDB URL for storage benchmarks                          | mongodb://localhost:27017/powersync_test                           |
| BENCHMARK_POSTGRES_STORAGE_URL | A Postgres URL for **bucket storage** in replication tests    | NONE                                                               |
| BENCHMARK_POSTGRES_SOURCE_URL  | A Postgres URL for a **source** database in replication tests | NONE                                                               |
| BENCHMARK_MONGODB_SOURCE_URL   | A MongoDB URL for a **source** database in replication tests  | NONE                                                               |
| BENCHMARK_MONGODB_STORAGE_URL  | A MongoDB URL for **bucket storage** in replication tests     | NONE                                                               |

To run the tests call the following commands in the root of this repo:

```
pnpm install
pnpm build
pnpm benchmark:test
```

This installs and builds the service, then runs the entire suite of tests, which may take some time. To combat this, a subset can be selected using the [Vitest test tags](https://vitest.dev/guide/test-tags.html).

For example, running just the quick suite can be be done like so:

```
pnpm benchmark:test --tags-filter="quick"
```

A full list of available tags can be retrieved either from the [vitest config](./src/vitest.config.ts), or by running `pnpm benchmark:test --list-tags`.

After a successful run `pnpm benchmark:report` can be run to generate report, both in CLI, and saved to a `benchmark-artifacts` folder.

### Available commands

- `benchmark:test` - runs the test suite
- `benchmark:report` - generates the report for a previous benchmark run
