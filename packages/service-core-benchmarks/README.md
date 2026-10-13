# PowerSync Service Core Benchmarks

Adds benchmarks for the areas of the PowerSync service.

This is generally used to test changes to different areas of the service, allowing for implementation comparisons that help improve the service as a whole.

## Adding new benchmarks

- Add a dataset to `baseline-datasets.ts` when all suites should run it.
- Pass a suite-specific dataset list to `createDatasetCases` in `benchmark-cases.ts` when only that suite needs it.
- Add a custom test to the list of cases for that suite in `benchmark-cases.ts`

The builders in `scenario-presets.ts` are configurable: `singleTableData`, `multiTableData`, and `categorySync` accept custom dimensions. `defineScenario` recalculates based identity, tags, client parameters, and expected operation counts. For example, a larger API case could be authored in `benchmark-cases.ts` as:

```ts
import { createPostgresApiCase, createPostgresQuickApiScenario } from './api-scenarios.js';
import { categorySync, defineScenario, singleTableData } from './scenario-presets.js';

const customApiCase = createPostgresApiCase(
  defineScenario(
    {
      ...createPostgresQuickApiScenario(2),
      description: 'Initial API sync with larger rows'
    },
    singleTableData({ rowCount: 25_000, rowBytes: 1_024, batchSize: 500 }),
    categorySync({ engine: 'sync-streams', bucketCount: 20 })
  )
);

export const apiBenchmarkCases = [...createDatasetCases(baseApiBenchmarkCases, BASELINE_DATASETS), customApiCase];
```

Append custom cases after dataset expansion to preserve their dimensions. Adding them to a `base*BenchmarkCases` list replaces their data and sync definitions with each selected dataset.

For arbitrary table layouts, pass `BenchmarkDataDefinition` and `BenchmarkSyncDefinition` objects directly to `defineScenario`. `makeStorageScenario` also supports custom storage dimensions and execution settings.

## Usage

The benchmarks use environment variables to set the location of running instances of databases, which are used within the benchmarks:

| Environment Variable           | Usage                                                         | Default Value |
| ------------------------------ | ------------------------------------------------------------- | ------------- |
| BENCHMARK_POSTGRES_STORAGE_URL | A Postgres URL for bucket storage in all benchmarks           | NONE          |
| BENCHMARK_POSTGRES_SOURCE_URL  | A Postgres URL for a **source** database in replication tests | NONE          |
| BENCHMARK_MONGODB_SOURCE_URL   | A MongoDB URL for a **source** database in replication tests  | NONE          |
| BENCHMARK_MONGODB_STORAGE_URL  | A MongoDB URL for bucket storage in all benchmarks            | NONE          |

For ease of running the suite, a docker-compose file exists to start up the required databases for a run. To start the databases, run from the repository root:

```sh
docker compose -f packages/service-core-benchmarks/docker-compose.yaml up -d --wait
```

Then export these urls in the shell that the benchmarks will be run in:

```sh
export BENCHMARK_POSTGRES_STORAGE_URL=postgres://postgres:postgres@localhost:5431/powersync_storage_test
export BENCHMARK_POSTGRES_SOURCE_URL=postgres://postgres:postgres@localhost:5432/powersync_source_test
export BENCHMARK_MONGODB_SOURCE_URL=mongodb://localhost:27017/powersync_test_data
export BENCHMARK_MONGODB_STORAGE_URL=mongodb://localhost:27018/powersync_test
```

This can then be stopped by running `docker compose -f packages/service-core-benchmarks/docker-compose.yaml down`.

To run the tests call the following commands in the root of this repo:

```
pnpm install
pnpm build
pnpm benchmark:test
```

This installs and builds the service, then runs the entire suite of tests, which may take some time. To combat this, a subset can be selected using the [Vitest test tags](https://vitest.dev/guide/test-tags.html).

For example, running just the quick suite can be be done like so:

```sh
pnpm benchmark:test --tags-filter="profile:quick"
```

Alternatively, set the `BENCHMARK_TAGS_FILTER` environment variable:

```sh
BENCHMARK_TAGS_FILTER="profile:quick and storage:postgres" pnpm benchmark:test
```

A full list of available tags can be retrieved either from the [vitest config](./src/vitest.config.ts), or by running `pnpm benchmark:test --list-tags`.

After a run, `pnpm benchmark:report` can be run to generate report, both in CLI, and saved to a `benchmark-artifacts` folder.

### Database CPU and memory

The suite records CPU time and sampled memory usage for local MongoDB and PostgreSQL Docker containers during measured iterations. Replication and combined benchmarks report source and storage databases separately. The monitor matches each database URL to a unique container by its published local port, so source and storage services need distinct ports. The reported container memory excludes inactive file cache and is different from process RSS.

If Docker is inaccessible, the URL points to a remote database, or no unique container matches, the database resource result is `unavailable` and the benchmark continues. Local runs can use `DOCKER_HOST=unix:///path/to/docker.sock` when the Docker socket is not at `/var/run/docker.sock`.

### Running from a pull request comment

Repository members and owners can trigger the benchmark workflow by commenting on an open pull request:

```
Benchmark this
```

To select a subset, add a Vitest tag expression after "Benchmark this", this will then be passed as an environment variable to the run

```
Benchmark this profile:quick
Benchmark this profile:quick and storage:postgres
Benchmark this (layer:api or layer:storage) and not storage:mongodb
```

### Available commands

- `benchmark:test` - runs the test suite
- `benchmark:report` - generates the report for a previous benchmark run

### Reports and run history

Each time `benchmark:test` is run, a new folder is created in `benchmark-artifacts`, within a unique `<run-id>/` folder, This folder will then contain the metrics and reports for that given run to prevent lost data over multiple runs, or different runs of the benchmarking suite to maintain previous runs in its own results.

Within the benchmark-artifacts folder will be a `latest.json` file containing the run-id for the most recent benchmark run.

Run `pnpm benchmark:report` to report the latest **started** suite, including failed or empty runs. It never falls back to an older successful run. To select a retained run explicitly:

```sh
pnpm benchmark:report --run-id <run-id>
```

When a report is made, it will take the raw data from the run's `json` folder, which is will then collate into a report, This report will calculate some metrics from the raw data, and present that data with the appropriate units, both in the console, and saved to a `report` folder within the run, containing the formatted data as `json` and `markdown`
