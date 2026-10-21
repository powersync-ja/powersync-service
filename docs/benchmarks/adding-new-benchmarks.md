# Adding to the benchmarking suite

There are several ways to extend the benchmark suite, depending on what you want to measure. From the smallest change to the largest, these are:

1. Adding a record to `baseline-datasets.ts`
2. Adding an item to the `base*BenchmarkCases` in `benchmark-cases.ts`
3. Creating a custom benchmark scenario in the relevant `<type>-scenarios.ts` file
4. Creating a new runner and `.bench.ts` to benchmark a new area of the system.

## Adding to baseline datasets

Use this when all four suites should run against a new data shape. Add an entry to `BASELINE_DATASETS` in [`scenarios/baseline-datasets.ts`](../../packages/service-core-benchmarks/src/scenarios/baseline-datasets.ts), such as:

```ts
{
  name: 'Single table, 1k rows, one bucket',
  data: singleTableData({ rowCount: 1_000, rowBytes: 512 }),
  sync: categorySync({ engine: 'sync-streams', bucketCount: 1 })
}
```

The `data` definition describes the generated source rows and `sync` describes the sync engine and bucket layout. The helpers in `scenario-presets.ts` cover common variations: `singleTableData` accepts row counts, target row sizes, batch sizes, and streaming transaction counts; `categorySync` changes the bucket count. `multiTableData` returns both definitions for an existing multi-table layout.

Each dataset is combined with every base case, covering its configured storage versions, sources, and modes. One entry can therefore add many benchmark runs. Use `profile: 'standard'` or `profile: 'manual'` for more expensive workloads, and set `timeout_ms` when needed. The timeout covers the whole case, including setup, warmups, measured iterations, and cleanup. Without overrides, the dataset inherits the base case's profile and timeout. The default run excludes `profile:manual`.

If only one suite needs the dataset, pass a different list to that suite's `createDatasetCases` call in `benchmark-cases.ts` instead. For example, this extends only the API suite's datasets:

```ts
const apiDatasets = [
  ...BASELINE_DATASETS,
  {
    name: 'Single table, 1k rows, larger payloads',
    data: singleTableData({ rowCount: 1_000, rowBytes: 4_096 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 1 })
  }
];

export const apiBenchmarkCases = createDatasetCases(baseApiBenchmarkCases, apiDatasets);
```

Import any additional preset helpers from `scenario-presets.ts`.

## Adding a base benchmark case

Use the `base*BenchmarkCases` lists in [`benchmark-cases.ts`](../../packages/service-core-benchmarks/src/scenarios/benchmark-cases.ts) when an case should run against every dataset selected for that suite. A case pairs a scenario with the implementation that runs it. The scenario describes the workload and execution settings and the implementation provides access to the relevant database or service resources.

For example, the existing API list creates initial and streaming cases for each PostgreSQL storage version:

```ts
...POSTGRES_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => [
  createPostgresApiCase(createPostgresQuickApiScenario(version)),
  createPostgresApiCase(createPostgresStreamingApiScenario(version))
])
```

Follow the relevant suite's existing case factory when adding another supported variant. Check that its runner and implementation handle the behavior you want to measure since changing a scenario field alone does not add support for a new mode or source.

`createDatasetCases` replaces each base scenario's `data` and `sync` definitions with those from each selected dataset. It also applies any dataset profile and timeout overrides. If your case needs its own data dimensions, append it after expansion as described below.

## Creating a custom scenario

Use a custom scenario when a benchmark needs specific dimensions or execution settings that should apply to only that case and should not be overwritten by the datasets mentioned above. Put reusable scenario builders in the relevant `scenarios/<type>-scenarios.ts` file and register their cases in `benchmark-cases.ts`.

Build from a nearby scenario and pass the result through `defineScenario`. This validates the dimensions and recalculates the scenario ID, tags, client parameters, and expected bucket and operation counts. For example, this builder could be added to `api-scenarios.ts` using its existing imports:

```ts
export function createPostgresLargeRowApiScenario(version: number): ApiBenchmarkScenario {
  return defineScenario<ApiBenchmarkScenario>(
    {
      ...createPostgresQuickApiScenario(version),
      description: 'Initial API sync with larger rows',
      profile: 'standard',
      timeout_ms: 300_000
    },
    singleTableData({ rowCount: 25_000, rowBytes: 1_024, batchSize: 500 }),
    categorySync({ engine: 'sync-streams', bucketCount: 20 })
  );
}
```

Import the builder in `benchmark-cases.ts`, then replace the API export with:

```ts
export const apiBenchmarkCases = [
  ...createDatasetCases(baseApiBenchmarkCases, BASELINE_DATASETS),
  createPostgresApiCase(createPostgresLargeRowApiScenario(2))
];
```

Appending the custom case here preserves its dimensions. It also keeps the case in `allBenchmarkScenarios`, which is used to validate environment requirements for the selected benchmarks.

For table layouts beyond the presets, pass `BenchmarkDataDefinition` and `BenchmarkSyncDefinition` objects directly to `defineScenario`. These still use the existing row generator and sync config renderer. A workload requiring different row contents or query behavior may need to extend those helpers too.

## Creating a new runner and benchmark file

A new runner is useful when the existing runners do not measure the path you need. Start with [`runner/Benchmark.ts`](../../packages/service-core-benchmarks/src/runner/Benchmark.ts) and the closest concrete runner, such as `StorageBenchmark.ts`, which can be used as an example. The base class handles warmup and measured iterations, resource monitors, error collection, and result aggregation. Concrete runners provide the lifecycle hooks:

- `setupRun` and `cleanupRun` manage resources shared across the case.
- `setupIteration` prepares each iteration, including any untimed seeding or initial sync.
- `executeIteration` performs the workload and records timing boundaries and counters.
- `verifyIteration` checks that the expected work actually completed.
- `cleanupIteration` releases iteration resources, and `collectRunMetadata` records environment details.

Define exactly where timing starts and ends before implementing the runner. For example, the existing storage runner measures writes until their checkpoint is safely committed:

```ts
runtime.metrics.startBoundary('storage_write', 'first_writer_save');
commit = await this.writeRows({
  writer: context.writer,
  sourceTables: context.sourceTables,
  manifest: context.manifest,
  signal: runtime.signal
});
runtime.metrics.endBoundary('storage_write', 'checkpoint_safe_commit');
runtime.metrics.setCounter('source_rows', context.manifest.snapshotRowCount);
```

Keep preparation and cleanup outside those boundaries. Verification should establish that the measured operation reached its intended result, such as the expected checkpoint or client data. Ensure setup also releases partially created resources if it fails, since the base class cannot clean up a context that setup never returned.

Add a matching file under `benchmarks/` with a `.bench.ts` suffix, following an existing file such as [`storage.bench.ts`](../../packages/service-core-benchmarks/src/benchmarks/storage.bench.ts). These files use Vitest `test` calls to construct the runner, configure monitors and cancellation, write the result with `writeBenchmarkResult`, and assert correctness. Use the scenario's tags and timeout, and preserve sequential execution so cases do not compete for resources.

Register the cases in `benchmark-cases.ts` and include their scenarios in `allBenchmarkScenarios`. Register new tags in `vitest.config.ts`. A new benchmark layer also needs changes to scenario types and validation, environment checks, metric definitions, and reporting: those currently recognize the four existing layers explicitly. Follow `utils/metric-definitions.ts` when adding throughput calculations or units for new metrics.

## Checking the addition

Follow the [package setup instructions](../../packages/service-core-benchmarks/README.md#usage) to build the service and start the required databases. The, from the repository root, run a focused selection first, for example:

```sh
pnpm benchmark:test --tags-filter="layer:api and storage:postgres and version:2 and profile:standard"
pnpm benchmark:report
```

This selects all matching cases, including the custom API example above. Check that the new case appears in the report, its correctness checks pass, and its timing boundaries and operation counts describe the work you intended to measure. Run a broader selection when the change affects shared generators, runners, or metrics.

For a new addition it may be required to add some new tags for the case if the pre-existing ones to not cover the work required, these tags are defined on the benchmark case in `tags`, and they will need to be added to [`vitest.config.ts`](../../packages/service-core-benchmarks/src/vitest.config.ts) otherwise the test will fail to run.
