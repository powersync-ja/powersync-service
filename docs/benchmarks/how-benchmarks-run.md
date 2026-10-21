# How benchmarks run

Each benchmark case describes a workload and pairs it with an implementation that provides the resources needed to run it. Vitest selects the cases and runs their tests. Each test creates a runner, which prepares the resources, runs the workload several times, checks the results, and cleans up.

This document follows one case through that process. The details of the workload vary between suites, but they all use the same base runner lifecycle.

## Starting Vitest

Running `pnpm benchmark:test` from the repository root starts Vitest with the [benchmark configuration](../../packages/service-core-benchmarks/src/vitest.config.ts). This finds the `.bench.ts` files under `src/benchmarks/`. These files use Vitest `test` calls, with the benchmark runner handling the repeated measurements.

The configuration runs one worker and disables parallel execution between files. The case tests also use `sequential: true`, so benchmark cases do not run against the databases at the same time.

Before the tests run, global setup creates the run's artifact folder and records its starting metadata. This folder is shared by all cases in the suite run.

## Creating and selecting cases

As Vitest loads the benchmark files, they import their case lists from [`benchmark-cases.ts`](../../packages/service-core-benchmarks/src/scenarios/benchmark-cases.ts). This builds scenarios from the suite's base cases and selected datasets.

A scenario describes what should happen. It includes the data shape, sync config settings, storage version, mode, tags, timeout, and iteration counts. It does not create the database rows at this point. For example, a dataset might contain:

```ts
{
  name: 'Single table, 1k rows, one bucket',
  data: singleTableData({ rowCount: 1_000, rowBytes: 512 }),
  sync: categorySync({ engine: 'sync-streams', bucketCount: 1 })
}
```

`createDatasetCases` combines this dataset with each base case for the suite. It uses `defineScenario` to validate the dimensions and calculate the scenario ID, tags, client parameters, and expected bucket and operation counts. Each case pairs that scenario with an implementation for its databases or service resources. Custom cases can also be appended with their own dimensions.

The benchmark file uses `describe.each` to register a test for each case. The test receives the scenario's tags and timeout. Vitest uses the tags to select which tests to run. For example:

```sh
pnpm benchmark:test --tags-filter="layer:storage and storage:postgres and profile:quick"
```

Before the selected tests execute, the environment setup checks the database URLs they require. It checks that the URLs are present and valid, and that source and storage locations are separate where required. A storage-only selection does not need source database URLs.

## Creating the runner

When a selected test starts, it creates the suite's runner and passes in the scenario, implementation, resource monitors, and cancellation signal. For example, the storage suite follows this pattern:

```ts
const benchmark = new StorageBenchmark(scenario, implementation, {
  runId: randomUUID(),
  monitors: [new NodeProcessResourceMonitor(), ...createDatabaseResourceMonitors(scenario)],
  signal: AbortSignal.timeout(scenario.timeout_ms - 10_000)
});

const result = await benchmark.run();
```

Other suites use their own runner and monitors. The cancellation signal expires before Vitest's test timeout to allow time for the runner to stop work and clean up.

Each concrete runner extends [`Benchmark`](../../packages/service-core-benchmarks/src/runner/Benchmark.ts). The base class controls the order of operations, while the concrete runner defines how to prepare, execute, verify, and clean up its workload.

Calling `run()` first validates the scenario, including its supported dimensions, iteration counts, and expected counts. If validation passes, `setupRun` opens the resources shared by the case. If either step fails, the runner returns a failed result without starting the iterations.

## Running iterations

The runner completes all warmup iterations first, followed by the measured iterations. For example, a scenario with `warmup_iterations: 1` and `measured_iterations: 3` runs the workload four times.

Warmups follow the same setup, execution, verification, and cleanup steps. Their results are saved, but they do not contribute to the summary statistics and resource monitors do not run for them.

### Preparing an iteration

`setupIteration` prepares the state needed for one execution. This can include creating storage, rendering and deploying the sync config, generating rows, seeding data, or opening a client connection. The work depends on the suite and mode. For example, a streaming case may need to complete an initial sync before its streaming workload can begin.

Setup is outside the measured execution. Each iteration also gets its own metrics collector and access to the case's cancellation signal.

### Executing the workload

For a measured iteration, the base runner starts the resource monitors and then calls `executeIteration`. The concrete runner performs the workload and records timing boundaries and counters.

A timing boundary starts and ends at named events. For example, this simplified fragment measures work until its result becomes visible. The function and event names here are placeholders for the operation being measured:

```ts
runtime.metrics.startBoundary('workload', 'work_started');
const observation = await performWork(runtime.signal);
runtime.metrics.endBoundary('workload', 'result_visible');
runtime.metrics.setCounter('source_rows', observation.rowCount);
```

The runner returns an observation containing the information needed to check the result. Timing boundaries can cover different parts of the execution, so the full `executeIteration` call is not necessarily one measured duration. See the [suite descriptions](./README.md) for the paths each suite measures.

The base runner stops the resource monitors when execution ends, including when it throws an error. Verification happens after the monitors have stopped.

### Checking the result and cleaning up

If execution succeeds, `verifyIteration` checks the observation against the expected result. Depending on the suite, this can check checkpoint positions, bucket contents, operation counts, or data received by a client. A fast result only passes if these checks pass too.

The base runner then finalizes the recorded metrics and calls `cleanupIteration` to release the iteration's resources. It attempts these steps even if execution or verification failed. If setup itself fails, the concrete setup method is responsible for releasing anything it already created.

An iteration fails if it has recorded errors, a failed correctness result, or a failed resource monitor. An unavailable monitor does not fail the iteration. Failures are kept with the iteration so they can be inspected later. A failed warmup also makes the case fail, even though warmups are excluded from the statistics.

## Completing the case

After the iterations, the base runner calculates summary statistics from successful measured iterations. It also collects environment metadata and calls `cleanupRun` to release the resources shared by the case. The final case status is failed if any iteration failed or a case-level error was recorded, including an error during cleanup.

The runner returns the result to the Vitest test. The test saves it before making its assertions:

```ts
await writeBenchmarkResult(result);
expect(result.status, JSON.stringify(result, null, 2)).toBe('passed');
```

This keeps the recorded result available even when the status assertion fails. The suite can also assert that the expected iterations, timing boundaries, counters, and correctness checks are present. Vitest reports the test as passed or failed based on those assertions.

## Completing the suite run

Vitest continues with the remaining selected cases. Once the tests finish, global teardown reads the saved results and uses them alongside Vitest's exit code to update the run manifest. It records the finish time and final run status.

The raw results are now available in the artifact folder. Running `pnpm benchmark:report` is a separate step that turns them into the terminal, JSON, and Markdown reports. See [Benchmark outputs and artifacts](./output-and-artifacts.md) for the file layout and how to read the results.
