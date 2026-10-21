# Benchmark outputs and artifacts

The benchmark suite saves the results of each run so they can be inspected later. These include the measurements from each iteration, correctness checks, environment details, and any errors. A separate report command turns these results into tables for the terminal and files that can be shared.

## Generating a report

After running the benchmarks, run `pnpm benchmark:report` from the project root. This prints the report to the terminal and saves JSON and Markdown copies in the run's `report` folder. The report starts with an overview of the cases, followed by detailed metrics and diagnostics for each case.

By default, the command selects the latest started run including runs that failed or produced no results, it does not fall back to an earlier successful run. A report for a previous run can be produced by running the report command with `--run-id`, like so:

```sh
pnpm benchmark:report --run-id <run-id>
```

The run ID is printed when the benchmark suite starts and is also used as the run's folder name. Generating a report again updates the report files for that run without changing its raw results.

## Artifact files

Artifacts are saved under `packages/service-core-benchmarks/benchmark-artifacts/`. Each run gets a separate folder, so later runs do not overwrite earlier results:

```text
benchmark-artifacts/
    latest.json
    runs/
        <run-id>/
            run.json
            json/
                <scenario-id>.json
            report/
                output.json
                output.md
```

| File                              | Contents                                                                                                                                   |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `latest.json`                     | The ID of the latest started run. Used when no `--run-id` is provided.                                                                     |
| `run.json`                        | Run metadata, including the commit SHA, start and finish times, command arguments, environment tag filter, status, and exit code.          |
| `json/<scenario-id>.json`         | The result for one benchmark case, including all recorded iterations. The scenario ID is URL-encoded for use as a filename.                |
| `report/output.json`              | The run metadata and all case results, with summaries recalculated from the recorded iterations.                                           |
| `report/output.md`                | The formatted report, including the overview, detailed metrics, and diagnostics. Use this for reading or sharing results.                  |

The `report` folder is created when the run starts, but its output files are only written when the report command runs.

## Raw benchmark results

Each case result contains these fields:

| Field         | Contents                                                                                                                         |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `scenario`    | The case ID, workload, storage and source settings, tags, and iteration settings.                                                |
| `status`      | Whether the case passed or failed.                                                                                               |
| `environment` | Details collected by the runner about the databases and service used.                                                            |
| `iterations`  | The measurements, correctness checks, and errors for each warmup and measured iteration.                                         |
| `summary`     | Statistics calculated from the iterations. This can be `null` if the case failed before results could be summarized.             |
| `errors`      | Errors raised during run setup, aggregation, metadata collection, or cleanup. Iteration errors are stored within each iteration. |

Within an iteration, `boundaries` records the timed operations, including their start and end events and duration in milliseconds. `counters` records how much work was done, such as source rows, bucket operations, or bytes read. `resources` contains CPU and memory measurements for each monitored component. `correctness` records whether the expected data or checkpoint was observed.

Warmup iterations are kept in the raw results, but are excluded from the statistics. Resource monitors run only for measured iterations.

## Reading the metrics

The overview shows each case's status, successful and failed measured iteration counts, and timing and throughput statistics for a selected boundary. The detailed tables show all recorded metrics, grouped as follows:

| Group    | Meaning                                                                                                                        |
| -------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Timing   | How long a defined operation took, in milliseconds. Examples include `storage_write`, `http_read`, and `end_to_end_streaming`. |
| Counter  | The amount of work recorded during an iteration, with a unit such as rows, operations, or bytes.                               |
| Rate     | A counter divided by a timing boundary's duration, expressed per second.                                                       |
| Resource | CPU time and memory usage for a monitored component, such as `service`, `load_generator`, or `storage_database`.               |

Timing boundaries depend on the benchmark and its mode. For example, the API benchmark's initial `http_read` includes opening the request, while its streaming `http_read` starts after the storage writes are committed. See the [suite descriptions](./README.md) for the measured paths. Some boundaries overlap, so their durations should not be added together.

Use the detailed tables when the overview does not show the boundary you need. For example, in combined streaming cases, `end_to_end_streaming` is in the detailed metrics whereas the overview selects `end_to_end_snapshot`.

Counters also depend on what is being measured. A source row can produce several bucket operations if it belongs to several sync streams. For this reason, a rate in operations per second should be compared with the same rate and workload in another run. Rates are calculated for each iteration before being summarized. For example, 1,000 operations over 500 ms gives 2,000 operations per second.

### Statistics and sample counts

Statistics use successful measured iterations only. Each metric shows its sample count, minimum, median, average, and maximum. A failed case can still have statistics from iterations that passed, so check the status and failure counts before comparing results.

The report includes p95 when a metric has at least 20 samples and p99 when it has at least 100. These are percentiles across iteration measurements. They do not describe the latency of individual rows or requests within an iteration.

A dash (`—`) means no value is available. Rate samples with a missing or non-positive duration, or a non-finite result, are excluded and explained in the metric's notes.

### CPU and memory

CPU values are time spent using the CPU, in milliseconds. They are separate from elapsed time. Memory values are recorded in bytes, with baseline, peak, average, and change values where the monitor supports them.

Resource measurements cover the monitor's execution window rather than an individual timing boundary. Process memory uses RSS. Database memory uses Docker container usage with inactive file cache excluded. 

A monitor can be `available`, `unavailable`, or `failed`. For example, database measurements can be unavailable when the database is remote or Docker cannot be accessed. An unavailable monitor does not fail the benchmark. The report includes the reason, and its missing measurements are not treated as zero.

## Failed or incomplete runs

Start with the status in `run.json` and the case statuses in the report. A run starts as `running` and is updated to `passed`, `failed`, or `interrupted` when it finishes. If the process stops before it can update the manifest, the status can remain `running`.

The detailed report lists errors by phase and names any failed correctness checks. Open the case's raw JSON for the full check details and individual measurements. The [benchmark runner](../../packages/service-core-benchmarks/src/runner/Benchmark.ts) defines the setup, execution, verification, and cleanup phases.

Results are written when a case's runner returns. An abrupt stop or a failure before that point can leave the run with missing case files or no results at all. Check the Vitest output or workflow logs alongside the artifacts when investigating these runs.

## Artifacts from CI

The [benchmark workflow](../../.github/workflows/benchmark.yml) runs the report command, adds the report to the GitHub Actions job summary, and uploads the run folder as an artifact named `benchmark-<workflow-run-id>-<attempt>`.

The pull request comment includes an overview, detailed metrics when they fit, and links to the workflow and uploaded artifacts. Large reports are shortened to fit the comment limit. Download the artifact to read the complete `report/output.md` or inspect the raw JSON files. The workflow also attempts reporting and artifact upload after benchmark failures, provided the run folder was created and the workflow was not cancelled.
