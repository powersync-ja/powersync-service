import { createCoreAPIMetrics, JwtPayload, storage, sync, updateSyncRulesFromYaml } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { performance } from 'node:perf_hooks';
import { afterAll, test } from 'vitest';
import {
  BATCH_OPTIONS,
  deploySyncRules,
  METRICS_HELPER,
  PARSE_OPTIONS,
  resolveTestTable
} from '../test-utils/test-utils-index.js';

const BENCHMARK_SYNC_RULES = `
bucket_definitions:
  by_list:
    parameters:
      - SELECT id AS list_id FROM lists
    data:
      - SELECT id, list_id, owner_id, description, completed FROM todos WHERE list_id = bucket.list_id
  by_user:
    parameters:
      - SELECT owner_id AS owner_id FROM lists
    data:
      - SELECT id, list_id, owner_id, description, completed FROM todos WHERE owner_id = bucket.owner_id
`;

export interface StorageBenchmarkScenario {
  name: string;
  /** Number of todo rows to persist and expose through data buckets. */
  todo_row_count: number;
  /** Number of list rows to persist and expose as parameter buckets. */
  list_row_count: number;
  /** Number of distinct users spread over list and todo rows. */
  user_count: number;
  /** Flush pending source rows after this many saves. */
  flush_every: number;
  /** Maximum expected number of resolved buckets for this scenario. */
  max_bucket_count?: number;
  /** Optional per-scenario Vitest timeout. */
  timeout_ms?: number;
}

export interface RegisterStorageBenchmarkOptions {
  storageName: string;
  storageVersion?: number;
  scenarios?: StorageBenchmarkScenario[];
  results?: StorageBenchmarkResult[];
  timeout_ms?: number;
  progress_interval_ms?: number;
}

export interface StorageBenchmarkResult {
  storage: string;
  version: number | null;
  scenario: string;
  source_rows: number;
  buckets: number;
  write_ms: number;
  write_rows_per_second: number;
  write_mebibytes_per_second: number;
  sync_drain_ms: number;
  ops: number;
  read_mebibytes_per_second: number;
}

export interface StorageBenchmarkOutput {
  schema_version: 1;
  generated_at: string;
  results: StorageBenchmarkResult[];
}

export const STORAGE_BENCHMARK_OUTPUT_PATH_ENV = 'POWERSYNC_STORAGE_BENCHMARK_OUTPUT';

const STORAGE_BENCHMARK_ROW_PERMUTATIONS = [
  {
    name: '1k-todos',
    todo_row_count: 1_000,
    timeout_ms: 300_000
  },
  {
    name: '10k-todos',
    todo_row_count: 10_000,
    timeout_ms: 600_000
  },
  {
    name: '100k-todos',
    todo_row_count: 100_000,
    timeout_ms: 1_800_000
  },
  {
    name: '1m-todos',
    todo_row_count: 1_000_000,
    timeout_ms: 10_800_000
  }
];

const STORAGE_BENCHMARK_BUCKET_PERMUTATIONS = [
  { name: '200-buckets', list_row_count: 100, bucket_count: 200, minimum_timeout_ms: 0 },
  { name: '1k-buckets', list_row_count: 500, bucket_count: 1_000, minimum_timeout_ms: 0 },
  { name: '10k-buckets', list_row_count: 5_000, bucket_count: 10_000, minimum_timeout_ms: 900_000 },
  { name: '20k-buckets', list_row_count: 10_000, bucket_count: 20_000, minimum_timeout_ms: 1_200_000 }
];

export const DEFAULT_STORAGE_BENCHMARK_SCENARIOS: StorageBenchmarkScenario[] =
  STORAGE_BENCHMARK_ROW_PERMUTATIONS.flatMap((rowPermutation) =>
    STORAGE_BENCHMARK_BUCKET_PERMUTATIONS.map((bucketPermutation) => ({
      name: `${rowPermutation.name}-${bucketPermutation.name}`,
      todo_row_count: rowPermutation.todo_row_count,
      list_row_count: bucketPermutation.list_row_count,
      user_count: bucketPermutation.list_row_count,
      flush_every: 1_000,
      max_bucket_count: bucketPermutation.bucket_count,
      timeout_ms: Math.max(rowPermutation.timeout_ms, bucketPermutation.minimum_timeout_ms)
    }))
  );

export function registerStorageBenchmarks(
  configOrFactory: storage.TestStorageConfig | storage.TestStorageFactory,
  options: RegisterStorageBenchmarkOptions
) {
  const config: storage.TestStorageConfig =
    typeof configOrFactory == 'function' ? { factory: configOrFactory, tableIdStrings: true } : configOrFactory;
  const storageVersion = options.storageVersion ?? config.storageVersion;
  const scenarios = options.scenarios ?? DEFAULT_STORAGE_BENCHMARK_SCENARIOS;

  createCoreAPIMetrics(METRICS_HELPER.metricsEngine);

  for (const scenario of scenarios) {
    test(
      `storage benchmark - ${scenario.name}`,
      async () => {
        const result = await runStorageBenchmark(
          config,
          {
            ...options,
            storageVersion
          },
          scenario
        );

        options.results?.push(result);
        console.log(formatStorageBenchmarkResultLine(result));
      },
      scenario.timeout_ms ?? options.timeout_ms ?? 1_200_000
    );
  }
}

export function registerStorageBenchmarkSummary(
  results: StorageBenchmarkResult[],
  title = 'Storage benchmark results'
) {
  afterAll(async () => {
    if (results.length == 0) {
      return;
    }

    console.log(`\n${title}\n${formatStorageBenchmarkResults(results)}`);

    const outputPath = process.env[STORAGE_BENCHMARK_OUTPUT_PATH_ENV];
    if (outputPath != null && outputPath.length > 0) {
      await writeStorageBenchmarkResults(outputPath, results);
      console.log(`\nWrote storage benchmark results to ${outputPath}`);
    }
  });
}

export async function writeStorageBenchmarkResults(outputPath: string, results: StorageBenchmarkResult[]) {
  const output: StorageBenchmarkOutput = {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    results
  };
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${JSON.stringify(output, null, 2)}\n`, 'utf8');
}

export function formatStorageBenchmarkResults(results: StorageBenchmarkResult[]) {
  const rows = [
    '| Storage | Version | Scenario | Source Rows | Buckets | Write ms | Write rows/s | Write MiB/s | Sync drain ms | Ops | Read MiB/s |',
    '| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |'
  ];

  for (const result of results) {
    rows.push(
      `| ${result.storage} | ${result.version ?? ''} | ${result.scenario} | ${result.source_rows} | ${
        result.buckets
      } | ${formatNumber(result.write_ms)} | ${formatNumber(result.write_rows_per_second)} | ${formatNumber(
        result.write_mebibytes_per_second
      )} | ${formatNumber(result.sync_drain_ms)} | ${result.ops} | ${formatNumber(result.read_mebibytes_per_second)} |`
    );
  }

  return rows.join('\n');
}

async function runStorageBenchmark(
  config: storage.TestStorageConfig,
  options: Required<Pick<RegisterStorageBenchmarkOptions, 'storageName'>> &
    Omit<RegisterStorageBenchmarkOptions, 'storageName'>,
  scenario: StorageBenchmarkScenario
): Promise<StorageBenchmarkResult> {
  validateScenario(scenario);

  await using factory = await config.factory();
  const syncRules = await deploySyncRules(
    factory,
    updateSyncRulesFromYaml(BENCHMARK_SYNC_RULES, {
      storageVersion: options.storageVersion
    })
  );

  const bucketStorage = factory.getInstance(syncRules.stream);
  const label = benchmarkLabel(options.storageName, options.storageVersion, scenario.name);
  const sourceRows = scenario.todo_row_count + scenario.list_row_count;
  const sourceBytes = calculateSourceBytes(scenario);

  const writeStarted = performance.now();
  await persistBenchmarkRows({
    config,
    bucketStorage,
    scenario,
    label,
    progressIntervalMs: options.progress_interval_ms ?? 5_000
  });
  const writeMs = performance.now() - writeStarted;

  const drain = await drainSyncStream({
    bucketStorage,
    label,
    scenario,
    progressIntervalMs: options.progress_interval_ms ?? 5_000
  });

  return {
    storage: options.storageName,
    version: options.storageVersion ?? null,
    scenario: scenario.name,
    source_rows: sourceRows,
    buckets: drain.buckets,
    write_ms: writeMs,
    write_rows_per_second: sourceRows / (writeMs / 1_000),
    write_mebibytes_per_second: sourceBytes / 1024 / 1024 / (writeMs / 1_000),
    sync_drain_ms: drain.elapsedMs,
    ops: drain.ops,
    read_mebibytes_per_second: drain.bytes / 1024 / 1024 / (drain.elapsedMs / 1_000)
  };
}

async function persistBenchmarkRows(options: {
  config: storage.TestStorageConfig;
  bucketStorage: storage.SyncRulesBucketStorage;
  scenario: StorageBenchmarkScenario;
  label: string;
  progressIntervalMs: number;
}) {
  const { config, bucketStorage, scenario, label } = options;
  const sourceRows = scenario.todo_row_count + scenario.list_row_count;
  const started = performance.now();
  let lastProgress = started;
  let savedRows = 0;
  let pendingRows = 0;

  await using writer = await bucketStorage.createWriter(BATCH_OPTIONS);
  const listsTable = await resolveTestTable(writer, 'lists', ['id'], config, 1);
  const todosTable = await resolveTestTable(writer, 'todos', ['id'], config, 2);

  await writer.markAllSnapshotDone('0/1');

  async function saveRow(row: storage.SaveOptions) {
    await writer.save(row);
    savedRows++;
    pendingRows++;

    if (pendingRows >= scenario.flush_every) {
      await writer.flush();
      pendingRows = 0;
      maybeLogProgress();
    }
  }

  function maybeLogProgress(force = false) {
    const now = performance.now();
    if (force || now - lastProgress >= options.progressIntervalMs) {
      lastProgress = now;
      console.log(
        `[${label}] write progress source_rows=${savedRows}/${sourceRows} elapsed_ms=${formatNumber(now - started)}`
      );
    }
  }

  for (let i = 0; i < scenario.list_row_count; i++) {
    const listId = listRowId(i);
    await saveRow({
      sourceTable: listsTable,
      tag: storage.SaveOperationTag.INSERT,
      after: listRow(i, scenario),
      afterReplicaId: listId
    });
  }

  for (let i = 0; i < scenario.todo_row_count; i++) {
    const todoId = todoRowId(i);
    await saveRow({
      sourceTable: todosTable,
      tag: storage.SaveOperationTag.INSERT,
      after: todoRow(i, scenario),
      afterReplicaId: todoId
    });
  }

  maybeLogProgress(true);
  await writer.commit('1/1');
}

async function drainSyncStream(options: {
  bucketStorage: storage.SyncRulesBucketStorage;
  label: string;
  scenario: StorageBenchmarkScenario;
  progressIntervalMs: number;
}) {
  const { bucketStorage, label, scenario, progressIntervalMs } = options;
  const started = performance.now();
  const tracker = new sync.RequestTracker(METRICS_HELPER.metricsEngine);
  const controller = new AbortController();
  const maxBucketCount = configuredMaxBucketCount(scenario);
  const syncContext = new sync.SyncContext({
    maxBuckets: maxBucketCount + 10,
    maxParameterQueryResults: maxBucketCount + 10,
    maxDataFetchConcurrency: 2
  });

  let lines = 0;
  let ops = 0;
  let bytes = 0;
  let buckets = 0;
  let completed = false;

  const heartbeat = setInterval(() => {
    console.log(
      `[${label}] sync stream heartbeat lines=${lines} ops=${ops} bytes=${bytes} elapsed_ms=${formatNumber(
        performance.now() - started
      )}`
    );
  }, progressIntervalMs);
  heartbeat.unref?.();

  try {
    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      token: new JwtPayload({ sub: 'user_00000000', exp: Date.now() / 1_000 + 3_600 }),
      isEncodingAsBson: false,
      signal: controller.signal
    });

    for await (const rawLine of stream) {
      if (rawLine == null) {
        continue;
      }

      lines++;
      const line = parseSyncLine(rawLine);
      bytes += Buffer.byteLength(typeof rawLine == 'string' ? rawLine : JSONBig.stringify(rawLine));

      if (line != null && 'checkpoint' in line) {
        buckets = line.checkpoint.buckets.length;
      } else if (line != null && 'checkpoint_diff' in line) {
        buckets = Math.max(buckets, line.checkpoint_diff.updated_buckets.length);
      } else if (line != null && 'data' in line) {
        ops += line.data.data.length;
      } else if (line != null && 'checkpoint_complete' in line) {
        completed = true;
        break;
      }
    }
  } finally {
    controller.abort();
    clearInterval(heartbeat);
  }

  if (!completed) {
    throw new Error(`[${label}] sync stream ended before checkpoint_complete`);
  }

  return {
    buckets,
    ops,
    bytes,
    elapsedMs: performance.now() - started
  };
}

function parseSyncLine(line: string | object) {
  if (typeof line != 'string') {
    return line as any;
  }

  try {
    return JSONBig.parse(line) as any;
  } catch {
    return null;
  }
}

function validateScenario(scenario: StorageBenchmarkScenario) {
  if (scenario.todo_row_count < 1) {
    throw new Error(`Benchmark scenario ${scenario.name} must have at least one todo row`);
  }
  if (scenario.list_row_count < 1) {
    throw new Error(`Benchmark scenario ${scenario.name} must have at least one list row`);
  }
  if (scenario.user_count < 1) {
    throw new Error(`Benchmark scenario ${scenario.name} must have at least one user`);
  }
  if (scenario.flush_every < 1) {
    throw new Error(`Benchmark scenario ${scenario.name} must flush at least every one row`);
  }

  const maxBucketCount = configuredMaxBucketCount(scenario);
  const buckets = expectedBucketCount(scenario);
  if (buckets > maxBucketCount) {
    throw new Error(
      `Benchmark scenario ${scenario.name} creates ${buckets} buckets, exceeding max_bucket_count=${maxBucketCount}`
    );
  }
}

function expectedBucketCount(scenario: StorageBenchmarkScenario) {
  return scenario.list_row_count + Math.min(scenario.list_row_count, scenario.user_count);
}

function configuredMaxBucketCount(scenario: StorageBenchmarkScenario) {
  return scenario.max_bucket_count ?? 1_000;
}

function calculateSourceBytes(scenario: StorageBenchmarkScenario) {
  let bytes = 0;
  for (let i = 0; i < scenario.list_row_count; i++) {
    bytes += Buffer.byteLength(JSONBig.stringify(listRow(i, scenario)));
  }
  for (let i = 0; i < scenario.todo_row_count; i++) {
    bytes += Buffer.byteLength(JSONBig.stringify(todoRow(i, scenario)));
  }
  return bytes;
}

function listRow(index: number, scenario: StorageBenchmarkScenario) {
  return {
    id: listRowId(index),
    owner_id: userId(index % scenario.user_count),
    name: `List ${index}`
  };
}

function todoRow(index: number, scenario: StorageBenchmarkScenario) {
  const listIndex = index % scenario.list_row_count;
  return {
    id: todoRowId(index),
    list_id: listRowId(listIndex),
    owner_id: userId(listIndex % scenario.user_count),
    description: `Todo ${index} for ${listRowId(listIndex)}`,
    completed: index % 3 == 0 ? 1 : 0
  };
}

function benchmarkLabel(storageName: string, storageVersion: number | undefined, scenarioName: string) {
  return `${storageName}/v${storageVersion ?? 'default'}/${scenarioName}`;
}

function listRowId(index: number) {
  return `list_${String(index).padStart(8, '0')}`;
}

function todoRowId(index: number) {
  return `todo_${String(index).padStart(8, '0')}`;
}

function userId(index: number) {
  return `user_${String(index).padStart(8, '0')}`;
}

function formatStorageBenchmarkResultLine(result: StorageBenchmarkResult) {
  return `[${result.storage}/v${result.version ?? 'default'}/${result.scenario}] result write_ms=${formatNumber(
    result.write_ms
  )} write_rows_per_second=${formatNumber(result.write_rows_per_second)} write_mebibytes_per_second=${formatNumber(
    result.write_mebibytes_per_second
  )} sync_drain_ms=${formatNumber(result.sync_drain_ms)} ops=${result.ops} buckets=${
    result.buckets
  } read_mebibytes_per_second=${formatNumber(result.read_mebibytes_per_second)}`;
}

function formatNumber(value: number) {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value.toFixed(value >= 100 ? 0 : 2);
}
