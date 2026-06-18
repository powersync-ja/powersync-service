import { createCoreAPIMetrics, JwtPayload, storage, sync, updateSyncRulesFromYaml } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
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
  sync_drain_ms: number;
  ops: number;
  mebibytes_per_second: number;
}

export const DEFAULT_STORAGE_BENCHMARK_SCENARIOS: StorageBenchmarkScenario[] = [
  {
    name: 'small',
    todo_row_count: 1_000,
    list_row_count: 100,
    user_count: 100,
    flush_every: 1_000,
    max_bucket_count: 1_000,
    timeout_ms: 120_000
  },
  {
    name: 'medium',
    todo_row_count: 10_000,
    list_row_count: 500,
    user_count: 500,
    flush_every: 1_000,
    max_bucket_count: 1_000,
    timeout_ms: 300_000
  },
  {
    name: 'large',
    todo_row_count: 100_000,
    list_row_count: 500,
    user_count: 500,
    flush_every: 1_000,
    max_bucket_count: 1_000,
    timeout_ms: 1_200_000
  }
];

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
  afterAll(() => {
    if (results.length == 0) {
      return;
    }

    console.log(`\n${title}\n${formatStorageBenchmarkResults(results)}`);
  });
}

export function formatStorageBenchmarkResults(results: StorageBenchmarkResult[]) {
  const rows = [
    '| Storage | Version | Scenario | Source Rows | Buckets | Write ms | Write rows/s | Sync drain ms | Ops | MiB/s |',
    '| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |'
  ];

  for (const result of results) {
    rows.push(
      `| ${result.storage} | ${result.version ?? ''} | ${result.scenario} | ${result.source_rows} | ${
        result.buckets
      } | ${formatNumber(result.write_ms)} | ${formatNumber(result.write_rows_per_second)} | ${formatNumber(
        result.sync_drain_ms
      )} | ${result.ops} | ${formatNumber(result.mebibytes_per_second)} |`
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
    sync_drain_ms: drain.elapsedMs,
    ops: drain.ops,
    mebibytes_per_second: drain.bytes / 1024 / 1024 / (drain.elapsedMs / 1_000)
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
      after: {
        id: listId,
        owner_id: userId(i % scenario.user_count),
        name: `List ${i}`
      },
      afterReplicaId: listId
    });
  }

  for (let i = 0; i < scenario.todo_row_count; i++) {
    const listIndex = i % scenario.list_row_count;
    const todoId = todoRowId(i);
    await saveRow({
      sourceTable: todosTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: todoId,
        list_id: listRowId(listIndex),
        owner_id: userId(listIndex % scenario.user_count),
        description: `Todo ${i} for ${listRowId(listIndex)}`,
        completed: i % 3 == 0 ? 1 : 0
      },
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
  const maxBucketCount = expectedBucketCount(scenario);
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

  const maxBucketCount = scenario.max_bucket_count ?? 1_000;
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
  )} write_rows_per_second=${formatNumber(result.write_rows_per_second)} sync_drain_ms=${formatNumber(
    result.sync_drain_ms
  )} ops=${result.ops} buckets=${result.buckets} mebibytes_per_second=${formatNumber(result.mebibytes_per_second)}`;
}

function formatNumber(value: number) {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value.toFixed(value >= 100 ? 0 : 2);
}
