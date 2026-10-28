import { storage } from '@powersync/service-core';
import { createHash } from 'node:crypto';
import { maxFixedRowBytes } from '../generators/benchmark-data.js';
import type { BenchmarkDataDefinition } from '../types/BenchmarkDataDefinition.js';
import type { BenchmarkScenario } from '../types/BenchmarkScenario.js';
import type { BenchmarkSyncDefinition, BenchmarkSyncEngine } from '../types/BenchmarkSyncDefinition.js';
import { benchmarkClientParameters, expectedMembership } from '../utils/benchmark-sync.js';

/** Matches the service's parameter result limit; never truncate requested buckets. */
export const MAX_BENCHMARK_BUCKETS = 1_000;

export function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive safe integer`);
}

export function singleTableData(options: {
  rowCount: number;
  rowBytes: number;
  streamingRowCount?: number;
  batchSize?: number;
  transactionCount?: number;
}): BenchmarkDataDefinition {
  const data: BenchmarkDataDefinition = {
    tables: [
      {
        key: 'items',
        name: 'benchmark_items',
        snapshot_row_count: options.rowCount,
        streaming_row_count: options.streamingRowCount ?? options.rowCount,
        target_source_json_bytes: options.rowBytes,
        distribution: { owner_count: 100 },
        marker: 'last'
      }
    ],
    batch_size: options.batchSize ?? 1_000,
    transaction_count: options.transactionCount ?? Math.min(10, options.streamingRowCount ?? options.rowCount)
  };
  validateDefinitions(data, categorySync({ engine: 'sync-streams', bucketCount: 1 }));
  return data;
}

export function categorySync(options: { engine: BenchmarkSyncEngine; bucketCount: number }): BenchmarkSyncDefinition {
  positiveInteger(options.bucketCount, 'bucketCount');
  if (options.bucketCount > MAX_BENCHMARK_BUCKETS)
    throw new RangeError(`bucketCount exceeds service limit ${MAX_BENCHMARK_BUCKETS}`);
  return { engine: options.engine, streams: [{ name: 'items', tables: ['items'], bucket_count: options.bucketCount }] };
}

export function multiTableData(options: { rowCount: number; rowBytes: number }): {
  data: BenchmarkDataDefinition;
  sync: BenchmarkSyncDefinition;
} {
  const single = singleTableData(options);
  const data = {
    ...single,
    tables: [...single.tables, { ...single.tables[0], key: 'notes', name: 'benchmark_notes' }]
  };
  const sync: BenchmarkSyncDefinition = {
    engine: 'sync-streams',
    streams: [
      { name: 'all', tables: ['items', 'notes'], bucket_count: 1 },
      { name: 'items_only', tables: ['items'], bucket_count: 1 }
    ]
  };
  return { data, sync };
}

export function validateDefinitions(data: BenchmarkDataDefinition, sync: BenchmarkSyncDefinition): void {
  positiveInteger(data.batch_size, 'batch_size');
  positiveInteger(data.transaction_count, 'transaction_count');
  if (!data.tables.length) throw new Error('At least one data table is required');
  const keys = new Set<string>();
  const names = new Set<string>();
  const identifier = /^[a-z][a-z0-9_]*$/;
  for (const table of data.tables) {
    if (!identifier.test(table.key) || !identifier.test(table.name)) throw new Error('Invalid table key or name');
    if (keys.has(table.key) || names.has(table.name)) throw new Error('Duplicate table key or name');
    keys.add(table.key);
    names.add(table.name);
    positiveInteger(table.snapshot_row_count, 'snapshot_row_count');
    positiveInteger(table.streaming_row_count, 'streaming_row_count');
    positiveInteger(table.distribution.owner_count, 'owner_count');
    positiveInteger(table.target_source_json_bytes, 'target_source_json_bytes');
    if (table.marker !== 'last') throw new Error('Unsupported marker location');
    const overhead = Math.max(maxFixedRowBytes(table, 'snapshot'), maxFixedRowBytes(table, 'streaming'));
    if (table.target_source_json_bytes < overhead)
      throw new RangeError(`target_source_json_bytes must cover fixed row bytes (${overhead})`);
    if (data.transaction_count > table.streaming_row_count)
      throw new Error('transaction_count exceeds streaming rows per table');
  }
  if (!['sync-streams', 'sync-rules'].includes(sync.engine)) throw new Error('Unsupported sync engine');
  if (!sync.streams.length) throw new Error('At least one stream definition is required');
  const streamNames = new Set<string>();
  let buckets = 0;
  for (const stream of sync.streams) {
    if (!identifier.test(stream.name) || streamNames.has(stream.name))
      throw new Error('Invalid or duplicate stream name');
    streamNames.add(stream.name);
    positiveInteger(stream.bucket_count, 'bucket_count');
    buckets += stream.bucket_count;
    if (!stream.tables.length || new Set(stream.tables).size !== stream.tables.length)
      throw new Error('Invalid stream table references');
    for (const key of stream.tables) if (!keys.has(key)) throw new Error(`Missing table reference ${key}`);
  }
  if (buckets > MAX_BENCHMARK_BUCKETS)
    throw new RangeError(`Requested buckets exceed service limit ${MAX_BENCHMARK_BUCKETS}`);
  for (const key of keys)
    if (!sync.streams.some((s) => s.tables.includes(key))) throw new Error(`Marker table ${key} is not selected`);
  for (const phase of ['snapshot_row_count', 'streaming_row_count'] as const) {
    const total = data.tables.reduce(
      (count, table) => count + table[phase] * sync.streams.filter((s) => s.tables.includes(table.key)).length,
      0
    );
    if (
      !Number.isSafeInteger(total) ||
      !Number.isSafeInteger(data.tables.reduce((bytes, t) => bytes + t[phase] * t.target_source_json_bytes, 0))
    )
      throw new RangeError('Aggregate counts and bytes must be safe integers');
  }
}

export function scenarioIdentity(prefix: string, data: BenchmarkDataDefinition, sync: BenchmarkSyncDefinition): string {
  const fingerprint = createHash('sha256')
    .update(
      JSON.stringify({ data, sync }, (_key, value) => {
        if (value != null && typeof value === 'object' && !Array.isArray(value))
          return Object.fromEntries(
            Object.keys(value)
              .sort()
              .map((key) => [key, value[key]])
          );
        return value;
      })
    )
    .digest('hex')
    .slice(0, 12);
  const rows = data.tables.reduce((sum, t) => sum + t.snapshot_row_count, 0);
  const buckets = sync.streams.reduce((sum, s) => sum + s.bucket_count, 0);
  return `${prefix}.${sync.engine}.${data.tables.length === 1 ? 'single-table' : 'multi-table'}.rows-${rows}.buckets-${buckets}.${fingerprint}`;
}

export function validateScenarioDimensions(
  scenario: Pick<BenchmarkScenario, 'data' | 'sync' | 'storage' | 'layer'>
): void {
  validateDefinitions(scenario.data, scenario.sync);
  const { implementation, version } = scenario.storage;
  positiveInteger(version, 'storage_version');
  if (!['storage', 'replication', 'api', 'combined'].includes(scenario.layer))
    throw new Error('Unsupported benchmark layer');
  if (
    !['storage:postgres', 'storage:mongodb'].includes(implementation) ||
    storage.STORAGE_VERSION_CONFIG[version] == null ||
    (implementation === 'storage:postgres' && version >= storage.STORAGE_VERSION_4)
  ) {
    throw new Error(`Unsupported storage version ${version} for ${implementation}`);
  }
  if (
    scenario.layer === 'replication' &&
    (!('phase' in scenario) || !['snapshot', 'streaming'].includes(String(scenario.phase)))
  ) {
    throw new Error('Unsupported replication benchmark phase');
  }
  if (
    scenario.layer === 'storage' &&
    (!('mode' in scenario) || !['write', 'read', 'write-read'].includes(String(scenario.mode)))
  ) {
    throw new Error('Unsupported storage benchmark mode');
  }
  if (scenario.layer === 'api' || scenario.layer === 'combined') {
    const settings = scenario as unknown as {
      mode: string;
      transport: { encoding: string; compression: string };
      clients: { count: number };
    };
    if (
      !['initial', 'streaming'].includes(settings.mode) ||
      settings.transport?.encoding !== 'ndjson' ||
      settings.transport.compression !== 'none' ||
      settings.clients?.count !== 1
    ) {
      throw new Error('Unsupported client benchmark configuration');
    }
  }
  if (
    (scenario.layer === 'replication' || scenario.layer === 'combined') &&
    (!('producer' in scenario) || !['source:postgres', 'source:mongodb'].includes(String(scenario.producer)))
  ) {
    throw new Error('Unsupported benchmark source');
  }
  if (
    (scenario.layer === 'replication' && 'phase' in scenario && scenario.phase === 'streaming') ||
    (scenario.layer === 'combined' && 'mode' in scenario && scenario.mode === 'streaming')
  ) {
    positiveInteger(
      expectedMembership(scenario.data, scenario.sync, 'snapshot').operations +
        expectedMembership(scenario.data, scenario.sync, 'streaming').operations,
      'Aggregate streaming evidence operations'
    );
  }
}

/** Adapt the existing execution settings to concrete, serializable dimensions. */
export function defineScenario<T extends BenchmarkScenario>(
  input: Omit<T, 'id' | 'sync_parameters' | 'expected_bucket_count' | 'expected_bucket_operation_count'>,
  data: BenchmarkDataDefinition = input.data,
  sync: BenchmarkSyncDefinition = input.sync
): T {
  const scenario = input;
  validateScenarioDimensions({ ...input, data, sync });
  const streaming =
    ('phase' in input && input.phase === 'streaming') || ('mode' in input && input.mode === 'streaming');
  const snapshotCount = expectedMembership(data, sync, 'snapshot').operations;
  const streamingCount = expectedMembership(data, sync, 'streaming').operations;
  const dimensionPrefixes = [
    'layer:',
    'profile:',
    'phase:',
    'source:',
    'storage:',
    'version:',
    'engine:',
    'layout:',
    'shape:'
  ];
  const producer = 'producer' in input ? String(input.producer) : undefined;
  const phase =
    'phase' in input
      ? String(input.phase)
      : input.layer === 'api' || input.layer === 'combined'
        ? streaming
          ? 'streaming'
          : 'snapshot'
        : undefined;
  const tags = [
    ...input.tags.filter((tag) => !dimensionPrefixes.some((prefix) => tag.startsWith(prefix))),
    `layer:${input.layer}`,
    `profile:${input.profile}`,
    input.storage.implementation,
    `version:${input.storage.version}`,
    ...(producer ? [producer] : []),
    ...(phase ? [`phase:${phase}`] : []),
    ...(sync.streams.length === 1 && sync.streams[0].bucket_count === 10 ? ['shape:10-buckets'] : []),
    ...(sync.streams.length === 1 && sync.streams[0].bucket_count === 100 ? ['shape:100-buckets'] : []),
    ...(sync.streams.length === 1 && sync.streams[0].bucket_count === 1 ? ['shape:single-bucket'] : []),
    `engine:${sync.engine}`,
    `layout:${data.tables.length === 1 ? 'single-table' : 'multi-table'}`
  ];
  return {
    ...scenario,
    data,
    sync,
    id: scenarioIdentity(
      `${input.layer}.${'phase' in input ? input.phase : 'mode' in input ? input.mode : 'write'}.${producer ?? 'direct'}.${input.storage.implementation}.v${input.storage.version}.${input.profile}`,
      data,
      sync
    ),
    sync_parameters: benchmarkClientParameters(sync),
    description:
      input.description.replace(/ \[(?:Sync Streams|legacy Sync Rules)\]$/, '') +
      ` [${sync.engine === 'sync-streams' ? 'Sync Streams' : 'legacy Sync Rules'}]`,
    tags: [...new Set(tags)],
    prerequisites: [
      ...new Set([
        ...input.prerequisites.filter((tag) => !tag.startsWith('source:') && !tag.startsWith('storage:')),
        ...(producer ? [producer] : []),
        input.storage.implementation
      ])
    ],
    expected_bucket_count: sync.streams.reduce((sum, s) => sum + s.bucket_count, 0),
    expected_bucket_operation_count: streaming
      ? streamingCount + (input.layer === 'replication' ? snapshotCount : 0)
      : snapshotCount
  } as unknown as T;
}
