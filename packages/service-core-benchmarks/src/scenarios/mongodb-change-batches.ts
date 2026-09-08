import { STORAGE_VERSION_4 } from '@powersync/service-core';
import { serialize, Timestamp } from 'bson';
import { BenchmarkScenario } from '../types/BenchmarkScenario.js';
import { createDocument, MutationMode } from './mongodb-throughput-workload.js';

export interface ChangeBatchScenario
  extends BenchmarkScenario<{
    row_count: number;
    snapshot_row_count: number;
    batch_size: number;
    payload_bytes: number;
    shape: string;
    mutations: MutationMode;
  }> {
  storage: { implementation: 'mongodb-storage'; version: typeof STORAGE_VERSION_4 };
  s3: boolean;
}

function integer(name: string, fallback: number, minimum = 1): number {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`${name} must be an integer >= ${minimum}`);
  return value;
}

export function operationAt(mode: MutationMode, index: number): Exclude<MutationMode, 'mixed'> {
  return mode === 'mixed' ? (['insert', 'update', 'delete'] as const)[index % 3] : mode;
}

export function createChangeBatchScenario(): ChangeBatchScenario {
  const rows = integer('BENCHMARK_ROWS', 10_000);
  const users = integer('BENCHMARK_USERS', 100);
  if (users > rows) throw new Error('BENCHMARK_USERS must not exceed BENCHMARK_ROWS');
  const mode = process.env.BENCHMARK_MUTATIONS ?? 'insert';
  if (!['insert', 'update', 'delete', 'mixed'].includes(mode)) throw new Error('Invalid BENCHMARK_MUTATIONS');
  const shape = process.env.BENCHMARK_SHAPE ?? 'sample';
  if (!['sample', 'synthetic'].includes(shape)) throw new Error('BENCHMARK_SHAPE must be sample or synthetic');
  if (!['true', 'false'].includes(process.env.BENCHMARK_S3 ?? 'true')) throw new Error('Invalid BENCHMARK_S3');
  const s3 = process.env.BENCHMARK_S3 !== 'false';
  const seeded = mode === 'insert' ? 0 : mode === 'mixed' ? rows - Math.ceil(rows / 3) : rows;
  return {
    id: `replication.change-batches.mongodb-v4.${s3 ? 's3' : 'inline'}.${mode}`,
    description: `Raw BSON to MongoDB v4 ${s3 ? '+ S3' : 'inline'}; ${mode}, ${shape}, ${users} users; ${process.env.BENCHMARK_LABEL ?? 'baseline'}`,
    layer: 'replication',
    profile: 'manual',
    tags: ['replication', 'mongodb-storage', 'storage-v4'],
    prerequisites: s3 ? ['mongodb-storage', 's3'] : ['mongodb-storage'],
    timeout_ms: integer('BENCHMARK_TIMEOUT_MS', 300_000),
    warmup_iterations: integer('BENCHMARK_WARMUPS', 0, 0),
    measured_iterations: integer('BENCHMARK_ITERATIONS', 1),
    storage: { implementation: 'mongodb-storage', version: STORAGE_VERSION_4 },
    s3,
    workload: {
      row_count: rows,
      snapshot_row_count: seeded,
      batch_size: integer('BENCHMARK_BATCH_SIZE', 6000),
      payload_bytes: integer('BENCHMARK_PAYLOAD_BYTES', 1024, 0),
      shape,
      mutations: mode as MutationMode
    },
    syncRule: () => `bucket_definitions:
  by_user:
    accept_potentially_dangerous_queries: true
    parameters: SELECT value AS user_id FROM json_each(request.parameter('users'))
    data:
      - SELECT * FROM public.benchmark_items WHERE benchmark_user = bucket.user_id
`,
    sync_parameters: { users: Array.from({ length: users }, (_, i) => `user-${i}`) },
    expected_bucket_count: users,
    expected_bucket_operation_count: seeded + rows
  };
}

/** Projected change-stream event BSON, before parseChangeDocument deserializes its envelope. */
export function rawEvent(scenario: ChangeBatchScenario, index: number, prefill = false): Buffer {
  const operationType = prefill ? 'insert' : operationAt(scenario.workload.mutations, index);
  const document = createDocument(index, prefill ? 0 : 1, scenario.workload.payload_bytes);
  return Buffer.from(
    serialize({
      _id: { _data: (index + 1).toString(16).padStart(48, '0') },
      operationType,
      clusterTime: new Timestamp({ t: 1, i: index + 1 }),
      ns: { db: 'public', coll: 'benchmark_items' },
      documentKey: { _id: document.id },
      ...(operationType === 'delete' ? {} : { fullDocument: { ...document, _id: document.id } })
    })
  );
}

/** Mirror the source cursor's limits, allowing a final partial batch. Generation is untimed. */
export function generateChangeBatches(scenario: ChangeBatchScenario): Buffer[][] {
  const batches: Buffer[][] = [];
  let batch: Buffer[] = [];
  let bytes = 0;
  for (let i = 0; i < scenario.workload.row_count; i++) {
    const event = rawEvent(scenario, i);
    if (batch.length && (batch.length >= scenario.workload.batch_size || bytes + event.length > 64 * 1024 * 1024)) {
      batches.push(batch);
      batch = [];
      bytes = 0;
    }
    batch.push(event);
    bytes += event.length;
  }
  if (batch.length) batches.push(batch);
  return batches;
}
