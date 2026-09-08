import { ControlledReplicationBenchmarkImplementation } from '../implementations/replication/ControlledReplicationBenchmarkImplementation.js';
import { assertDistinctMongoSourceAndStorage } from '../implementations/replication/mongodb/MongoSourceBenchmarkConfiguration.js';
import type { ReplicationBenchmarkPhase } from '../types/ReplicationBenchmark.js';
import { qualifiedTable } from '../utils/replication-sync-rules.js';
import { createMongoThroughputManifest, type MutationMode } from './mongodb-throughput-workload.js';
import {
  createMongoSourceQuickReplicationScenario,
  mongoReplicationSource,
  mongoReplicationStorage
} from './replication-scenarios.js';

function integer(name: string, fallback: number, minimum = 1): number {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < minimum) throw new Error(`${name} must be an integer >= ${minimum}`);
  return value;
}

export function mongoThroughputCase(phase: ReplicationBenchmarkPhase) {
  const scenario = createMongoSourceQuickReplicationScenario(phase, 'mongodb-storage', 3);
  if (!['sample', 'synthetic'].includes(process.env.BENCHMARK_SHAPE ?? 'sample'))
    throw new Error('BENCHMARK_SHAPE must be sample or synthetic');
  const mode = process.env.BENCHMARK_MUTATIONS ?? 'mixed';
  if (!['insert', 'update', 'delete', 'mixed'].includes(mode)) throw new Error('Invalid BENCHMARK_MUTATIONS');
  const s3 = process.env.BENCHMARK_S3 !== 'false';
  const rows = integer('BENCHMARK_ROWS', 100_000);
  const users = integer('BENCHMARK_USERS', 100);
  if (users > rows) throw new Error('BENCHMARK_USERS must not exceed BENCHMARK_ROWS');
  const mutations = integer('BENCHMARK_MUTATION_COUNT', rows);
  const batchSize = integer('BENCHMARK_BATCH_SIZE', 1000);
  if (mutations % batchSize !== 0)
    throw new Error('BENCHMARK_MUTATION_COUNT must be divisible by BENCHMARK_BATCH_SIZE');
  Object.assign(scenario, {
    id: `replication.${phase}.mongodb-v3.${s3 ? 's3' : 'inline'}.${mode}.throughput`,
    description: `MongoDB v3 ${phase}, ${mode}, ${s3 ? 'S3' : 'inline'}; ${process.env.BENCHMARK_SHAPE ?? 'sample'}, ${users} users; ${process.env.BENCHMARK_LABEL ?? 'baseline'}`,
    profile: 'manual',
    tags: ['replication', 'throughput', phase, 'mongodb-source', 'mongodb-storage', 'storage-v3'],
    timeout_ms: integer('BENCHMARK_TIMEOUT_MS', 3_600_000),
    warmup_iterations: integer('BENCHMARK_WARMUPS', 1, 0),
    measured_iterations: integer('BENCHMARK_ITERATIONS', 3),
    workload: {
      snapshot_row_count: rows,
      streaming_mutation_count: mutations,
      transaction_count: mutations / batchSize,
      payload_bytes: integer('BENCHMARK_PAYLOAD_BYTES', 1024, 0)
    },
    expected_bucket_operation_count: phase === 'snapshot' ? rows : rows + mutations
  });
  scenario.syncRule = (source) => `bucket_definitions:
  by_user:
    accept_potentially_dangerous_queries: true
    parameters: SELECT value AS user_id FROM json_each(request.parameter('users'))
    data:
      - SELECT * FROM ${qualifiedTable(source)} WHERE benchmark_user = bucket.user_id
`;
  scenario.sync_parameters = { users: Array.from({ length: users }, (_, index) => `user-${index}`) };
  scenario.expected_bucket_count = users;
  const environment = {
    ...process.env,
    BENCHMARK_MONGODB_SOURCE_URL:
      process.env.BENCHMARK_MONGODB_SOURCE_URL ?? 'mongodb://127.0.0.1:27117/?directConnection=true',
    BENCHMARK_MONGODB_STORAGE_URL:
      process.env.BENCHMARK_MONGODB_STORAGE_URL ?? 'mongodb://127.0.0.1:27118/?directConnection=true',
    BENCHMARK_MONGODB_WRITER_URL:
      process.env.BENCHMARK_MONGODB_WRITER_URL ??
      process.env.BENCHMARK_MONGODB_SOURCE_URL ??
      'mongodb://127.0.0.1:27217/?directConnection=true',
    BENCHMARK_S3_ENDPOINT: process.env.BENCHMARK_S3_ENDPOINT ?? 'http://127.0.0.1:19000'
  };
  scenario.createManifest = (resolved) => createMongoThroughputManifest(resolved, mode as MutationMode);
  return {
    scenario,
    implementation: new ControlledReplicationBenchmarkImplementation({
      source: mongoReplicationSource(),
      storage: mongoReplicationStorage(3, s3),
      validateEnvironment: assertDistinctMongoSourceAndStorage,
      environment
    })
  };
}
