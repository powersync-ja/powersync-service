import { container } from '@powersync/lib-services-framework';
import {
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  MetricsEngine,
  replication,
  storage,
  system,
  updateSyncRulesFromYaml,
  utils
} from '@powersync/service-core';
import { METRICS_HELPER, StorageDataHelpers } from '@powersync/service-core-tests';
import { MongoModule } from '@powersync/service-module-mongodb';
import { SyntheticReplicationModule } from '../implementations/replication/synthetic/SyntheticReplicationModule.js';
import { SyntheticReplicationSource } from '../implementations/replication/synthetic/SyntheticReplicationSource.js';
import { SyntheticReplicator } from '../implementations/replication/synthetic/SyntheticReplicator.js';
import { MongoStorageBenchmarkImplementation } from '../implementations/storage/MongoStorageBenchmarkImplementation.js';
import { PostgresStorageBenchmarkImplementation } from '../implementations/storage/PostgresStorageBenchmarkImplementation.js';
import { ReplicationBenchmarkManifest, ReplicationBenchmarkObservation } from '../types/ReplicationBenchmark.js';
import { StorageBenchmarkRunResource } from '../types/StorageBenchmark.js';
import {
  REPLICATION_CHILD_PROTOCOL_VERSION,
  ReplicationChildCommand,
  ReplicationChildEvent,
  serializeError
} from './replication-child-protocol.js';

type StorageOptions =
  | { readonly implementation: 'postgres-storage'; readonly url: string }
  | { readonly implementation: 'mongodb-storage'; readonly url: string; readonly isCI: boolean };

interface ChildState {
  resource?: StorageBenchmarkRunResource;
  context?: system.ServiceContextContainer;
  source?: SyntheticReplicationSource;
  replicationStream?: storage.PersistedReplicationStream;
  bucketStorage?: storage.SyncRulesBucketStorage;
  syncRulesContent?: storage.PersistedSyncConfigContent;
  lifecycleStarted: boolean;
  environment?: object;
}

const state: ChildState = { lifecycleStarted: false };
container.registerDefaults();

process.on('message', (message: unknown) => {
  void handle(message as ReplicationChildCommand);
});

async function handle(command: ReplicationChildCommand): Promise<void> {
  try {
    assertCommand(command);
    const payload = await execute(command);
    send({
      protocolVersion: REPLICATION_CHILD_PROTOCOL_VERSION,
      direction: 'event',
      kind: 'response',
      runId: command.runId,
      iterationId: command.iterationId,
      requestId: command.requestId,
      command: command.kind,
      payload
    });
    if (command.kind === 'shutdown' || command.kind === 'abort') {
      process.disconnect();
      setImmediate(() => process.exit(command.kind === 'shutdown' ? 0 : 1));
    }
  } catch (error) {
    send({
      protocolVersion: REPLICATION_CHILD_PROTOCOL_VERSION,
      direction: 'event',
      kind: 'fatal',
      runId: command.runId,
      iterationId: command.iterationId,
      requestId: command.requestId,
      error: serializeError(error)
    });
  }
}

async function execute(command: ReplicationChildCommand): Promise<unknown> {
  switch (command.kind) {
    case 'initialize':
      return await initialize(command.payload as { storage: StorageOptions });
    case 'setup_iteration':
      return await setupIteration(
        command.payload as {
          manifest: ReplicationBenchmarkManifest;
          syncRules: string;
          storageVersion: number;
          source?: ReplicationSourceOptions;
        }
      );
    case 'release_replication': {
      const context = required(state.context, 'service context');
      const releasedAtNs = state.source?.releaseReplication() ?? process.hrtime.bigint().toString();
      await context.lifeCycleEngine.start();
      state.lifecycleStarted = true;
      const waitForSnapshot = (command.payload as { waitForSnapshot?: boolean }).waitForSnapshot === true;
      return { releasedAtNs, snapshot: waitForSnapshot ? await waitForSnapshotCompletion() : undefined };
    }
    case 'commit_transaction':
      return required(state.source, 'source').commitTransaction(
        (command.payload as { transactionId: string }).transactionId
      );
    case 'keepalive': {
      const source = required(state.source, 'source');
      const target = source.keepalive();
      const visible = await source.waitForPosition(target.nativePosition!);
      return { target, visible };
    }
    case 'observe_checkpoint':
      return await observeCheckpoint(
        command.payload as {
          markerId: string;
          target: ReplicationBenchmarkObservation['target'];
          releasedAtNs?: string;
        }
      );
    case 'collect_evidence':
      return await collectEvidence();
    case 'cleanup_iteration':
      await cleanupIteration();
      return {};
    case 'monitor_start':
      return {
        pid: process.pid,
        cpu: process.cpuUsage(),
        memory: process.memoryUsage(),
        sampledAtNs: process.hrtime.bigint().toString()
      };
    case 'monitor_stop':
      return {
        pid: process.pid,
        cpu: process.cpuUsage(),
        memory: process.memoryUsage(),
        sampledAtNs: process.hrtime.bigint().toString()
      };
    case 'shutdown':
    case 'abort':
      await cleanupAll();
      return {};
  }
}

async function initialize(payload: { storage: StorageOptions }): Promise<object> {
  if (state.resource != null) throw new Error('Replication child is already initialized');
  const implementation =
    payload.storage.implementation === 'postgres-storage'
      ? new PostgresStorageBenchmarkImplementation({ url: payload.storage.url })
      : new MongoStorageBenchmarkImplementation({ url: payload.storage.url, isCI: payload.storage.isCI });
  state.resource = await implementation.open(new AbortController().signal);
  state.environment = state.resource.environment;
  createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  return { environment: state.environment, pid: process.pid };
}

async function setupIteration(payload: {
  manifest: ReplicationBenchmarkManifest;
  syncRules: string;
  storageVersion: number;
  source?: ReplicationSourceOptions;
}): Promise<object> {
  const resource = required(state.resource, 'storage resource');
  if (state.context != null) throw new Error('An iteration is already configured');
  const source = payload.source ?? { implementation: 'synthetic-source' as const };
  state.context = createServiceContext(resource, source);
  const context = state.context;
  state.source =
    source.implementation === 'synthetic-source' ? new SyntheticReplicationSource(payload.manifest) : undefined;
  state.replicationStream = await resource.factory.updateSyncRules(
    updateSyncRulesFromYaml(payload.syncRules, {
      validate: true,
      defaultSchema: source.implementation === 'mongodb-source' ? source.database : 'public',
      storageVersion: payload.storageVersion
    })
  );
  state.syncRulesContent = state.replicationStream.syncConfigContent[0];
  state.bucketStorage = resource.factory.getInstance(state.replicationStream);

  const engine = new replication.ReplicationEngine();
  context.register(replication.ReplicationEngine, engine);
  context.register(MetricsEngine, METRICS_HELPER.metricsEngine);
  const rateLimiter: replication.ErrorRateLimiter = {
    async waitUntilAllowed() {},
    reportError() {},
    mayPing: () => true
  };
  if (source.implementation === 'synthetic-source') {
    const replicator = new SyntheticReplicator({
      id: 'synthetic-benchmark',
      storageEngine: context.storageEngine,
      metricsEngine: METRICS_HELPER.metricsEngine,
      syncRuleProvider: { get: async () => undefined, exitOnError: true },
      rateLimiter,
      heartbeatIntervalSeconds: 0,
      source: () => required(state.source, 'source')
    });
    await new SyntheticReplicationModule(replicator).initialize(context);
  } else {
    await new MongoModule().initialize(context);
  }
  context.lifeCycleEngine.withLifecycle(engine, {
    start: (component) => component.start(),
    stop: (component) => component.shutDown()
  });
  return { replicationStreamName: state.replicationStream.replicationStreamName };
}

type ReplicationSourceOptions =
  | { readonly implementation: 'synthetic-source' }
  | { readonly implementation: 'mongodb-source'; readonly uri: string; readonly database: string };

function createServiceContext(
  resource: StorageBenchmarkRunResource,
  source: ReplicationSourceOptions
): system.ServiceContextContainer {
  const configuration = {
    storage: { type: 'benchmark' },
    api_parameters: {
      max_concurrent_connections: 1,
      max_data_fetch_concurrency: 1,
      max_buckets_per_connection: 1_000,
      max_parameter_query_results: 1_000,
      checkpoint_request_retention_minutes: 60,
      bucket_count_cache_ttl_minutes: 60
    },
    telemetry: { disable_telemetry_sharing: true, internal_service_endpoint: '' },
    sync_rules: { present: false, exit_on_error: true },
    api_tokens: [],
    jwt_audiences: [],
    token_max_expiration: '1h',
    metadata: {},
    port: 0,
    slot_name_prefix: 'benchmark_',
    healthcheck: { probes: { use_filesystem: false, use_http: false, use_legacy: false } },
    parameters: {},
    base_config: {},
    client_keystore: {},
    connections:
      source.implementation === 'mongodb-source'
        ? [
            {
              type: 'mongodb',
              uri: source.uri,
              database: source.database,
              post_images: 'off',
              heartbeat_interval_seconds: 5
            }
          ]
        : undefined
  } as unknown as utils.ResolvedPowerSyncConfig;
  const context = new system.ServiceContextContainer({
    serviceMode: system.ServiceContextMode.SYNC,
    configuration
  });
  context.storageEngine.registerProvider({
    type: 'benchmark',
    async getStorage() {
      return {
        storage: resource.factory,
        reportStorage: {} as never,
        async shutDown() {},
        async tearDown() {
          return false;
        }
      };
    }
  });
  return context;
}

async function waitForSnapshotCompletion(): Promise<{ position: string; visibleAtNs: string }> {
  if (state.source != null) return await state.source.waitForSnapshot();
  const bucketStorage = required(state.bucketStorage, 'bucket storage');
  while (true) {
    const [status, checkpoint] = await Promise.all([bucketStorage.getStatus(), bucketStorage.getCheckpoint()]);
    if (status.snapshotDone && checkpoint.lsn != null) {
      return { position: checkpoint.lsn, visibleAtNs: process.hrtime.bigint().toString() };
    }
    await new Promise((resolve) => setTimeout(resolve, 30));
  }
}

async function observeCheckpoint(payload: {
  markerId: string;
  target: ReplicationBenchmarkObservation['target'];
  releasedAtNs?: string;
}): Promise<ReplicationBenchmarkObservation> {
  const source = required(state.source, 'source');
  const visible = await source.waitForTarget(payload.markerId);
  const evidence = await collectEvidence();
  if (evidence.checkpoint == null) throw new Error('Synthetic target was visible before its source position');
  return {
    target: payload.target,
    checkpoint: evidence.checkpoint,
    checkpointVisibleAtNs: visible.visibleAtNs,
    replicationReleasedAtNs: payload.releasedAtNs,
    operations: evidence.operations,
    snapshotDone: evidence.snapshotDone,
    keepalives: 0,
    retries: 0,
    restarts: 0
  };
}

async function collectEvidence(): Promise<{
  checkpoint: string | null;
  operations: ReplicationBenchmarkObservation['operations'];
  snapshotDone: boolean;
}> {
  const bucketStorage = required(state.bucketStorage, 'bucket storage');
  const content = required(state.syncRulesContent, 'sync rules content');
  const checkpoint = await bucketStorage.getCheckpoint();
  const status = await bucketStorage.getStatus();
  const lsn = checkpoint.lsn;
  if (lsn == null) {
    return { checkpoint: null, operations: [], snapshotDone: status.snapshotDone };
  }
  const operations = await new StorageDataHelpers(bucketStorage, content).getBucketData('global[]', checkpoint);
  return {
    checkpoint: lsn,
    operations: operations.map((operation) => ({
      op: operation.op,
      object_id: operation.object_id,
      data: operation.data
    })),
    snapshotDone: status.snapshotDone
  };
}

async function cleanupIteration(): Promise<void> {
  const errors: unknown[] = [];
  state.source?.stop();
  if (state.lifecycleStarted && state.context != null) {
    try {
      await state.context.lifeCycleEngine.stop();
    } catch (error) {
      errors.push(error);
    }
    state.lifecycleStarted = false;
  }
  if (state.replicationStream != null && state.bucketStorage != null) {
    let lock: storage.ReplicationLock | undefined;
    try {
      lock = await state.replicationStream.lock();
      await state.bucketStorage.terminate({ clearStorage: true });
    } catch (error) {
      errors.push(error);
    } finally {
      try {
        await lock?.release();
      } catch (error) {
        errors.push(error);
      }
    }
  }
  state.context = undefined;
  state.source = undefined;
  state.replicationStream = undefined;
  state.bucketStorage = undefined;
  state.syncRulesContent = undefined;
  if (errors.length > 0) throw new AggregateError(errors, 'Replication child cleanup failed');
}

async function cleanupAll(): Promise<void> {
  const errors: unknown[] = [];
  try {
    await cleanupIteration();
  } catch (error) {
    errors.push(error);
  }
  if (state.resource != null) {
    try {
      await state.resource.dispose();
    } catch (error) {
      errors.push(error);
    }
  }
  state.resource = undefined;
  if (errors.length > 0) throw new AggregateError(errors, 'Replication child run cleanup failed');
}

function required<T>(value: T | undefined, name: string): T {
  if (value == null) throw new Error(`Replication child ${name} is not initialized`);
  return value;
}

function assertCommand(command: ReplicationChildCommand): void {
  if (command.protocolVersion !== REPLICATION_CHILD_PROTOCOL_VERSION || command.direction !== 'command') {
    throw new Error('Invalid replication child command envelope');
  }
}

function send(event: ReplicationChildEvent): void {
  if (process.send == null) throw new Error('Replication child requires an IPC channel');
  process.send(event);
}
