import {
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  MetricsEngine,
  replication,
  storage,
  system,
  updateSyncRulesFromYaml
} from '@powersync/service-core';
import { METRICS_HELPER, StorageDataHelpers } from '@powersync/service-core-tests';
import { StorageBenchmarkImplementation, StorageBenchmarkRunResource } from '../types/StorageBenchmark.js';
import { bucketRequests, resolveBenchmarkBuckets } from '../utils/benchmark-buckets.js';
import { createBenchmarkServiceContext } from './BenchmarkServiceContext.js';
import { constructReplicationChildImplementation } from './ReplicationChildClassLoader.js';
import { ReplicationChildSourceImplementation } from './ReplicationChildSourceImplementation.js';
import {
  ReplicationChildCommand,
  ReplicationChildEvidencePayload,
  ReplicationChildInitializePayload,
  ReplicationChildResponsePayloads,
  ReplicationChildSetupIterationPayload
} from './replication-child-protocol.js';

interface ReplicationChildRuntimeState {
  resource?: StorageBenchmarkRunResource;
  context?: system.ServiceContextContainer;
  replicationStream?: storage.PersistedReplicationStream;
  bucketStorage?: storage.SyncRulesBucketStorage;
  syncRulesContent?: storage.PersistedSyncConfigContent;
  lifecycleStarted: boolean;
  environment?: object;
  syncParameters?: Record<string, unknown>;
  defaultSchema?: string;
}

export class ReplicationChildRuntime {
  private readonly state: ReplicationChildRuntimeState = { lifecycleStarted: false };

  async execute(command: ReplicationChildCommand): Promise<unknown> {
    switch (command.kind) {
      case 'initialize':
        return await this.initialize(command.payload);
      case 'setup_iteration':
        return await this.setupIteration(command.payload);
      case 'release_replication': {
        const context = required(this.state.context, 'service context');
        const releasedAtNs = process.hrtime.bigint().toString();
        await context.lifeCycleEngine.start();
        this.state.lifecycleStarted = true;
        const waitForSnapshot = command.payload.waitForSnapshot === true;
        return { releasedAtNs, snapshot: waitForSnapshot ? await this.waitForSnapshotCompletion() : undefined };
      }
      case 'collect_evidence':
        return await this.collectEvidence();
      case 'cleanup_iteration':
        await this.cleanupIteration();
        return {};
      case 'monitor_start':
      case 'monitor_stop':
        return {
          pid: process.pid,
          cpu: process.cpuUsage(),
          memory: process.memoryUsage(),
          sampledAtNs: process.hrtime.bigint().toString()
        };
      case 'shutdown':
      case 'abort':
        await this.cleanupAll();
        return {};
      default:
        return assertNever(command);
    }
  }

  private async initialize(
    payload: ReplicationChildInitializePayload
  ): Promise<ReplicationChildResponsePayloads['initialize']> {
    if (this.state.resource != null) throw new Error('Replication child is already initialized');
    const implementation = await constructReplicationChildImplementation<unknown>(payload.storage);
    assertStorageImplementation(implementation, payload.storage.exportName);
    const resource = await implementation.open(new AbortController().signal);

    try {
      createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
      initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
      this.state.resource = resource;
      this.state.environment = resource.environment;
      return { environment: resource.environment, pid: process.pid };
    } catch (error) {
      try {
        await resource.dispose();
      } catch (cleanupError) {
        throw new AggregateError([error, cleanupError], 'Replication child initialization and cleanup failed');
      }
      throw error;
    }
  }

  private async setupIteration(
    payload: ReplicationChildSetupIterationPayload
  ): Promise<ReplicationChildResponsePayloads['setup_iteration']> {
    const resource = required(this.state.resource, 'storage resource');
    if (this.state.context != null) throw new Error('An iteration is already configured');
    const implementation = await constructReplicationChildImplementation<unknown>(payload.source);
    assertSourceImplementation(implementation, payload.source.exportName);
    const sourceConfiguration = implementation.getServiceSetup();
    const context = createBenchmarkServiceContext({
      serviceMode: system.ServiceContextMode.SYNC,
      configuration: sourceConfiguration.configuration,
      storageResource: resource
    });
    this.state.context = context;

    try {
      this.state.replicationStream = await resource.factory.updateSyncRules(
        updateSyncRulesFromYaml(payload.syncRules, {
          validate: true,
          defaultSchema: sourceConfiguration.defaultSchema,
          storageVersion: payload.storageVersion
        })
      );
      this.state.syncRulesContent = this.state.replicationStream.syncConfigContent[0];
      this.state.bucketStorage = resource.factory.getInstance(this.state.replicationStream);
      this.state.syncParameters = payload.syncParameters;
      this.state.defaultSchema = sourceConfiguration.defaultSchema;

      const engine = new replication.ReplicationEngine();
      context.register(replication.ReplicationEngine, engine);
      context.register(MetricsEngine, METRICS_HELPER.metricsEngine);
      await implementation.initialize(context);
      context.lifeCycleEngine.withLifecycle(engine, {
        start: (component) => component.start(),
        stop: (component) => component.shutDown()
      });
      return { replicationStreamName: this.state.replicationStream.replicationStreamName };
    } catch (error) {
      try {
        await this.cleanupIteration();
      } catch (cleanupError) {
        throw new AggregateError([error, cleanupError], 'Replication child iteration setup and cleanup failed');
      }
      throw error;
    }
  }

  private async waitForSnapshotCompletion(): Promise<{ position: string; visibleAtNs: string }> {
    const bucketStorage = required(this.state.bucketStorage, 'bucket storage');
    while (true) {
      const [status, checkpoint] = await Promise.all([bucketStorage.getStatus(), bucketStorage.getCheckpoint()]);
      if (status.snapshotDone && checkpoint.lsn != null) {
        return { position: checkpoint.lsn, visibleAtNs: process.hrtime.bigint().toString() };
      }
      await new Promise((resolve) => setTimeout(resolve, 30));
    }
  }

  private async collectEvidence(): Promise<ReplicationChildEvidencePayload> {
    const bucketStorage = required(this.state.bucketStorage, 'bucket storage');
    const content = required(this.state.syncRulesContent, 'sync rules content');
    const checkpoint = await bucketStorage.getCheckpoint();
    const status = await bucketStorage.getStatus();
    const lsn = checkpoint.lsn;
    if (lsn == null) {
      return { checkpoint: null, operations: [], snapshotDone: status.snapshotDone, bucketCount: 0 };
    }
    const buckets = await resolveBenchmarkBuckets({
      syncRules: bucketStorage.getParsedSyncRules({
        defaultSchema: required(this.state.defaultSchema, 'default schema')
      }),
      checkpoint,
      syncParameters: required(this.state.syncParameters, 'sync parameters')
    });
    const chunks = await new StorageDataHelpers(bucketStorage, content).getAllBucketData(
      bucketRequests(buckets),
      checkpoint
    );
    const operations = chunks.flatMap((chunk) => chunk.chunkData.data);
    return {
      checkpoint: lsn,
      operations: operations.map((operation) => ({
        op: operation.op,
        object_id: operation.object_id,
        data: operation.data
      })),
      snapshotDone: status.snapshotDone,
      bucketCount: buckets.length
    };
  }

  private async cleanupIteration(): Promise<void> {
    const errors: unknown[] = [];
    if (this.state.lifecycleStarted && this.state.context != null) {
      try {
        await this.state.context.lifeCycleEngine.stop();
      } catch (error) {
        errors.push(error);
      }
      this.state.lifecycleStarted = false;
    }
    if (this.state.replicationStream != null && this.state.bucketStorage != null) {
      let lock: storage.ReplicationLock | undefined;
      try {
        lock = await this.state.replicationStream.lock();
        await this.state.bucketStorage.terminate({ clearStorage: true });
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
    this.state.context = undefined;
    this.state.replicationStream = undefined;
    this.state.bucketStorage = undefined;
    this.state.syncRulesContent = undefined;
    this.state.syncParameters = undefined;
    this.state.defaultSchema = undefined;
    if (errors.length > 0) throw new AggregateError(errors, 'Replication child cleanup failed');
  }

  private async cleanupAll(): Promise<void> {
    const errors: unknown[] = [];
    try {
      await this.cleanupIteration();
    } catch (error) {
      errors.push(error);
    }
    if (this.state.resource != null) {
      try {
        await this.state.resource.dispose();
      } catch (error) {
        errors.push(error);
      }
    }
    this.state.resource = undefined;
    this.state.environment = undefined;
    if (errors.length > 0) throw new AggregateError(errors, 'Replication child run cleanup failed');
  }
}

function required<T>(value: T | undefined, name: string): T {
  if (value == null) throw new Error(`Replication child ${name} is not initialized`);
  return value;
}

function assertNever(value: never): never {
  throw new Error(`Unsupported replication child command ${JSON.stringify(value)}`);
}

function assertStorageImplementation(
  value: unknown,
  exportName: string
): asserts value is StorageBenchmarkImplementation {
  if (!isRecord(value) || typeof value.id !== 'string' || typeof value.open !== 'function') {
    throw new Error(`Replication child class "${exportName}" does not implement benchmark storage`);
  }
}

function assertSourceImplementation(
  value: unknown,
  exportName: string
): asserts value is ReplicationChildSourceImplementation {
  if (!isRecord(value) || typeof value.getServiceSetup !== 'function' || typeof value.initialize !== 'function') {
    throw new Error(`Replication child class "${exportName}" does not implement a replication source`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value != null && typeof value === 'object';
}
