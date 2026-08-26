import {
  auth,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  replication,
  storage,
  system,
  updateSyncRulesFromYaml
} from '@powersync/service-core';
import { METRICS_HELPER, StorageDataHelpers } from '@powersync/service-core-tests';
import { CoreModule } from '@powersync/service-module-core';
import { createBenchmarkServiceContext } from '../replication/BenchmarkServiceContext.js';
import { constructReplicationChildImplementation } from '../replication/ReplicationChildClassLoader.js';
import { ReplicationChildSourceImplementation } from '../replication/ReplicationChildSourceImplementation.js';
import { StorageBenchmarkImplementation, StorageBenchmarkRunResource } from '../types/StorageBenchmark.js';
import { bucketRequests, resolveBenchmarkBuckets } from '../utils/benchmark-buckets.js';
import {
  CombinedChildCommand,
  CombinedChildCommandKind,
  CombinedChildEvidencePayload,
  CombinedChildInitializePayload,
  CombinedChildResponsePayloads,
  CombinedChildSetupIterationPayload
} from './combined-child-protocol.js';
import { createCombinedBenchmarkConfiguration } from './combined-configuration.js';

type CombinedChildRuntimePhase = 'new' | 'initialized' | 'iteration-ready' | 'running' | 'failed' | 'stopped';

interface CombinedChildRuntimeState {
  resource?: StorageBenchmarkRunResource;
  context?: system.ServiceContextContainer;
  replicationStream?: storage.PersistedReplicationStream;
  bucketStorage?: storage.SyncRulesBucketStorage;
  replicationLock?: storage.ReplicationLock;
  syncRulesContent?: storage.PersistedSyncConfigContent;
  lifecycleStarted: boolean;
  storageTerminated: boolean;
  environment?: object;
  syncParameters?: Record<string, unknown>;
  defaultSchema?: string;
}

const ALLOWED_COMMANDS: Record<CombinedChildRuntimePhase, readonly CombinedChildCommandKind[]> = {
  new: ['initialize', 'abort'],
  initialized: ['setup_iteration', 'shutdown', 'abort'],
  'iteration-ready': ['release_replication', 'monitor_start', 'monitor_stop', 'cleanup_iteration', 'abort'],
  running: ['collect_evidence', 'monitor_start', 'monitor_stop', 'cleanup_iteration', 'abort'],
  failed: ['abort'],
  stopped: []
};

export class CombinedChildRuntime {
  private readonly state: CombinedChildRuntimeState = { lifecycleStarted: false, storageTerminated: false };
  private phase: CombinedChildRuntimePhase = 'new';
  private monitorRunning = false;

  async execute(command: CombinedChildCommand): Promise<unknown> {
    this.assertAllowed(command.kind);
    try {
      switch (command.kind) {
        case 'initialize': {
          const response = await this.initialize(command.payload);
          this.phase = 'initialized';
          return response;
        }
        case 'setup_iteration': {
          const response = await this.setupIteration(command.payload);
          this.phase = 'iteration-ready';
          return response;
        }
        case 'release_replication': {
          const context = required(this.state.context, 'service context');
          if (this.state.lifecycleStarted) throw new Error('Combined child lifecycle is already running');
          const releasedAtNs = process.hrtime.bigint().toString();
          // A failed start may leave earlier lifecycle components running. Mark cleanup as required before awaiting it.
          this.state.lifecycleStarted = true;
          await context.lifeCycleEngine.start();
          this.phase = 'running';
          const waitForSnapshot = command.payload.waitForSnapshot === true;
          return { releasedAtNs, snapshot: waitForSnapshot ? await this.waitForSnapshotCompletion() : undefined };
        }
        case 'collect_evidence':
          return await this.collectEvidence();
        case 'cleanup_iteration':
          await this.cleanupIteration();
          this.phase = 'initialized';
          return {};
        case 'monitor_start': {
          const sample = this.sampleResources();
          this.monitorRunning = true;
          return sample;
        }
        case 'monitor_stop': {
          const sample = this.sampleResources();
          this.monitorRunning = false;
          return sample;
        }
        case 'shutdown':
        case 'abort':
          await this.cleanupAll();
          this.phase = 'stopped';
          return {};
        default:
          return assertNever(command);
      }
    } catch (error) {
      this.phase = 'failed';
      throw error;
    }
  }

  private async initialize(
    payload: CombinedChildInitializePayload
  ): Promise<CombinedChildResponsePayloads['initialize']> {
    if (this.state.resource != null) throw new Error('Combined child is already initialized');
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
        throw new AggregateError([error, cleanupError], 'Combined child initialization and cleanup failed');
      }
      throw error;
    }
  }

  private async setupIteration(
    payload: CombinedChildSetupIterationPayload
  ): Promise<CombinedChildResponsePayloads['setup_iteration']> {
    const resource = required(this.state.resource, 'storage resource');
    if (this.state.context != null) throw new Error('A combined child iteration is already configured');

    const sourceImplementation = await constructReplicationChildImplementation<unknown>(payload.source);
    assertSourceImplementation(sourceImplementation, payload.source.exportName);
    const sourceSetup = sourceImplementation.getServiceSetup();
    const collector = await auth.StaticKeyCollector.importKeys([payload.jwk]);
    const configuration = createCombinedBenchmarkConfiguration(
      sourceSetup.configuration,
      payload.port,
      new auth.KeyStore(collector)
    );
    const context = createBenchmarkServiceContext({
      serviceMode: system.ServiceContextMode.UNIFIED,
      configuration,
      storageResource: resource
    });
    this.state.context = context;

    try {
      const replicationStream = await resource.factory.updateSyncRules(
        updateSyncRulesFromYaml(payload.syncRules, {
          validate: true,
          defaultSchema: sourceSetup.defaultSchema,
          storageVersion: payload.storageVersion
        })
      );
      const syncRulesContent = required(replicationStream.syncConfigContent[0], 'sync rules content');
      const bucketStorage = resource.factory.getInstance(replicationStream);
      this.state.replicationStream = replicationStream;
      this.state.syncRulesContent = syncRulesContent;
      this.state.bucketStorage = bucketStorage;
      this.state.syncParameters = payload.syncParameters;
      this.state.defaultSchema = sourceSetup.defaultSchema;

      const replicationEngine = new replication.ReplicationEngine();
      context.register(replication.ReplicationEngine, replicationEngine);
      await new CoreModule().initialize(context);
      await sourceImplementation.initialize(context);

      // Register replication last: lifecycle start order makes storage and the HTTP router ready first.
      context.lifeCycleEngine.withLifecycle(replicationEngine, {
        start: (engine) => engine.start(),
        stop: (engine) => engine.shutDown()
      });

      return {
        replicationStreamName: replicationStream.replicationStreamName,
        endpoint: `http://127.0.0.1:${payload.port}`
      };
    } catch (error) {
      try {
        await this.cleanupIteration();
      } catch (cleanupError) {
        throw new AggregateError([error, cleanupError], 'Combined child iteration setup and cleanup failed');
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

  private async collectEvidence(): Promise<CombinedChildEvidencePayload> {
    const bucketStorage = required(this.state.bucketStorage, 'bucket storage');
    const content = required(this.state.syncRulesContent, 'sync rules content');
    const [checkpoint, status] = await Promise.all([bucketStorage.getCheckpoint(), bucketStorage.getStatus()]);
    const lsn = checkpoint.lsn;
    const storageCheckpoint = checkpoint.checkpoint.toString(10);
    if (lsn == null) {
      return { storageCheckpoint, checkpoint: null, operations: [], snapshotDone: status.snapshotDone, bucketCount: 0 };
    }

    const checkpointVisibleAtNs = process.hrtime.bigint().toString();
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
      storageCheckpoint,
      checkpoint: lsn,
      operations: operations.map((operation) => ({
        op: operation.op,
        object_id: operation.object_id,
        data: operation.data
      })),
      snapshotDone: status.snapshotDone,
      bucketCount: buckets.length,
      checkpointVisibleAtNs
    };
  }

  private sampleResources(): CombinedChildResponsePayloads['monitor_start'] {
    return {
      pid: process.pid,
      cpu: process.cpuUsage(),
      memory: process.memoryUsage(),
      sampledAtNs: process.hrtime.bigint().toString()
    };
  }

  private async cleanupIteration(): Promise<void> {
    const errors: unknown[] = [];
    if (this.state.lifecycleStarted && this.state.context != null) {
      try {
        await this.state.context.lifeCycleEngine.stop();
        this.state.lifecycleStarted = false;
      } catch (error) {
        errors.push(error);
      }
    }

    if (!this.state.lifecycleStarted && this.state.replicationStream != null && this.state.bucketStorage != null) {
      if (!this.state.storageTerminated && this.state.replicationLock == null) {
        try {
          this.state.replicationLock = await this.state.replicationStream.lock();
        } catch (error) {
          errors.push(error);
        }
      }

      if (!this.state.storageTerminated && this.state.replicationLock != null) {
        try {
          await this.state.bucketStorage.terminate({ clearStorage: true });
          this.state.storageTerminated = true;
        } catch (error) {
          errors.push(error);
        }
      }

      if (this.state.replicationLock != null) {
        try {
          await this.state.replicationLock.release();
          this.state.replicationLock = undefined;
        } catch (error) {
          errors.push(error);
        }
      }
    }

    const storageCleanupComplete =
      this.state.replicationStream == null || (this.state.storageTerminated && this.state.replicationLock == null);
    if (!this.state.lifecycleStarted && storageCleanupComplete) {
      this.state.context = undefined;
      this.state.replicationStream = undefined;
      this.state.bucketStorage = undefined;
      this.state.syncRulesContent = undefined;
      this.state.syncParameters = undefined;
      this.state.defaultSchema = undefined;
      this.state.storageTerminated = false;
      this.monitorRunning = false;
    }
    if (errors.length > 0) throw new AggregateError(errors, 'Combined child iteration cleanup failed');
  }

  private async cleanupAll(): Promise<void> {
    const errors: unknown[] = [];
    try {
      await this.cleanupIteration();
    } catch (error) {
      errors.push(error);
    }
    if (errors.length === 0 && this.state.resource != null) {
      try {
        await this.state.resource.dispose();
        this.state.resource = undefined;
        this.state.environment = undefined;
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length > 0) throw new AggregateError(errors, 'Combined child run cleanup failed');
  }

  private assertAllowed(kind: CombinedChildCommandKind): void {
    if (!ALLOWED_COMMANDS[this.phase].includes(kind)) {
      throw new Error(`Command ${kind} is invalid while combined child runtime is ${this.phase}`);
    }
    if (kind === 'monitor_start' && this.monitorRunning) {
      throw new Error('Command monitor_start is invalid while combined child runtime monitor is running');
    }
    if (kind === 'monitor_stop' && !this.monitorRunning) {
      throw new Error('Command monitor_stop is invalid while combined child runtime monitor is stopped');
    }
    if (kind === 'cleanup_iteration' && this.monitorRunning) {
      throw new Error('Command cleanup_iteration is invalid while combined child runtime monitor is running');
    }
  }
}

function required<T>(value: T | undefined, name: string): T {
  if (value == null) throw new Error(`Combined child ${name} is not initialized`);
  return value;
}

function assertNever(value: never): never {
  throw new Error(`Unsupported combined child command ${JSON.stringify(value)}`);
}

function assertStorageImplementation(
  value: unknown,
  exportName: string
): asserts value is StorageBenchmarkImplementation {
  if (!isRecord(value) || typeof value.id !== 'string' || typeof value.open !== 'function') {
    throw new Error(`Combined child class "${exportName}" does not implement benchmark storage`);
  }
}

function assertSourceImplementation(
  value: unknown,
  exportName: string
): asserts value is ReplicationChildSourceImplementation {
  if (!isRecord(value) || typeof value.getServiceSetup !== 'function' || typeof value.initialize !== 'function') {
    throw new Error(`Combined child class "${exportName}" does not implement a replication source`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value != null && typeof value === 'object';
}
