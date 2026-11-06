import { storage, system, updateSyncRulesFromYaml } from '@powersync/service-core';
import { BATCH_OPTIONS } from '@powersync/service-core-tests';
import { CoreModule } from '@powersync/service-module-core';
import { randomUUID } from 'node:crypto';
import { createDataManifest, rowBatches } from '../generators/benchmark-data.js';
import { createBenchmarkServiceContext } from '../replication/BenchmarkServiceContext.js';
import { ApiBenchmarkScenario } from '../types/ApiBenchmark.js';
import { BenchmarkCorrectnessCheck, BenchmarkCorrectnessResult } from '../types/BenchmarkIteration.js';
import { BenchmarkIterationRuntime, BenchmarkRunOptions } from '../types/BenchmarkRunOptions.js';
import { StorageBenchmarkImplementation, StorageBenchmarkRunResource } from '../types/StorageBenchmark.js';
import {
  check,
  cleanup,
  createBenchmarkKey,
  createBenchmarkRouteApi,
  createConfiguration,
  createToken,
  reservePort
} from '../utils/api-utils.js';
import {
  benchmarkClientStreams,
  benchmarkMarkerIds,
  renderBenchmarkSync,
  verifyBenchmarkEvidence
} from '../utils/benchmark-sync.js';
import { resolveBenchmarkTables } from '../utils/benchmark-tables.js';
import {
  createNdjsonStreamReader,
  drainNdjsonResponse,
  drainNdjsonUntilCheckpoint,
  NdjsonDrainObservation,
  NdjsonStreamReader
} from '../utils/ndjson-drain.js';
import { Benchmark } from './Benchmark.js';

const TARGET_POSITION = '1/1';
const SOURCE_TABLE = { schema: 'public', table: 'benchmark_items' } as const;

interface ApiBenchmarkRunContext {
  readonly resource: StorageBenchmarkRunResource;
}
interface ApiBenchmarkIterationContext {
  readonly runtime: BenchmarkIterationRuntime;
  readonly replicationStream: storage.PersistedReplicationStream;
  readonly replicationLock: storage.ReplicationLock;
  readonly bucketStorage: storage.SyncRulesBucketStorage;
  readonly writer: storage.BucketStorageBatch;
  readonly sourceTables: ReadonlyMap<string, storage.SourceTable>;
  readonly serviceContext: system.ServiceContextContainer;
  readonly endpoint: string;
  readonly token: string;
  readonly streamReader?: NdjsonStreamReader;
  readonly initialCheckpoint?: string;
}

export class ApiBenchmark extends Benchmark<
  ApiBenchmarkScenario,
  ApiBenchmarkRunContext,
  ApiBenchmarkIterationContext,
  NdjsonDrainObservation
> {
  constructor(
    scenario: ApiBenchmarkScenario,
    private readonly implementation: StorageBenchmarkImplementation,
    runOptions: BenchmarkRunOptions
  ) {
    super(scenario, runOptions);
  }

  protected async setupRun(signal: AbortSignal): Promise<ApiBenchmarkRunContext> {
    if (this.implementation.id !== this.scenario.storage.implementation)
      throw new Error(
        `Storage implementation ${this.implementation.id} does not match scenario ${this.scenario.storage.implementation}`
      );
    return { resource: await this.implementation.open(signal) };
  }

  protected async setupIteration(
    run: ApiBenchmarkRunContext,
    runtime: BenchmarkIterationRuntime
  ): Promise<ApiBenchmarkIterationContext> {
    let replicationStream: storage.PersistedReplicationStream | undefined;
    let replicationLock: storage.ReplicationLock | undefined;
    let bucketStorage: storage.SyncRulesBucketStorage | undefined;
    let writer: storage.BucketStorageBatch | undefined;
    let serviceContext: system.ServiceContextContainer | undefined;
    let streamReader: NdjsonStreamReader | undefined;

    const manifest = createDataManifest(this.scenario.data, 'snapshot');

    try {
      const syncRules = renderBenchmarkSync(this.scenario.data, this.scenario.sync, SOURCE_TABLE);
      replicationStream = await run.resource.factory.updateSyncRules(
        updateSyncRulesFromYaml(syncRules, {
          validate: true,
          defaultSchema: 'public',
          storageVersion: this.scenario.storage.version
        })
      );

      replicationLock = await replicationStream.lock();
      bucketStorage = run.resource.factory.getInstance(replicationStream, { replicationLock });
      writer = await bucketStorage.createWriter(BATCH_OPTIONS);
      const sourceTables = await resolveBenchmarkTables(writer, this.scenario.data);
      await writer.markAllSnapshotDone('0/0');

      for (const range of manifest.snapshot)
        for (const batch of rowBatches(range, this.scenario.data.batch_size)) {
          runtime.signal.throwIfAborted();
          for (const row of batch)
            await writer.save({
              sourceTable: sourceTables.get(range.table.key)!,
              tag: storage.SaveOperationTag.INSERT,
              after: row,
              afterReplicaId: row.id
            });
          await writer.flush();
        }

      const commit = await writer.commit(TARGET_POSITION);

      if (commit.checkpointBlocked || !commit.checkpointCreated) {
        throw new Error('Initial benchmark data did not create a checkpoint');
      }

      const port = await reservePort();
      const key = await createBenchmarkKey();

      serviceContext = createBenchmarkServiceContext({
        serviceMode: system.ServiceContextMode.API,
        configuration: createConfiguration(port, key.store),
        storageResource: run.resource
      });
      serviceContext.routerEngine.registerAPI(createBenchmarkRouteApi());

      await new CoreModule().initialize(serviceContext);
      await serviceContext.lifeCycleEngine.start();

      const endpoint = `http://127.0.0.1:${port}`;
      const token = await createToken(key.signingKey);
      let initialCheckpoint: string | undefined;
      if (this.scenario.mode === 'streaming') {
        const response = await this.openSyncStream(endpoint, token, runtime.signal);
        streamReader = createNdjsonStreamReader(response, benchmarkMarkerIds(this.scenario.data));
        const initial = await streamReader.nextCheckpoint();
        if (
          initial.status !== 200 ||
          !verifyBenchmarkEvidence(this.scenario.data, this.scenario.sync, initial.evidence, ['snapshot']).every(
            (check) => check.passed
          ) ||
          initial.completedCheckpoint == null
        ) {
          throw new Error('Streaming API benchmark initial sync did not complete');
        }
        initialCheckpoint = initial.completedCheckpoint;
      }

      return {
        runtime,
        replicationStream,
        replicationLock,
        bucketStorage,
        writer,
        sourceTables,
        serviceContext,
        endpoint,
        token,
        streamReader,
        initialCheckpoint
      };
    } catch (error) {
      await streamReader?.close();
      await cleanup(serviceContext, writer, replicationLock, bucketStorage, error);
      throw error;
    }
  }

  protected async executeIteration(
    context: ApiBenchmarkIterationContext,
    runtime: BenchmarkIterationRuntime
  ): Promise<NdjsonDrainObservation> {
    if (this.scenario.mode === 'streaming') {
      const reader = context.streamReader;
      if (reader == null) throw new Error('Streaming API client was not initialized');
      const manifest = createDataManifest(this.scenario.data, 'streaming');
      runtime.metrics.startBoundary('end_to_end_streaming', 'storage_write_start');
      try {
        let transactionIndex = 0;
        for (const transaction of manifest.transactions) {
          for (const range of transaction.ranges)
            for (const batch of rowBatches(range, this.scenario.data.batch_size)) {
              runtime.signal.throwIfAborted();
              for (const row of batch)
                await context.writer.save({
                  sourceTable: context.sourceTables.get(range.table.key)!,
                  tag: storage.SaveOperationTag.INSERT,
                  after: row,
                  afterReplicaId: row.id
                });
              await context.writer.flush();
            }
          const commit = await context.writer.commit(`1/${(++transactionIndex + 1).toString(16)}`);
          if (commit.checkpointBlocked || !commit.checkpointCreated) {
            throw new Error('Streaming benchmark transaction did not create a checkpoint');
          }
        }
        runtime.metrics.startBoundary('http_read', 'storage_committed');
        const checkpoint = await context.bucketStorage.getCheckpoint();
        const observation = await drainNdjsonUntilCheckpoint(reader, checkpoint.checkpoint.toString());
        runtime.metrics.endBoundary('http_read', 'client_checkpoint_complete');
        this.recordClientCounters(observation, runtime);
        return observation;
      } finally {
        runtime.metrics.endBoundary('end_to_end_streaming', 'client_checkpoint_complete');
      }
    }
    runtime.metrics.startBoundary('http_read', 'request_start');
    let observation: NdjsonDrainObservation;
    try {
      const response = await this.openSyncStream(context.endpoint, context.token, runtime.signal);
      observation = await drainNdjsonResponse(response, benchmarkMarkerIds(this.scenario.data));
    } finally {
      runtime.metrics.endBoundary('http_read', 'stream_complete');
    }

    this.recordClientCounters(observation, runtime);
    return observation;
  }

  protected async verifyIteration(
    observation: NdjsonDrainObservation,
    context: ApiBenchmarkIterationContext
  ): Promise<BenchmarkCorrectnessResult> {
    const checks: BenchmarkCorrectnessCheck[] = [
      check('http_status', observation.status === 200, { actual: observation.status }),
      check('ndjson_content_type', observation.headers['content-type']?.includes('application/x-ndjson') === true, {
        actual: observation.headers['content-type']
      }),
      check('checkpoint_complete', observation.completedCheckpoint != null, {
        actual: observation.completedCheckpoint
      }),
      check('bucket_count', observation.bucketNames.length === this.scenario.expected_bucket_count, {
        expected: this.scenario.expected_bucket_count,
        actual: observation.bucketNames.length
      }),
      check('operation_count', observation.evidence.operationCount === this.scenario.expected_bucket_operation_count, {
        expected: this.scenario.expected_bucket_operation_count,
        actual: observation.evidence.operationCount
      }),
      ...verifyBenchmarkEvidence(this.scenario.data, this.scenario.sync, observation.evidence, [
        this.scenario.mode === 'streaming' ? 'streaming' : 'snapshot'
      ]),
      check('response_has_bytes', observation.wireBytes > 0, { actual: observation.wireBytes })
    ];

    if (this.scenario.mode === 'streaming') {
      checks.push(
        check('checkpoint_advanced', observation.completedCheckpoint !== context.initialCheckpoint, {
          initial: context.initialCheckpoint,
          actual: observation.completedCheckpoint
        }),
        check(
          'post_initial_rows_only',
          observation.evidence.byPhase.streaming === observation.evidence.operationCount,
          {
            operation_count: observation.evidence.operationCount
          }
        )
      );
    }

    return { passed: checks.every((item) => item.passed), checks };
  }

  protected async cleanupIteration(context: ApiBenchmarkIterationContext): Promise<void> {
    await context.streamReader?.close();
    await cleanup(context.serviceContext, context.writer, context.replicationLock, context.bucketStorage);
  }

  protected async collectRunMetadata(run: ApiBenchmarkRunContext): Promise<object> {
    return {
      ...run.resource.environment,
      storage_version: this.scenario.storage.version,
      mode: this.scenario.mode,
      transport: this.scenario.transport.encoding
    };
  }

  protected async cleanupRun(run: ApiBenchmarkRunContext): Promise<void> {
    await run.resource.dispose();
  }

  private async openSyncStream(endpoint: string, token: string, signal: AbortSignal): Promise<Response> {
    return await fetch(`${endpoint}/sync/stream`, {
      method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', 'accept-encoding': 'identity' },
      body: JSON.stringify({
        raw_data: true,
        client_id: randomUUID(),
        buckets: [],
        parameters: this.scenario.sync_parameters,
        streams: { include_defaults: true, subscriptions: benchmarkClientStreams(this.scenario.sync) }
      }),
      signal
    });
  }

  private recordClientCounters(observation: NdjsonDrainObservation, runtime: BenchmarkIterationRuntime): void {
    runtime.metrics.setCounter('response_wire_bytes', observation.wireBytes);
    runtime.metrics.setCounter('response_lines', observation.lineCount);
    runtime.metrics.setCounter('bucket_operations', observation.evidence.operationCount);
    runtime.metrics.setCounter('distinct_buckets', observation.bucketNames.length);
  }
}
