import { storage, system, updateSyncRulesFromYaml } from '@powersync/service-core';
import { BATCH_OPTIONS, resolveTestTable } from '@powersync/service-core-tests';
import { CoreModule } from '@powersync/service-module-core';
import { randomUUID } from 'node:crypto';
import { generateBaselineStorageRows } from '../generators/generate-baseline-storage-rows.js';
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
import { drainNdjsonResponse, NdjsonDrainObservation } from '../utils/ndjson-drain.js';
import { Benchmark } from './Benchmark.js';

const TARGET_POSITION = '1/1';
const SOURCE_TABLE = { schema: 'public', table: 'benchmark_items' } as const;

interface ApiBenchmarkRunContext {
  readonly resource: StorageBenchmarkRunResource;
}
interface ApiBenchmarkIterationContext {
  readonly runtime: BenchmarkIterationRuntime;
  readonly replicationStream: storage.PersistedReplicationStream;
  readonly bucketStorage: storage.SyncRulesBucketStorage;
  readonly writer: storage.BucketStorageBatch;
  readonly serviceContext: system.ServiceContextContainer;
  readonly endpoint: string;
  readonly token: string;
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
    let bucketStorage: storage.SyncRulesBucketStorage | undefined;
    let writer: storage.BucketStorageBatch | undefined;
    let serviceContext: system.ServiceContextContainer | undefined;

    const manifest = generateBaselineStorageRows(this.scenario.workload);

    try {
      const syncRules = this.scenario.syncRule(SOURCE_TABLE);
      replicationStream = await run.resource.factory.updateSyncRules(
        updateSyncRulesFromYaml(syncRules, {
          validate: true,
          defaultSchema: 'public',
          storageVersion: this.scenario.storage.version
        })
      );

      bucketStorage = run.resource.factory.getInstance(replicationStream);
      writer = await bucketStorage.createWriter(BATCH_OPTIONS);
      const sourceTable = await resolveTestTable(writer, 'benchmark_items', ['id'], run.resource);
      await writer.markAllSnapshotDone('0/0');

      for (const row of manifest.rows) {
        await writer.save({ sourceTable, tag: storage.SaveOperationTag.INSERT, after: row, afterReplicaId: row.id });
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

      return {
        runtime,
        replicationStream,
        bucketStorage,
        writer,
        serviceContext,
        endpoint: `http://127.0.0.1:${port}`,
        token: await createToken(key.signingKey)
      };
    } catch (error) {
      await cleanup(serviceContext, writer, replicationStream, bucketStorage, error);
      throw error;
    }
  }

  protected async executeIteration(
    context: ApiBenchmarkIterationContext,
    runtime: BenchmarkIterationRuntime
  ): Promise<NdjsonDrainObservation> {
    runtime.metrics.startBoundary('http_read', 'request_start');
    let observation: NdjsonDrainObservation;
    try {
      const response = await fetch(`${context.endpoint}/sync/stream`, {
        method: 'POST',
        headers: { authorization: `Bearer ${context.token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          raw_data: true,
          client_id: randomUUID(),
          buckets: [],
          parameters: this.scenario.sync_parameters
        }),
        signal: runtime.signal
      });
      observation = await drainNdjsonResponse(response);
    } finally {
      runtime.metrics.endBoundary('http_read', 'stream_complete');
    }

    runtime.metrics.setCounter('response_wire_bytes', observation.wireBytes);
    runtime.metrics.setCounter('response_lines', observation.lines.length);
    runtime.metrics.setCounter('bucket_operations', observation.operations.length);
    runtime.metrics.setCounter('distinct_buckets', observation.bucketNames.length);
    return observation;
  }

  protected async verifyIteration(
    observation: NdjsonDrainObservation,
    context: ApiBenchmarkIterationContext
  ): Promise<BenchmarkCorrectnessResult> {
    const dataLines = observation.lines.filter(
      (line) => line != null && typeof line === 'object' && 'data' in line
    ).length;

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
      check('operation_count', observation.operations.length === this.scenario.expected_bucket_operation_count, {
        expected: this.scenario.expected_bucket_operation_count,
        actual: observation.operations.length
      }),
      check('response_contains_data', dataLines > 0, {
        data_lines: dataLines,
        expected_operations: this.scenario.expected_bucket_operation_count
      }),
      check('response_has_bytes', observation.wireBytes > 0, { actual: observation.wireBytes })
    ];

    return { passed: checks.every((item) => item.passed), checks };
  }

  protected async cleanupIteration(context: ApiBenchmarkIterationContext): Promise<void> {
    await cleanup(context.serviceContext, context.writer, context.replicationStream, context.bucketStorage);
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
}
