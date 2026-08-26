import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { BATCH_OPTIONS, resolveTestTable, StorageDataHelpers } from '@powersync/service-core-tests';
import { generateBaselineStorageRows } from '../generators/generate-baseline-storage-rows.js';
import { BenchmarkCorrectnessCheck, BenchmarkCorrectnessResult } from '../types/BenchmarkIteration.js';
import { BenchmarkRunOptions } from '../types/BenchmarkRunOptions.js';
import {
  StorageBenchmarkImplementation,
  StorageBenchmarkItem,
  StorageBenchmarkIterationContext,
  StorageBenchmarkObservation,
  StorageBenchmarkRunContext,
  StorageBenchmarkScenario
} from '../types/StorageBenchmark.js';
import { bucketRequests, resolveBenchmarkBuckets } from '../utils/benchmark-buckets.js';
import { Benchmark } from './Benchmark.js';

const TARGET_POSITION = '1/1';
const SOURCE_TABLE = { schema: 'public', table: 'benchmark_items' } as const;

export class StorageBenchmark extends Benchmark<
  StorageBenchmarkScenario,
  StorageBenchmarkRunContext,
  StorageBenchmarkIterationContext,
  StorageBenchmarkObservation
> {
  constructor(
    scenario: StorageBenchmarkScenario,
    private readonly implementation: StorageBenchmarkImplementation,
    runOptions: BenchmarkRunOptions
  ) {
    super(scenario, runOptions);
  }

  protected async setupRun(signal: AbortSignal): Promise<StorageBenchmarkRunContext> {
    if (this.implementation.id !== this.scenario.storage.implementation) {
      throw new Error(
        `Storage implementation ${this.implementation.id} does not match scenario ${this.scenario.storage.implementation}`
      );
    }
    return { resource: await this.implementation.open(signal) };
  }

  protected async setupIteration(
    run: StorageBenchmarkRunContext,
    runtime: StorageBenchmarkIterationContext['runtime']
  ): Promise<StorageBenchmarkIterationContext> {
    runtime.signal.throwIfAborted();
    const manifest = generateBaselineStorageRows(this.scenario.workload);
    const flushes = { count: 0 };
    let replicationStream: storage.PersistedReplicationStream | undefined;
    let bucketStorage: storage.SyncRulesBucketStorage | undefined;
    let writer: storage.BucketStorageBatch | undefined;

    try {
      replicationStream = await run.resource.factory.updateSyncRules(
        updateSyncRulesFromYaml(this.scenario.syncRule(SOURCE_TABLE), {
          validate: true,
          defaultSchema: 'public',
          storageVersion: this.scenario.storage.version
        })
      );
      bucketStorage = run.resource.factory.getInstance(replicationStream);
      const syncRulesContent = replicationStream.syncConfigContent[0];
      writer = await bucketStorage.createWriter({
        ...BATCH_OPTIONS,
        hooks: {
          afterBatchFlush: async () => {
            flushes.count += 1;
          }
        }
      });
      const sourceTable = await resolveTestTable(writer, 'benchmark_items', ['id'], run.resource);
      await writer.markAllSnapshotDone('0/0');

      return {
        runtime,
        replicationStream,
        storage: bucketStorage,
        syncRulesContent,
        writer,
        sourceTable,
        manifest,
        flushes,
        targetPosition: TARGET_POSITION
      };
    } catch (error) {
      await this.cleanupPartialIteration(writer, replicationStream, bucketStorage, error);
      throw error;
    }
  }

  protected async executeIteration(
    context: StorageBenchmarkIterationContext,
    runtime: StorageBenchmarkIterationContext['runtime']
  ): Promise<StorageBenchmarkObservation> {
    runtime.signal.throwIfAborted();
    runtime.metrics.startBoundary('storage_write', 'first_writer_save');

    for (const row of context.manifest.rows) {
      runtime.signal.throwIfAborted();
      await context.writer.save({
        sourceTable: context.sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: row,
        afterReplicaId: row.id
      });
    }

    const commit = await context.writer.commit(context.targetPosition);
    runtime.metrics.endBoundary('storage_write', 'checkpoint_safe_commit');
    if (commit.checkpointBlocked || !commit.checkpointCreated) {
      throw new Error(`Storage commit did not create an unblocked checkpoint: ${JSON.stringify(commit)}`);
    }
    runtime.metrics.setCounter('source_rows', context.manifest.rows.length);
    runtime.metrics.setCounter('source_logical_bytes', context.manifest.sourceLogicalBytes);
    runtime.metrics.setCounter('payload_bytes', context.manifest.payloadBytes);
    runtime.metrics.setCounter('writer_save_calls', context.manifest.rows.length);
    runtime.metrics.setCounter('writer_flushes', context.flushes.count);

    return { commit };
  }

  protected async verifyIteration(
    observation: StorageBenchmarkObservation,
    context: StorageBenchmarkIterationContext,
    runtime: StorageBenchmarkIterationContext['runtime']
  ): Promise<BenchmarkCorrectnessResult> {
    const checkpoint = await context.storage.getCheckpoint();
    const buckets = await resolveBenchmarkBuckets({
      syncRules: context.storage.getParsedSyncRules({ defaultSchema: SOURCE_TABLE.schema }),
      checkpoint,
      syncParameters: this.scenario.sync_parameters
    });
    const chunks = await new StorageDataHelpers(context.storage, context.syncRulesContent).getAllBucketData(
      bucketRequests(buckets),
      checkpoint
    );
    const operations = chunks.flatMap((chunk) => chunk.chunkData.data);
    const firstExpected = context.manifest.rows[0];
    const lastExpected = context.manifest.rows.at(-1)!;
    const firstActual = operations.find((operation) => operation.object_id === firstExpected.id);
    const lastActual = operations.find((operation) => operation.object_id === lastExpected.id);
    const putCount = operations.filter((operation) => operation.op === 'PUT').length;

    const checks: BenchmarkCorrectnessCheck[] = [
      check('checkpoint_created', observation.commit.checkpointCreated && !observation.commit.checkpointBlocked, {
        commit: observation.commit
      }),
      check('checkpoint_position', checkpoint.lsn === context.targetPosition, {
        expected: context.targetPosition,
        actual: checkpoint.lsn
      }),
      check('bucket_count', buckets.length === this.scenario.expected_bucket_count, {
        expected: this.scenario.expected_bucket_count,
        actual: buckets.length
      }),
      check('operation_count', operations.length === this.scenario.expected_bucket_operation_count, {
        expected: this.scenario.expected_bucket_operation_count,
        actual: operations.length
      }),
      check('put_operation_count', putCount === this.scenario.expected_bucket_operation_count, {
        expected: this.scenario.expected_bucket_operation_count,
        actual: putCount
      }),
      check(
        'sample_payload_bytes',
        [firstExpected, lastExpected].every(
          (row) => Buffer.byteLength(row.payload, 'utf8') === this.scenario.workload.payload_bytes
        ),
        {
          expected: this.scenario.workload.payload_bytes,
          actual: [firstExpected, lastExpected].map((row) => Buffer.byteLength(row.payload, 'utf8'))
        }
      ),
      check('first_row', operationMatchesRow(firstActual, firstExpected), {
        expected_id: firstExpected.id,
        actual: firstActual ?? null
      }),
      check('last_row', operationMatchesRow(lastActual, lastExpected), {
        expected_id: lastExpected.id,
        actual: lastActual ?? null
      })
    ];

    runtime.metrics.setCounter('bucket_operations', operations.length);
    runtime.metrics.setCounter('parameter_operations', 0);
    runtime.metrics.setCounter('distinct_buckets', buckets.length);

    return {
      passed: checks.every((item) => item.passed),
      checks
    };
  }

  protected async cleanupIteration(context: StorageBenchmarkIterationContext): Promise<void> {
    await this.cleanupResources(context.writer, context.replicationStream, context.storage);
  }

  protected async collectRunMetadata(run: StorageBenchmarkRunContext): Promise<object> {
    return {
      ...run.resource.environment,
      storage_version: this.scenario.storage.version,
      mode: this.scenario.mode
    };
  }

  protected async cleanupRun(run: StorageBenchmarkRunContext): Promise<void> {
    await run.resource.dispose();
  }

  private async cleanupPartialIteration(
    writer: storage.BucketStorageBatch | undefined,
    replicationStream: storage.PersistedReplicationStream | undefined,
    bucketStorage: storage.SyncRulesBucketStorage | undefined,
    setupError: unknown
  ): Promise<void> {
    try {
      await this.cleanupResources(writer, replicationStream, bucketStorage);
    } catch (cleanupError) {
      throw new AggregateError([setupError, cleanupError], 'Storage iteration setup and cleanup failed');
    }
  }

  private async cleanupResources(
    writer: storage.BucketStorageBatch | undefined,
    replicationStream: storage.PersistedReplicationStream | undefined,
    bucketStorage: storage.SyncRulesBucketStorage | undefined
  ): Promise<void> {
    const errors: unknown[] = [];
    if (writer != null) {
      try {
        await writer[Symbol.asyncDispose]();
      } catch (error) {
        errors.push(error);
      }
    }
    if (replicationStream != null && bucketStorage != null) {
      let lock: storage.ReplicationLock | undefined;
      try {
        lock = await replicationStream.lock();
        await bucketStorage.terminate({ clearStorage: true });
      } catch (error) {
        errors.push(error);
      } finally {
        if (lock != null) {
          try {
            await lock.release();
          } catch (error) {
            errors.push(error);
          }
        }
      }
    }
    if (errors.length > 0) {
      throw new AggregateError(errors, 'Storage iteration cleanup failed');
    }
  }
}

function check(name: string, passed: boolean, details: object): BenchmarkCorrectnessCheck {
  return { name, passed, details };
}

function operationMatchesRow(
  operation: { op: string; data?: string | null } | undefined,
  expected: StorageBenchmarkItem
): boolean {
  if (operation?.op !== 'PUT' || typeof operation.data !== 'string') {
    return false;
  }
  try {
    const actual = JSON.parse(operation.data) as Partial<StorageBenchmarkItem>;
    return (
      actual.id === expected.id &&
      actual.owner_id === expected.owner_id &&
      actual.category === expected.category &&
      actual.version === expected.version &&
      actual.updated_at === expected.updated_at &&
      actual.payload === expected.payload
    );
  } catch {
    return false;
  }
}
