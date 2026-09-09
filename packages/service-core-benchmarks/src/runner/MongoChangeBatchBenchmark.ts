import { isBatchEnd, storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { BATCH_OPTIONS, resolveTestTable } from '@powersync/service-core-tests';
import { DirectSourceRowConverter, parseChangeDocument, writeMongoChange } from '@powersync/service-module-mongodb';
import { MongoStorageBenchmarkImplementation } from '../implementations/storage/MongoStorageBenchmarkImplementation.js';
import {
  ChangeBatchScenario,
  generateChangeBatches,
  operationAt,
  rawEvent
} from '../scenarios/mongodb-change-batches.js';
import { BenchmarkIterationRuntime, BenchmarkRunOptions } from '../types/BenchmarkRunOptions.js';
import { StorageBenchmarkRunResource } from '../types/StorageBenchmark.js';
import { bucketRequests, resolveBenchmarkBuckets } from '../utils/benchmark-buckets.js';
import { Benchmark } from './Benchmark.js';
import { ChangeBatchProfile } from './ChangeBatchProfile.js';

type Run = { resource: StorageBenchmarkRunResource; batches: Buffer[][]; generationMs: number };
type Iteration = {
  run: Run;
  storage: storage.SyncRulesBucketStorage;
  stream: storage.PersistedReplicationStream;
  writer: storage.BucketStorageBatch;
  table: storage.SourceTable;
  converter: DirectSourceRowConverter;
  flushes: { count: number };
  uploadsBefore: number;
  bytesBefore: number;
};
const TARGET = '2/0';

/** Exercise the production conversion/save path with a synthetic, already received change stream. */
export class MongoChangeBatchBenchmark extends Benchmark<ChangeBatchScenario, Run, Iteration, { duration: number }> {
  constructor(scenario: ChangeBatchScenario, options: BenchmarkRunOptions) {
    super(scenario, options);
  }

  protected async setupRun(signal: AbortSignal): Promise<Run> {
    const started = performance.now();
    const batches = generateChangeBatches(this.scenario);
    const generationMs = performance.now() - started;
    const resource = await new MongoStorageBenchmarkImplementation({
      url: process.env.BENCHMARK_MONGODB_STORAGE_URL ?? 'mongodb://127.0.0.1:27118/?directConnection=true',
      isCI: process.env.CI === 'true',
      ...(this.scenario.s3
        ? {
            inlineThresholdBytes: Number(process.env.BENCHMARK_S3_INLINE_THRESHOLD_BYTES ?? 0),
            objectStorage: {
              endpoint: process.env.BENCHMARK_S3_ENDPOINT ?? 'http://127.0.0.1:19000',
              bucket: process.env.BENCHMARK_S3_BUCKET ?? 'powersync-benchmark',
              region: process.env.BENCHMARK_S3_REGION ?? 'us-east-1',
              forcePathStyle: true,
              accessKeyId: process.env.BENCHMARK_S3_ACCESS_KEY ?? 'minioadmin',
              secretAccessKey: process.env.BENCHMARK_S3_SECRET_KEY ?? 'minioadmin'
            }
          }
        : {})
    }).open(signal);
    return { resource, batches, generationMs };
  }

  protected async setupIteration(run: Run, runtime: BenchmarkIterationRuntime): Promise<Iteration> {
    const started = performance.now();
    const stream = await run.resource.factory.updateSyncRules(
      updateSyncRulesFromYaml(this.scenario.syncRule({ schema: 'public', table: 'benchmark_items' }), {
        validate: true,
        defaultSchema: 'public',
        storageVersion: this.scenario.storage.version
      })
    );
    const bucketStorage = run.resource.factory.getInstance(stream);
    let writer: storage.BucketStorageBatch | undefined;
    try {
      const flushes = { count: 0 };
      writer = await bucketStorage.createWriter({
        ...BATCH_OPTIONS,
        // MongoDB supplies complete postimages, matching ChangeStream.streamChangesInternal.
        storeCurrentData: false,
        signal: runtime.signal,
        hooks: {
          afterBatchFlush: async () => {
            flushes.count++;
          }
        }
      });
      const table = await resolveTestTable(writer, 'benchmark_items', ['_id'], run.resource);
      const converter = new DirectSourceRowConverter(
        bucketStorage.getParsedSyncRules({ defaultSchema: 'public' }).compatibility
      );
      await writer.markAllSnapshotDone('0/0');
      for (let i = 0; i < this.scenario.workload.row_count; i++) {
        runtime.signal.throwIfAborted();
        if (operationAt(this.scenario.workload.mutations, i) !== 'insert') {
          await writeMongoChange(writer, table, parseChangeDocument(rawEvent(this.scenario, i, true)), converter);
        }
      }
      await writer.commit('1/0');
      flushes.count = 0;
      runtime.metrics.setCounter('setup_ms', performance.now() - started);
      runtime.metrics.setCounter('fixture_generation_ms', run.generationMs);
      runtime.metrics.setCounter('prefill_rows', this.scenario.workload.snapshot_row_count);
      return {
        run,
        stream,
        storage: bucketStorage,
        writer,
        table,
        converter,
        flushes,
        uploadsBefore: run.resource.objectStorageMetrics?.().uploads ?? 0,
        bytesBefore: run.resource.objectStorageMetrics?.().bytes ?? 0
      };
    } catch (error) {
      try {
        await writer?.[Symbol.asyncDispose]();
      } catch (cleanupError) {
        throw new AggregateError([error, cleanupError], 'Change-batch setup and cleanup failed');
      }
      throw error; // Run cleanup drops the isolated database and S3 prefix, including partial setup.
    }
  }

  protected async executeIteration(context: Iteration, runtime: BenchmarkIterationRuntime) {
    await using profile =
      runtime.kind === 'measured'
        ? await ChangeBatchProfile.start(`${this.scenario.id}.${this.runOptions.runId}.${runtime.iteration}`)
        : undefined;
    const started = performance.now();
    let bytes = 0;
    for (const [index, events] of context.run.batches.entries()) {
      for (const raw of events) {
        runtime.signal.throwIfAborted();
        bytes += raw.length;
        await writeMongoChange(context.writer, context.table, parseChangeDocument(raw), context.converter);
      }
      // Match production page admission. The final commit awaits every receipt;
      // these fixtures contain no split transactions or intermediate markers.
      const lsn = `1/${String(index + 1).padStart(12, '0')}`;
      if (context.writer.queueResumeLsn != null) {
        await context.writer.queueResumeLsn(lsn);
      } else {
        await context.writer.flush();
        await context.writer.setResumeLsn(lsn);
      }
    }
    // Model one safe checkpoint marker after the backlog, without a source marker round trip.
    const commit = await context.writer.commit(TARGET);
    const ended = performance.now();
    await profile?.finish(true);
    runtime.metrics.recordBoundary(
      'change_batches',
      'raw_bson_batch_received',
      'checkpoint_safe_commit',
      started,
      ended
    );
    if (!commit.checkpointCreated || commit.checkpointBlocked) throw new Error('Checkpoint was not committed');
    const duration = ended - started;
    for (const [name, value] of Object.entries({
      source_rows: this.scenario.workload.row_count,
      source_logical_bytes: bytes,
      raw_bson_bytes: bytes,
      rows_per_second: (this.scenario.workload.row_count * 1000) / duration,
      logical_mib_per_second: ((bytes / (1024 * 1024)) * 1000) / duration,
      change_batches: context.run.batches.length,
      writer_save_calls: this.scenario.workload.row_count,
      writer_flushes: context.flushes.count,
      s3_uploads: (context.run.resource.objectStorageMetrics?.().uploads ?? 0) - context.uploadsBefore,
      s3_upload_bytes: (context.run.resource.objectStorageMetrics?.().bytes ?? 0) - context.bytesBefore
    }))
      runtime.metrics.setCounter(name, value);
    return { duration };
  }

  protected async verifyIteration(
    _observation: { duration: number },
    context: Iteration,
    runtime: BenchmarkIterationRuntime
  ) {
    const started = performance.now();
    const checkpoint = await context.storage.getCheckpoint();
    const buckets = await resolveBenchmarkBuckets({
      syncRules: context.storage.getParsedSyncRules({ defaultSchema: 'public' }),
      checkpoint,
      syncParameters: this.scenario.sync_parameters
    });
    const remaining = new Map(bucketRequests(buckets).map((request) => [request.bucket, request]));
    let operations = 0,
      puts = 0,
      payloadBytes = 0,
      valid = true;
    const seen = new Uint8Array(this.scenario.workload.row_count);
    // Page through output without retaining payloads in memory. Check every final row and its routing.
    while (remaining.size) {
      runtime.signal.throwIfAborted();
      for await (const chunk of context.storage.getBucketDataBatch(checkpoint, [...remaining.values()])) {
        if (isBatchEnd(chunk)) break;
        for (const op of chunk.chunkData.data) {
          operations++;
          const index = Number(op.object_id?.replace(/^row-/, ''));
          if (!Number.isInteger(index) || index < 0 || index >= seen.length || op.object_id !== `row-${index}`) {
            valid = false;
            continue;
          }
          valid &&= chunk.chunkData.bucket.endsWith(`["user-${index % this.scenario.expected_bucket_count}"]`);
          if (op.op === 'PUT' && op.data != null) {
            puts++;
            payloadBytes += Buffer.byteLength(op.data);
            const row = JSON.parse(op.data);
            valid &&=
              row.id === `row-${index}` &&
              row.benchmark_user === `user-${index % this.scenario.expected_bucket_count}` &&
              chunk.chunkData.bucket.endsWith(`["${row.benchmark_user}"]`);
            if (row.version === 1) seen[index] = 1;
          } else if (op.op === 'REMOVE') seen[index] = 2;
          else valid = false;
        }
        if (chunk.chunkData.has_more) remaining.get(chunk.chunkData.bucket)!.start = BigInt(chunk.chunkData.next_after);
        else remaining.delete(chunk.chunkData.bucket);
      }
    }
    const uploads = (context.run.resource.objectStorageMetrics?.().uploads ?? 0) - context.uploadsBefore;
    const checks = [
      { name: 'checkpoint_position', passed: checkpoint.lsn === TARGET },
      { name: 'bucket_count', passed: buckets.length === this.scenario.expected_bucket_count },
      {
        name: 'operation_count',
        passed: operations === this.scenario.expected_bucket_operation_count,
        details: { actual: operations, expected: this.scenario.expected_bucket_operation_count }
      },
      { name: 'payload_and_routing', passed: valid },
      {
        name: 'every_final_change',
        passed: seen.every(
          (value, i) => value === (operationAt(this.scenario.workload.mutations, i) === 'delete' ? 2 : 1)
        )
      },
      { name: 's3_uploads', passed: !context.run.resource.objectStorageMetrics?.().required || uploads > 0 }
    ];
    runtime.metrics.setCounter('bucket_operations', operations);
    runtime.metrics.setCounter('distinct_buckets', buckets.length);
    runtime.metrics.setCounter('put_payload_bytes_mean', puts ? payloadBytes / puts : 0);
    runtime.metrics.setCounter('verification_ms', performance.now() - started);
    return { passed: checks.every((check) => check.passed), checks };
  }

  protected async cleanupIteration(context: Iteration) {
    await context.writer[Symbol.asyncDispose]();
    // The factory keeps stopped streams, but terminate removes iteration data before the next run.
    const lock = await context.stream.lock();
    try {
      await context.storage.terminate({ clearStorage: true });
    } finally {
      await lock.release();
    }
  }
  protected async collectRunMetadata(run: Run) {
    return {
      ...run.resource.environment,
      storage_version: this.scenario.storage.version,
      source: 'synthetic-raw-bson',
      profiling: process.env.BENCHMARK_PROFILE ?? 'false',
      checkpoint_policy: 'queued-resume-per-page-final-commit'
    };
  }
  protected async cleanupRun(run: Run) {
    await run.resource.dispose();
  }
}
