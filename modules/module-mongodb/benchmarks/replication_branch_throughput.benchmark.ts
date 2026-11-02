import { createCheckpoint, STANDALONE_CHECKPOINT_ID } from '@module/replication/MongoRelation.js';
import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { createMongoReplicationStreamBatch } from '@module/replication/MongoReplicationStream.js';
import { isBatchEnd, unsettledPromise } from '@powersync/service-core';
import { bucketRequest } from '@powersync/service-core-tests';
import { performance } from 'node:perf_hooks';
import { setTimeout } from 'node:timers/promises';
import { isMainThread } from 'node:worker_threads';
import { describe, expect, test } from 'vitest';
import { openChangeStreamTestContext } from '../test/src/change_stream_test_setup.js';
import { describeWithStorage, StorageVersionTestContext } from '../test/src/util.js';
import { ENV } from './env.js';

const EAGER = ENV.MEMORY_EAGER;
const ROW_COUNT = ENV.THROUGHPUT_ROWS;
const PAYLOAD_BYTES = ENV.THROUGHPUT_PAYLOAD_BYTES;
const PAYLOAD = 'x'.repeat(PAYLOAD_BYTES);
const BATCH_SIZE = 1_000;
const INSERT_BATCH_SIZE = Math.max(1, Math.min(BATCH_SIZE, Math.floor((8 * 1024 * 1024) / (PAYLOAD_BYTES + 64))));
const MEMORY_FIELDS = ['rss', 'heapUsed', 'arrayBuffers'] as const;

const CONFIG = /* yaml */ `
  # Same workload for both parsing modes.
  config:
    edition: 3
  streams:
    global:
      query: SELECT _id AS id, payload FROM documents
`;

/**
 * Run one replay in a fresh Node process using Vitest's forks pool, so RSS excludes the parent runner.
 * From this module: TEST_POSTGRES_STORAGE=false pnpm benchmark:replication -t 'storage v2'
 * The benchmark config supplies the forks pool, GC flag and serial execution.
 * Repeat separately with MEMORY_EAGER=true. Run sequentially against otherwise idle test databases.
 * Defaults to 2,000 rows with 1 MiB payloads; THROUGHPUT_ROWS and THROUGHPUT_PAYLOAD_BYTES override them.
 * This measures replay plus storage writes, not just parsing. MongoDB server memory is excluded.
 */
describe('replication throughput and memory', () => {
  describeWithStorage({ timeout: 300_000 }, (storage) => {
    test('measures one backlog replay', async () => {
      if (!isMainThread || typeof globalThis.gc != 'function') {
        throw new Error('Run with --pool=forks --execArgv=--expose-gc to isolate worker RSS');
      }
      const result = await measure(storage);
      process.stdout.write(
        JSON.stringify(
          {
            mode: EAGER ? 'eager' : 'lazy',
            pid: process.pid,
            rows: ROW_COUNT,
            payloadBytes: PAYLOAD_BYTES,
            ...result
          },
          null,
          2
        ) + '\n'
      );
    });
  });
});

async function measure({ factory, storageVersion }: StorageVersionTestContext) {
  {
    await using setup = await openChangeStreamTestContext(factory, { storageVersion });
    await setup.updateSyncRules(CONFIG);
    await setup.db.createCollection('documents');
    await setup.replicateSnapshot();
    await setup.getCheckpoint();
    // Leaving this scope aborts and awaits replication, then closes its storage and source connections.
  }

  const batchStats = { batches: 0, events: 0, maxEvents: 0 };
  let sampleBoundary = () => {};
  await using context = await openChangeStreamTestContext(factory, {
    storageVersion,
    doNotClear: true,
    streamOptions: {
      snapshotChunkLength: BATCH_SIZE,
      maxAwaitTimeMS: 200,
      createReplicationQueryProvider: () => ({
        ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
        async *openChangeStream({ open }) {
          for await (const batch of open({})) {
            // Both variants use the same wrapper. Only eager mode materializes parsed envelopes.
            const events = EAGER ? [...batch.events] : batch.events;
            sampleBoundary();
            yield createMongoReplicationStreamBatch({
              events: (function* () {
                let count = 0;
                for (const change of events) {
                  count++;
                  yield change;
                }
                batchStats.batches++;
                batchStats.events += count;
                batchStats.maxEvents = Math.max(batchStats.maxEvents, count);
              })(),
              resolveResumeToken: () => batch.getResumeToken(),
              resolveMetadata: () => batch.metadata
            });
          }
        }
      })
    }
  });
  await context.loadActiveSyncRules();
  // This context has not started replication. Populate the full backlog before the timed streaming phase.
  const collection = context.db.collection<{ _id: string; payload: string }>('documents');
  for (let offset = 0; offset < ROW_COUNT; offset += INSERT_BATCH_SIZE) {
    await collection.insertMany(
      Array.from({ length: Math.min(INSERT_BATCH_SIZE, ROW_COUNT - offset) }, (_, index) => ({
        _id: String(offset + index),
        payload: PAYLOAD
      }))
    );
  }
  // Create the target barrier before starting the measured replay.
  const target = await createCheckpoint(context.db, STANDALONE_CHECKPOINT_ID);
  // Reclaim unreachable setup allocations before sampling. GC runs naturally during replay.
  await setTimeout(0);
  globalThis.gc!();
  await setTimeout(0);
  globalThis.gc!();
  using memoryMonitor = sampleMemory();
  sampleBoundary = memoryMonitor.sample;
  const start = performance.now();
  const replication = context.startStreaming();
  const polling = new AbortController();
  const wait = (async () => {
    while (performance.now() - start < 120_000) {
      const checkpoint = await context.storage!.getCheckpoint();
      if (checkpoint.lsn != null && checkpoint.lsn >= target) {
        return checkpoint;
      }
      await setTimeout(10, undefined, { signal: polling.signal });
    }
    throw new Error('Timed out waiting for the backlog checkpoint');
  })();
  let checkpoint;
  try {
    checkpoint = await Promise.race([
      wait,
      unsettledPromise(replication).then(() => {
        throw new Error('Replication exited before the target checkpoint');
      })
    ]);
  } finally {
    polling.abort();
    await wait.catch(() => {});
  }
  const elapsedMs = performance.now() - start;
  const memory = memoryMonitor.finish();
  sampleBoundary = () => {};
  context.abort();
  await replication;
  // Verify incrementally so large payloads do not accumulate in a multi-gigabyte output array.
  const parsed = context.storage!.getParsedSyncConfigSet({ defaultSchema: context.db.databaseName });
  const ids = new Set<string>();
  let rowCount = 0;
  let after = 0n;
  let hasMore = true;
  while (hasMore) {
    hasMore = false;
    const previousAfter = after;
    for await (const chunk of context.storage!.getBucketDataBatch(
      checkpoint,
      [bucketRequest(parsed, 'global|0[]', after)],
      { limit: 8, chunkLimitBytes: 8 * 1024 * 1024 }
    )) {
      if (isBatchEnd(chunk)) {
        hasMore = chunk.hasMore;
      } else {
        after = BigInt(chunk.chunkData.next_after);
        hasMore = chunk.chunkData.has_more;
        for (const row of chunk.chunkData.data ?? []) {
          if (row.op != 'PUT') {
            continue;
          }
          rowCount++;
          ids.add(row.object_id!);
          expect(JSON.parse(row.data!).payload).toBe(PAYLOAD);
        }
      }
    }
    if (hasMore && after <= previousAfter) {
      throw new Error('Output verification made no progress');
    }
  }
  expect(rowCount).toBe(ROW_COUNT);
  expect(ids.size).toBe(ROW_COUNT);
  return { elapsedMs, rowsPerSecond: (ROW_COUNT * 1000) / elapsedMs, memory, batchStats };
}

/**
 * Sample process memory during replay, plus the provider boundary after eager materialization.
 * Peaks are per-field samples, not exact allocation maxima; RSS can retain setup allocations.
 */
function sampleMemory() {
  const baseline = process.memoryUsage();
  const peak = { ...baseline };
  let samples = 1;
  let lastSample = performance.now();
  let maxSampleGapMs = 0;
  function sample() {
    const now = performance.now();
    maxSampleGapMs = Math.max(maxSampleGapMs, now - lastSample);
    lastSample = now;
    const memory = process.memoryUsage();
    for (const field of MEMORY_FIELDS) {
      peak[field] = Math.max(peak[field], memory[field]);
    }
    samples++;
  }
  const timer = setInterval(sample, 25);
  return {
    sample,
    finish() {
      clearInterval(timer);
      sample();
      return {
        samples,
        maxSampleGapMs,
        MiB: Object.fromEntries(
          MEMORY_FIELDS.map((field) => [
            field,
            {
              baseline: baseline[field] / 1024 ** 2,
              peak: peak[field] / 1024 ** 2,
              growth: (peak[field] - baseline[field]) / 1024 ** 2
            }
          ])
        )
      };
    },
    [Symbol.dispose]() {
      clearInterval(timer);
    }
  };
}
