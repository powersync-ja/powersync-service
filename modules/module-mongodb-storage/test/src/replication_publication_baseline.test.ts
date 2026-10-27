import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { PerformanceTracer, Span, storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { compactActive, getTestStorage, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test, vi } from 'vitest';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';
import { PublicationCommandMonitor } from './helpers/PublicationCommandMonitor.js';

const SYNC_CONFIG = `
config:
  edition: 3
streams:
  items:
    query: SELECT id, description FROM items
`;

describe.each([3, 4])('replication publication baseline v%s', (storageVersion) => {
  test.each([
    { enabled: false, offloaded: false, compaction: false },
    { enabled: false, offloaded: false, compaction: true },
    { enabled: true, offloaded: false, compaction: false },
    { enabled: true, offloaded: true, compaction: false },
    { enabled: true, offloaded: false, compaction: true },
    { enabled: true, offloaded: true, compaction: true }
  ])('records wire commands and uploads: %j', async ({ enabled, offloaded, compaction }) => {
    const objectStorage = new MemoryObjectStorage();
    const factoryGen = mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      monitorCommands: true,
      objectStorage: enabled ? objectStorage : undefined,
      inlineThresholdBytes: offloaded ? 0 : 16 * 1024
    });
    await using factory = await factoryGen.factory();
    const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_CONFIG, { storageVersion }));
    const bucketStorage = (await getTestStorage(factory, stream)) as MongoSyncBucketStorageV3;
    const tracer = new PerformanceTracer<'storage' | 'evaluate'>('replication publication baseline');
    await using writer = await bucketStorage.createWriter({ ...test_utils.BATCH_OPTIONS, tracer });
    const table = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    async function save(description: string) {
      await writer.save({
        sourceTable: table,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'A', description: description.repeat(1000) },
        afterReplicaId: test_utils.rid('A')
      });
    }

    // Activate the config, reserve IDs and clear errors before measurement.
    // Two versions ensure real compaction can replace a chunk and bucket state.
    await save('a');
    await writer.commit('1/2');
    await save('b');
    await writer.commit('1/3');
    const horizon = (await bucketStorage.getCheckpoint()).checkpoint;
    const definitionId = stream.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const bucketStates = bucketStorage.db.bucketState(bucketStorage.replicationStreamId);
    const stateBefore = await bucketStates.findOne({ '_id.d': definitionId });
    expect(stateBefore?.bucket_stats.count).toBe(2);

    using monitor = new PublicationCommandMonitor(factory.db.client);
    const spans: Span[] = [];
    const span = tracer.span.bind(tracer);
    using _traceSpy = vi.spyOn(tracer, 'span').mockImplementation((...args) => {
      const result = span(...args);
      spans.push(result);
      return result;
    });
    let compacted = false;
    let compacting = false;
    async function compactOnce() {
      if (!compaction || compacted) {
        return;
      }
      compacted = true;
      compacting = true;
      try {
        await compactActive(factory, {
          maxOpId: horizon,
          compactBuckets: [stateBefore!._id.b],
          minBucketChanges: 1,
          minChangeRatio: 0
        });
      } finally {
        compacting = false;
      }
    }

    const uploads: { path: string; bytes: number; startedAt: number; endedAt: number; transactions: string[] }[] = [];
    const put = objectStorage.put.bind(objectStorage);
    using _putSpy = vi.spyOn(objectStorage, 'put').mockImplementation(async (...args) => {
      if (compacting) {
        return put(...args);
      }
      const sample = {
        path: args[0],
        bytes: args[1].byteLength,
        startedAt: performance.now(),
        endedAt: 0,
        transactions: monitor.activeReplicationAttempts.map((attempt) => attempt.key)
      };
      uploads.push(sample);
      // Hold the replication PUT open while real compaction commits. No elapsed
      // time assertion or scheduler race is needed to force the stale snapshot.
      await compactOnce();
      await put(...args);
      sample.endedAt = performance.now();
    });
    const flush = PersistedBatchV3.prototype.publish;
    using _flushSpy = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementation(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      // Inline batches have no PUT to pause. Compact after lookup/evaluation,
      // immediately before the writer persists its bucket-state delta instead.
      if (!offloaded) {
        await compactOnce();
      }
      return flush.apply(this, args);
    });

    await save('c');
    await writer.commit('1/4');
    const summary = monitor.summary();
    const attempts = monitor.replicationAttempts;
    // Compaction during an offloaded PUT now finishes before replication opens
    // its transaction. Inline work still injects compaction after the fence.
    const conflict = compaction && !offloaded;
    expect(compacted).toBe(compaction);
    expect(attempts).toHaveLength(conflict ? 2 : 1);
    expect(attempts.map((attempt) => attempt.outcome)).toEqual(conflict ? ['aborted', 'committed'] : ['committed']);
    expect(monitor.activeReplicationAttempts).toHaveLength(0);
    const bodies = spans.filter((span) => span.name === 'storage:transaction_body');
    expect(bodies).toHaveLength(attempts.length);
    expect(bodies.every((span) => span.endAt >= span.startAt)).toBe(true);
    const transactions = spans.filter((span) => span.name === 'storage:transaction');
    expect(transactions).toHaveLength(1);
    for (const body of bodies) {
      expect(transactions[0].startAt).toBeLessThanOrEqual(body.startAt);
      expect(transactions[0].endAt).toBeGreaterThanOrEqual(body.endAt);
    }
    // Ending a parent span also ends its children. Check containment of work
    // spans to catch a transaction span disposed before its promise settles.
    for (const persist of spans.filter((span) => span.name === 'storage:persist_flush')) {
      expect(bodies.some((body) => body.startAt <= persist.startAt && body.endAt >= persist.endAt)).toBe(true);
    }
    expect(tracer.stack).toHaveLength(0);
    expect(uploads).toHaveLength(offloaded ? 1 : 0);
    // Real compaction can run throughout a replication PUT without extending or
    // invalidating a replication transaction: the fence has not started yet.
    for (const upload of uploads) {
      expect(upload.transactions).toHaveLength(0);
      expect(attempts[0].startedAt).toBeGreaterThanOrEqual(upload.endedAt);
    }
    expect(new Set(uploads.map((upload) => upload.path)).size).toBe(uploads.length);
    expect(summary.replicationAttempts.at(-1)?.commands).toMatchObject({ findAndModify: 1, commitTransaction: 1 });
    if (summary.replicationAttempts.at(-1)?.commands.bulkWrite != null) {
      // The source-size query and grouped source-record read (R = 2) now run
      // outside the transaction, once per preparation. A conflict omits the
      // tail/commit but adds an abort command; replay sends no source reads.
      expect(summary.replicationAttempts.map((attempt) => attempt.commands)).toEqual([
        ...(conflict ? [{ findAndModify: 1, bulkWrite: 1, abortTransaction: 1 }] : []),
        { findAndModify: 1, bulkWrite: 2, commitTransaction: 1 }
      ]);
    }

    if (process.env.POWERSYNC_PUBLICATION_BASELINE) {
      console.info(
        JSON.stringify({
          storageVersion,
          objectStorageEnabled: enabled,
          offloaded,
          compaction,
          ...summary,
          uploads: uploads.map((upload) => ({ bytes: upload.bytes, durationMs: upload.endedAt - upload.startedAt })),
          transactionBodyMs: bodies.map((span) => (span.endAt - span.startAt) / 1000)
        })
      );
    }

    // Keep verification commands outside the measured summary.
    const checkpoint = await bucketStorage.getCheckpoint();
    expect(checkpoint.lsn).toBe('1/4');
    expect(checkpoint.checkpoint).toBe(horizon + 1n);
    const documents = await bucketStorage.db
      .bucketData(bucketStorage.replicationStreamId, definitionId)
      .find({})
      .toArray();
    const referencedPaths = documents.flatMap((doc) => (doc.storage_ref ? [doc.storage_ref.path] : []));
    for (const path of referencedPaths) {
      expect(objectStorage.store.has(path)).toBe(true);
    }
    if (offloaded && compaction) {
      expect(referencedPaths).toContain(uploads[0].path);
      expect(objectStorage.store.has(uploads[0].path)).toBe(true);
    }
  });
});
