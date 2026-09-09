import { mongo } from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, register, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test, vi } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { MongoReplicationLease } from '../../src/storage/implementation/MongoReplicationLease.js';
import { ReplicationStreamDocumentV3 } from '../../src/storage/implementation/v3/models.js';
import { ObjectStorageLifecycle } from '../../src/storage/implementation/v3/object-storage/ObjectStorageLifecycle.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { SourceRecordStoreV3 } from '../../src/storage/implementation/v3/SourceRecordStoreV3.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { mongoTableId } from '../../src/utils/util.js';
import { env } from './env.js';
import { createMemoryS3TestStorageSuite } from './helpers/s3TestFactory.js';

const rules = `bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

// Run the shared writer contract against the actual pipeline, including partial
// rows, parameter changes, truncation, snapshot barriers and custom checkpoints.
// Keep the shared suite's inline cursor-pagination assumptions. Object storage
// is configured; the tests below exercise offloaded payloads.
const suite = createMemoryS3TestStorageSuite({
  url: env.MONGO_TEST_URL,
  isCI: env.CI,
  inlineThresholdBytes: 16 * 1024 * 1024
});
const pipelineFactory = { ...suite.factoryGen, storageVersion: 4, compressedBucketStorage: true };
describe('pipelined S3 writer data', () => register.registerDataStorageDataTests(pipelineFactory));
describe('pipelined S3 writer parameters', () => register.registerDataStorageParameterTests(pipelineFactory));
describe('pipelined S3 writer checkpoints', () => register.registerDataStorageCheckpointTests(pipelineFactory));

async function setup(syncRules = rules) {
  const { factoryGen, objectStorage } = createMemoryS3TestStorageSuite({ url: env.MONGO_TEST_URL, isCI: env.CI });
  const factory = await factoryGen.factory();
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(syncRules, { storageVersion: 4 }));
  const bucketStorage = factory.getInstance(stream) as MongoSyncBucketStorage;
  const writer = await bucketStorage.createWriter({ ...test_utils.BATCH_OPTIONS, storeCurrentData: false });
  const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
  await writer.markAllSnapshotDone('1/1');
  const db = bucketStorage.db as VersionedPowerSyncMongoV3;
  // clear() preserves S3 deletion markers. This setup creates a new in-memory
  // object store, so markers from previous test stores have no remaining objects.
  await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).deleteMany({});
  const definition = stream.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
  return { factory, writer, sourceTable, bucketStorage, stream, db, definition, objectStorage };
}

describe('replication pipeline', () => {
  test.each(['string', 'uuid', 'objectid', 'numeric', 'repeated'])(
    'batches independent mixed membership writes without reordering repeated keys (%s)',
    async (kind) => {
      const context = await setup();
      await using factory = context.factory;
      await using writer = context.writer;
      const ids = [0, 1, 2].map((i) =>
        kind === 'numeric'
          ? i + 1
          : kind === 'uuid'
            ? test_utils.rid(String(i))
            : kind === 'objectid'
              ? new mongo.ObjectId()
              : String(i)
      );
      const save = (tag: storage.SaveOperationTag, i: number, description: string) =>
        writer.save(
          tag === storage.SaveOperationTag.DELETE
            ? { sourceTable: context.sourceTable, tag, beforeReplicaId: ids[i] }
            : { sourceTable: context.sourceTable, tag, after: { id: String(i), description }, afterReplicaId: ids[i] }
        );
      await save(storage.SaveOperationTag.INSERT, 1, 'old');
      await save(storage.SaveOperationTag.INSERT, 2, 'old');
      await writer.commit('1/2');
      const records = context.db.sourceRecords(
        context.bucketStorage.replicationStreamId,
        mongoTableId(context.sourceTable.id)
      );
      const collection = vi.spyOn(context.db, 'sourceRecords').mockReturnValue(records);
      const writes = vi.spyOn(records, 'bulkWrite');
      try {
        await save(storage.SaveOperationTag.INSERT, 0, 'new');
        await save(storage.SaveOperationTag.UPDATE, 1, 'updated');
        await save(storage.SaveOperationTag.DELETE, 2, '');
        if (kind === 'repeated') {
          await save(storage.SaveOperationTag.DELETE, 0, '');
          await save(storage.SaveOperationTag.INSERT, 0, 'reinserted');
        }
        await writer.commit('1/3');
        expect(writes).toHaveBeenCalled();
        expect(writes.mock.calls[0][1]?.ordered).toBe(kind === 'numeric' || kind === 'repeated');
        const chunks = await test_utils.fromAsync(
          context.bucketStorage.getBucketDataBatch(await context.bucketStorage.getCheckpoint(), [
            bucketRequest(context.stream.syncConfigContent[0], 'global[]', 0n)
          ])
        );
        const ops = chunks.flatMap((chunk) => ('chunkData' in chunk ? chunk.chunkData.data : []));
        expect(JSON.parse(ops.filter((op) => op.object_id === '0').at(-1)!.data!).description).toBe(
          kind === 'repeated' ? 'reinserted' : 'new'
        );
        expect(JSON.parse(ops.filter((op) => op.object_id === '1').at(-1)!.data!).description).toBe('updated');
        expect(ops.filter((op) => op.object_id === '2').at(-1)!.op).toBe('REMOVE');
      } finally {
        writes.mockRestore();
        collection.mockRestore();
      }
    }
  );

  test('snapshot conflict replay retains membership-only rows without operation IDs', async () => {
    const context = await setup(`bucket_definitions:
  global:
    data:
      - SELECT id FROM items WHERE description = 'included'
`);
    await using factory = context.factory;
    await using original = context.writer;
    const { sourceTable, bucketStorage, db } = context;
    await original.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'existing', description: 'excluded' },
      afterReplicaId: test_utils.rid('existing')
    });
    await original.flush();
    await using writer = await bucketStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      storeCurrentData: false,
      skipExistingRows: true
    });
    for (let i = 0; i < 2000; i++)
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: String(i), description: 'excluded' },
        afterReplicaId: test_utils.rid(String(i))
      });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'existing', description: 'excluded' },
      afterReplicaId: test_utils.rid('existing')
    });
    const progress = await writer.queueTableProgress!(sourceTable, { replicatedCount: 2001 });
    await progress.persisted;
    expect(
      await db.sourceRecords(bucketStorage.replicationStreamId, mongoTableId(sourceTable.id)).countDocuments()
    ).toBe(2001);
    expect((await writer.getSourceTableStatus(sourceTable))?.snapshotStatus?.replicatedCount).toBe(2001);
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
  });

  test('snapshot replay backpressure reclaims a published prefix without draining later groups', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using original = context.writer;
    const atLimit = Promise.withResolvers<void>();
    let applications = 0;
    await using writer = await context.bucketStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      storeCurrentData: false,
      skipExistingRows: true,
      hooks: {
        beforeBatchFlush: async () => {
          if (++applications === 12) atLimit.resolve();
        }
      }
    });
    const gates = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
    const prepare = PersistedBatchV3.prototype.prepare;
    let groups = 0;
    const spy = vi.spyOn(PersistedBatchV3.prototype, 'prepare').mockImplementation(async function (
      this: PersistedBatchV3
    ) {
      const index = groups++;
      if (index < gates.length) await gates[index].promise;
      await prepare.call(this);
    });
    let admitted = false;
    const admission = (async () => {
      for (let i = 0; i < 30000; i++)
        await writer.save({
          sourceTable: context.sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: String(i), description: 'snapshot' },
          afterReplicaId: test_utils.rid(String(i))
        });
      admitted = true;
    })();
    void admission.catch(() => {});
    try {
      await atLimit.promise;
      expect(admitted).toBe(false);
      expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
      gates[0].resolve();
      // This would deadlock with the old drain-all barrier: group two is still held.
      await admission;
      expect(admitted).toBe(true);
      gates[1].resolve();
      await writer.commit('1/2');
      expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(30000n);
    } finally {
      for (const gate of gates) gate.resolve();
      await admission.catch(() => {});
      spy.mockRestore();
    }
  });

  test.each(['none', 'live', 'tombstone', 'upload-failure'])(
    'snapshot receipts publish ordered progress and recover without another producer call (%s)',
    async (mode) => {
      const context = await setup();
      await using factory = context.factory;
      await using original = context.writer;
      const { sourceTable, bucketStorage, objectStorage } = context;
      if (mode === 'live' || mode === 'tombstone') {
        await original.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: 'second', description: 'original' },
          afterReplicaId: test_utils.rid('second')
        });
        if (mode === 'tombstone')
          await original.save({
            sourceTable,
            tag: storage.SaveOperationTag.DELETE,
            beforeReplicaId: test_utils.rid('second')
          });
        await original.flush();
      }
      await using writer = await bucketStorage.createWriter({
        ...test_utils.BATCH_OPTIONS,
        storeCurrentData: false,
        skipExistingRows: true
      });
      const gates = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
      const put = objectStorage.put.bind(objectStorage);
      let uploads = 0;
      const upload = vi.spyOn(objectStorage, 'put').mockImplementation(async (...args) => {
        const index = uploads++;
        if (index < gates.length) await gates[index].promise;
        if (mode === 'upload-failure' && index === 1) throw new Error('upload failed');
        await put(...args);
      });
      const add = async (id: string, count: number) => {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id, description: 'snapshot' },
          afterReplicaId: test_utils.rid(id)
        });
        return writer.queueTableProgress!(sourceTable, {
          replicatedCount: count,
          totalEstimatedCount: 3,
          lastKey: new Uint8Array([count])
        });
      };
      try {
        const first = await add('first', 1);
        const second = await add('second', 2);
        const third = await add('third', 3);
        expect((await writer.getSourceTableStatus(sourceTable))?.snapshotStatus?.replicatedCount ?? 0).toBe(0);
        // The third upload is already free to finish, but cannot publish past either gate.
        gates[0].resolve();
        await first.persisted;
        expect((await writer.getSourceTableStatus(sourceTable))?.snapshotStatus?.replicatedCount).toBe(1);
        expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
        const empty =
          mode === 'none' ? await writer.queueTableProgress!(third.table, { totalEstimatedCount: 4 }) : undefined;
        expect((await writer.getSourceTableStatus(sourceTable))?.snapshotStatus?.replicatedCount).toBe(1);
        gates[1].resolve();
        if (mode === 'upload-failure') {
          await expect(second.persisted).rejects.toThrow('upload failed');
          await expect(third.persisted).rejects.toThrow('upload failed');
          await expect(writer.flush()).rejects.toThrow('upload failed');
          expect((await writer.getSourceTableStatus(sourceTable))?.snapshotStatus?.replicatedCount).toBe(1);
          return;
        }
        // In conflict cases these original receipts must survive replay, even
        // with no subsequent save/flush to drive recovery.
        await second.persisted;
        await third.persisted;
        await empty?.persisted;
        const status = (await writer.getSourceTableStatus(sourceTable))!.snapshotStatus!;
        expect(status.replicatedCount).toBe(3);
        expect(status.totalEstimatedCount).toBe(mode === 'none' ? 4 : 3);
        expect([...status.lastKey!]).toEqual([3]);
        await writer.commit('1/2');
        const checkpoint = await bucketStorage.getCheckpoint();
        const chunks = await test_utils.fromAsync(
          bucketStorage.getBucketDataBatch(checkpoint, [
            bucketRequest(context.stream.syncConfigContent[0], 'global[]', 0n)
          ])
        );
        const ops = chunks.flatMap((chunk) => ('chunkData' in chunk ? chunk.chunkData.data : []));
        expect(ops).toHaveLength(mode === 'tombstone' ? 4 : 3);
        const secondOps = ops.filter((op) => op.object_id === 'second');
        if (mode === 'tombstone') expect(secondOps.at(-1)?.op).toBe('REMOVE');
        else expect(JSON.parse(secondOps[0].data!).description).toBe(mode === 'live' ? 'original' : 'snapshot');
      } finally {
        for (const gate of gates) gate.resolve();
        upload.mockRestore();
      }
    }
  );

  test('snapshot progress receipts order empty boundaries and reject on disposal', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using original = context.writer;
    const writer = await context.bucketStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      storeCurrentData: false,
      skipExistingRows: true
    });
    const gate = Promise.withResolvers<void>();
    const put = context.objectStorage.put.bind(context.objectStorage);
    const upload = vi.spyOn(context.objectStorage, 'put').mockImplementation(async (...args) => {
      await gate.promise;
      await put(...args);
    });
    try {
      await writer.save({
        sourceTable: context.sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'row' },
        afterReplicaId: test_utils.rid('row')
      });
      const first = await writer.queueTableProgress!(context.sourceTable, { replicatedCount: 1 });
      const empty = await writer.queueTableProgress!(first.table, { totalEstimatedCount: 2 });
      expect((await writer.getSourceTableStatus(context.sourceTable))?.snapshotStatus?.replicatedCount ?? 0).toBe(0);
      const disposed = writer[Symbol.asyncDispose]();
      gate.resolve();
      await expect(first.persisted).rejects.toThrow();
      await expect(empty.persisted).rejects.toThrow();
      await disposed;
      expect((await original.getSourceTableStatus(context.sourceTable))?.snapshotStatus?.replicatedCount ?? 0).toBe(0);
    } finally {
      gate.resolve();
      upload.mockRestore();
    }
  });

  test.each([6000, 24001])('coalesces snapshot preparation blocks with bounded replay (%s rows)', async (count) => {
    const context = await setup();
    await using factory = context.factory;
    await using original = context.writer;
    await using writer = await context.bucketStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      storeCurrentData: false,
      skipExistingRows: true
    });
    const diagnostics = new storage.ReplicationDiagnostics();
    storage.ReplicationDiagnostics.active = diagnostics;
    const reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    try {
      for (let i = 0; i < count; i++) {
        await writer.save({
          sourceTable: context.sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `${i}`, description: 'snapshot' },
          afterReplicaId: test_utils.rid(`${i}`)
        });
      }
      await writer.commit('1/2');
      // Published input can be reclaimed without draining later groups.
      const publications = diagnostics.snapshot()['publication.transaction'].count;
      expect(publications).toBeGreaterThanOrEqual(count <= 24000 ? 1 : 2);
      expect(publications).toBeLessThanOrEqual(count <= 24000 ? 1 : 3);
      expect(reads).not.toHaveBeenCalled();
      expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(BigInt(count));
    } finally {
      storage.ReplicationDiagnostics.active = undefined;
      reads.mockRestore();
    }
  });

  test.each(['live', 'tombstone'])('replays a snapshot window after an earlier group commits (%s)', async (mode) => {
    const context = await setup();
    await using factory = context.factory;
    await using original = context.writer;
    const { sourceTable, bucketStorage, stream } = context;
    await original.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: '2100', description: 'original' },
      afterReplicaId: test_utils.rid('2100')
    });
    if (mode === 'tombstone')
      await original.save({
        sourceTable,
        tag: storage.SaveOperationTag.DELETE,
        beforeReplicaId: test_utils.rid('2100')
      });
    if (mode === 'tombstone') await original.flush();
    else await original.commit('1/2');
    await using writer = await bucketStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      storeCurrentData: false,
      skipExistingRows: true
    });
    // Exercise multiple publications inside one replay window. The first group
    // commits before the existing source key conflicts in the second group.
    const limit = vi.spyOn(PersistedBatchV3.prototype, 'shouldPublish').mockImplementation(function (
      this: PersistedBatchV3
    ) {
      return this.currentData.length >= 2000;
    });
    const diagnostics = new storage.ReplicationDiagnostics();
    storage.ReplicationDiagnostics.active = diagnostics;
    try {
      for (let i = 0; i < 6000; i++) {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `${i}`, description: 'snapshot' },
          afterReplicaId: test_utils.rid(`${i}`)
        });
      }
      await writer.commit('1/3');
      expect(diagnostics.snapshot()['snapshot.conflict_retry'].count).toBeGreaterThan(0);
      const checkpoint = await bucketStorage.getCheckpoint();
      const chunks = await test_utils.fromAsync(
        bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(stream.syncConfigContent[0], 'global[]', 0n)], {
          limit: 10000,
          chunkLimitBytes: 16 * 1024 * 1024
        })
      );
      const ops = chunks.flatMap((chunk) => ('chunkData' in chunk ? chunk.chunkData.data : []));
      expect(ops).toHaveLength(mode === 'live' ? 6000 : 6001);
      const retained = ops.filter((op) => op.object_id === '2100');
      expect(retained).toHaveLength(mode === 'live' ? 1 : 2);
      if (mode === 'live') expect(JSON.parse(retained[0].data!).description).toBe('original');
      else expect(retained.at(-1)?.op).toBe('REMOVE');
      expect(new Set(ops.filter((op) => op.object_id !== '2100').map((op) => op.object_id)).size).toBe(5999);
      expect(checkpoint.lsn).toBe('1/3');
    } finally {
      storage.ReplicationDiagnostics.active = undefined;
      limit.mockRestore();
    }
  });

  test.each(['new', 'existing', 'same-batch', 'tombstone'])(
    'optimistic membership inserts retry atomically on conflict (%s)',
    async (mode) => {
      const context = await setup();
      await using factory = context.factory;
      await using writer = context.writer;
      const { db, objectStorage, sourceTable, bucketStorage, definition, stream } = context;
      const records = db.sourceRecords(bucketStorage.replicationStreamId, mongoTableId(sourceTable.id));
      if (mode === 'existing' || mode === 'tombstone') {
        // A real unique-key conflict, including resurrection of a soft-deleted record.
        await records.insertOne({
          _id: test_utils.rid('row'),
          data: null,
          buckets: [],
          lookups: [],
          ...(mode === 'tombstone' ? { pending_delete: 1n } : {})
        });
      }
      const uploads = vi.spyOn(objectStorage, 'put');
      const reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
      const diagnostics = new storage.ReplicationDiagnostics();
      storage.ReplicationDiagnostics.active = diagnostics;
      const save = async (
        id: string,
        description: string,
        tag: storage.SaveOperationTag.INSERT | storage.SaveOperationTag.UPDATE = storage.SaveOperationTag.INSERT
      ) => {
        await writer.save({ sourceTable, tag, after: { id, description }, afterReplicaId: test_utils.rid(id) });
      };
      try {
        await save('prefix', 'one');
        if (mode === 'same-batch') await save('row', 'initial');
        await save('row', 'insert');
        await save('row', 'updated', storage.SaveOperationTag.UPDATE);
        await save('suffix', 'last');
        const receipt = await writer.queueResumeLsn!('1/2');
        await receipt.persisted;
        // Only the UPDATE is looked up, even when INSERTs conflict at publication.
        expect(reads).toHaveBeenCalledTimes(1);
        expect(reads.mock.calls[0][1]).toHaveLength(1);
        const timings = diagnostics.snapshot();
        expect(timings['transaction.callback'].count).toBe(mode === 'new' ? 1 : 2);
        expect(timings['transaction.abort']?.count ?? 0).toBe(mode === 'new' ? 0 : 1);
        expect(timings['transaction.commit'].count).toBe(1);
        expect(uploads).toHaveBeenCalledTimes(1);
        expect(await records.countDocuments()).toBe(3);
        const row = (await records.find({}).toArray()).find((record) =>
          record.buckets.some((bucket) => bucket.id === 'row')
        );
        expect(row?.pending_delete).toBeUndefined();
        expect(row?.buckets).toHaveLength(1);
        expect(await db.bucketData(bucketStorage.replicationStreamId, definition).countDocuments()).toBe(1);
        expect(await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).countDocuments()).toBe(0);
        const count = mode === 'same-batch' ? 5 : 4;
        const bucket = await db.bucketState(bucketStorage.replicationStreamId).findOne({});
        expect(bucket?.bucket_stats.count).toBe(count);
        await writer.commit('1/2');
        const checkpoint = await bucketStorage.getCheckpoint();
        expect(checkpoint.checkpoint).toBe(BigInt(count));
        const chunks = await test_utils.fromAsync(
          bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(stream.syncConfigContent[0], 'global[]', 0n)])
        );
        const data = chunks.flatMap((chunk) => ('chunkData' in chunk ? chunk.chunkData.data : []));
        expect(data).toHaveLength(count);
        expect(data.at(-2)).toMatchObject({ op: 'PUT', object_id: 'row' });
        expect(JSON.parse(data.at(-2)!.data!)).toMatchObject({ description: 'updated' });
      } finally {
        storage.ReplicationDiagnostics.active = undefined;
        uploads.mockRestore();
        reads.mockRestore();
      }
    }
  );

  test.each([false, true])(
    'skips insert lookups but preserves snapshot skips (skipExistingRows=%s)',
    async (skipExistingRows) => {
      const context = await setup();
      await using factory = context.factory;
      await using original = context.writer;
      await original.save({
        sourceTable: context.sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'row', description: 'original' },
        afterReplicaId: test_utils.rid('row')
      });
      await original.commit('1/2');
      await using writer = await context.bucketStorage.createWriter({
        ...test_utils.BATCH_OPTIONS,
        storeCurrentData: true,
        skipExistingRows
      });
      const reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
      const sizes = vi.spyOn(SourceRecordStoreV3.prototype, 'loadSizes');
      try {
        await writer.save({
          sourceTable: context.sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: 'row', description: 'replacement' },
          afterReplicaId: test_utils.rid('row')
        });
        await writer.save({
          sourceTable: context.sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: 'new', description: 'new' },
          afterReplicaId: test_utils.rid('new')
        });
        await writer.commit('1/3');
        expect(reads).toHaveBeenCalledTimes(skipExistingRows ? 1 : 0);
        expect(sizes).not.toHaveBeenCalled();
        const checkpoint = await context.bucketStorage.getCheckpoint();
        const chunks = await test_utils.fromAsync(
          context.bucketStorage.getBucketDataBatch(checkpoint, [
            bucketRequest(context.stream.syncConfigContent[0], 'global[]', 0n)
          ])
        );
        const data = chunks.flatMap((chunk) => ('chunkData' in chunk ? chunk.chunkData.data : []));
        expect(data).toHaveLength(skipExistingRows ? 2 : 3);
        const existing = data.filter((op) => op.object_id === 'row');
        expect(JSON.parse(existing.at(-1)!.data!)).toMatchObject({
          description: skipExistingRows ? 'original' : 'replacement'
        });
        expect(data.filter((op) => op.object_id === 'new')).toHaveLength(1);
      } finally {
        reads.mockRestore();
        sizes.mockRestore();
      }
    }
  );

  test('fresh snapshots skip membership and size reads', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using original = context.writer;
    await using writer = await context.bucketStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      storeCurrentData: true,
      skipExistingRows: true
    });
    const reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    const sizes = vi.spyOn(SourceRecordStoreV3.prototype, 'loadSizes');
    try {
      for (const id of ['first', 'second']) {
        await writer.save({
          sourceTable: context.sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id, description: id },
          afterReplicaId: test_utils.rid(id)
        });
      }
      await writer.commit('1/2');
      expect(reads).not.toHaveBeenCalled();
      expect(sizes).not.toHaveBeenCalled();
      const checkpoint = await context.bucketStorage.getCheckpoint();
      expect(checkpoint.checkpoint).toBe(2n);
    } finally {
      reads.mockRestore();
      sizes.mockRestore();
    }
  });

  test.each([false, true])('records transaction phases without changing publication (retry=%s)', async (retry) => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const diagnostics = new storage.ReplicationDiagnostics();
    const publish = ObjectStorageLifecycle.prototype.publishUploads;
    let calls = 0;
    const spy = vi.spyOn(ObjectStorageLifecycle.prototype, 'publishUploads').mockImplementation(async function (
      this: ObjectStorageLifecycle,
      ...args
    ) {
      await publish.apply(this, args);
      if (retry && calls++ === 0) {
        throw new mongo.MongoServerError({
          message: 'Retry instrumentation test',
          code: 112,
          errorLabels: ['TransientTransactionError']
        });
      }
    });
    storage.ReplicationDiagnostics.active = diagnostics;
    try {
      await writer.save({
        sourceTable: context.sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'profile', description: 'one' },
        afterReplicaId: test_utils.rid('profile')
      });
      const receipt = await writer.queueResumeLsn!('1/2');
      await receipt.persisted;
      const timings = diagnostics.snapshot();
      expect(timings['publication.transaction'].count).toBe(1);
      expect(timings['transaction.callback'].count).toBe(retry ? 2 : 1);
      expect(timings['transaction.commit'].count).toBe(1);
      expect(timings['transaction.abort']?.count ?? 0).toBe(retry ? 1 : 0);
      for (const phase of [
        'fence',
        'bucket_data.insert',
        'bucket_data.publish_uploads',
        'current_data.membership_write',
        'bucket_states',
        'resume_lsn',
        'persisted_op'
      ]) {
        expect(timings[`transaction.${phase}`].total_ms).toBeGreaterThanOrEqual(0);
      }
      const head = await context.db.sync_rules.findOne<ReplicationStreamDocumentV3>({
        _id: context.bucketStorage.replicationStreamId
      });
      expect(head?.resume_lsn).toBe('1/2');
      expect(head?.last_persisted_op).toBe(1n);
    } finally {
      storage.ReplicationDiagnostics.active = undefined;
      spy.mockRestore();
    }
  });

  test('persists each page resume position with its prefix, including empty pages', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { db, objectStorage, sourceTable, bucketStorage } = context;
    const releases = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
    const put = objectStorage.put.bind(objectStorage);
    let uploads = 0;
    vi.spyOn(objectStorage, 'put').mockImplementation(async (...args) => {
      await releases[uploads++].promise;
      await put(...args);
    });
    try {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'first', description: 'one' },
        afterReplicaId: test_utils.rid('first')
      });
      const first = await writer.queueResumeLsn!('1/1.1');
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'second', description: 'two' },
        afterReplicaId: test_utils.rid('second')
      });
      const second = await writer.queueResumeLsn!('1/1.2');
      releases[0].resolve();
      await first.persisted;
      const head = await db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: bucketStorage.replicationStreamId });
      expect(head?.resume_lsn).toBe('1/1.1');
      expect(head?.last_persisted_op).toBe(1n);
      expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
      // An empty page queues behind the second page rather than advancing the
      // restart position while that page's payload is still uploading.
      const empty = await writer.queueResumeLsn!('1/1.3');
      expect(
        (await db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: bucketStorage.replicationStreamId }))
          ?.resume_lsn
      ).toBe('1/1.1');
      releases[1].resolve();
      await second.persisted;
      await empty.persisted;
      expect(uploads).toBe(2);
      expect(
        (await db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: bucketStorage.replicationStreamId }))
          ?.resume_lsn
      ).toBe('1/1.3');
      await writer.commit('1/2');
      expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(2n);
    } finally {
      for (const release of releases) release.resolve();
    }
  });

  test('packs multiple preparation blocks into one publication group', async () => {
    const context = await setup(`bucket_definitions:
  by_user:
    accept_potentially_dangerous_queries: true
    parameters: SELECT value AS user_id FROM json_each(request.parameter('users'))
    data:
      - SELECT id, description FROM items WHERE owner = bucket.user_id
`);
    await using factory = context.factory;
    await using writer = context.writer;
    const { objectStorage, sourceTable, bucketStorage, db } = context;
    const uploads = vi.spyOn(objectStorage, 'put');
    for (let i = 0; i < 6000; i++) {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: `row-${i}`, owner: `user-${i % 100}`, description: 'x'.repeat(1024) },
        afterReplicaId: test_utils.rid(`row-${i}`)
      });
    }
    // Three preparation blocks have run; they have not forced 300 small objects.
    expect(uploads).not.toHaveBeenCalled();
    const receipt = await writer.queueResumeLsn!('1/1.1');
    await receipt.persisted;
    expect(uploads).toHaveBeenCalledTimes(100);
    const head = await db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: bucketStorage.replicationStreamId });
    expect(head?.last_persisted_op).toBe(6000n);
    expect(head?.resume_lsn).toBe('1/1.1');
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    await writer.commit('1/2');
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(6000n);
  });

  test('retains prepared memberships when a stale read finishes after publication', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { db, objectStorage, sourceTable, bucketStorage } = context;
    const release = Promise.withResolvers<void>();
    const put = objectStorage.put.bind(objectStorage);
    vi.spyOn(objectStorage, 'put').mockImplementationOnce(async (...args) => {
      await release.promise;
      await put(...args);
    });
    try {
      for (let i = 0; i < 2000; i++) {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: 'old', description: `${i}` },
          afterReplicaId: test_utils.rid('row')
        });
      }
      await writer.queueResumeLsn!('1/1.1');
      const load = SourceRecordStoreV3.prototype.loadDocuments;
      const reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
        this: SourceRecordStoreV3,
        ...args
      ) {
        const stale = await load.apply(this, args);
        release.resolve();
        await expect
          .poll(
            async () =>
              (await db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: bucketStorage.replicationStreamId }))
                ?.last_persisted_op
          )
          .toBe(2000n);
        return stale;
      });
      try {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.UPDATE,
          after: { id: 'new', description: 'updated' },
          afterReplicaId: test_utils.rid('row')
        });
        await writer.commit('1/2');
        // Removing the old output identity requires the preceding membership,
        // although it was absent from the query result used for this group.
        expect((await db.bucketState(bucketStorage.replicationStreamId).findOne({}))?.bucket_stats.count).toBe(2002);
      } finally {
        reads.mockRestore();
      }
    } finally {
      release.resolve();
    }
  });

  test('backpressures preparation when three groups are in flight', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { objectStorage, sourceTable, bucketStorage } = context;
    const started = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const put = objectStorage.put.bind(objectStorage);
    let uploads = 0;
    vi.spyOn(objectStorage, 'put').mockImplementation(async (...args) => {
      if (++uploads === 3) started.resolve();
      await release.promise;
      await put(...args);
    });
    let admitted = false;
    const saving = (async () => {
      for (let i = 0; i < 8000; i++) {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: 'row', description: `${i}` },
          afterReplicaId: test_utils.rid('row')
        });
        if ((i + 1) % 2000 === 0) await writer.queueResumeLsn!(`1/1.${i + 1}`);
      }
      admitted = true;
    })();
    try {
      await started.promise;
      // Let preparation reach its next admission attempt without relying on a
      // wall-clock throughput expectation.
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(admitted).toBe(false);
      expect(uploads).toBe(3);
      expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    } finally {
      release.resolve();
      await saving;
    }
    await writer.commit('1/2');
    expect(uploads).toBe(4);
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(8000n);
  });

  test('uploads later groups before the first finishes, but publishes a closed prefix', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { db, objectStorage, sourceTable, bucketStorage, definition, stream } = context;
    const firstStarted = Promise.withResolvers<void>();
    const secondUploaded = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const put = objectStorage.put.bind(objectStorage);
    let calls = 0;
    vi.spyOn(objectStorage, 'put').mockImplementation(async (...args) => {
      if (++calls === 1) {
        firstStarted.resolve();
        await release.promise;
      }
      await put(...args);
      if (calls >= 2) secondUploaded.resolve();
    });
    try {
      // One source record changes repeatedly across input and publication groups.
      for (let i = 0; i < 4000; i++) {
        await writer.save({
          sourceTable,
          tag: i === 0 ? storage.SaveOperationTag.INSERT : storage.SaveOperationTag.UPDATE,
          after: { id: 'row', description: `revision-${i}` },
          afterReplicaId: test_utils.rid('row')
        });
        if ((i + 1) % 2000 === 0) await writer.queueResumeLsn!(`1/1.${i + 1}`);
      }
      await firstStarted.promise;
      await secondUploaded.promise;
      expect(await db.bucketData(bucketStorage.replicationStreamId, definition).countDocuments()).toBe(0);
      const head = await db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: bucketStorage.replicationStreamId });
      expect(head?.last_persisted_op ?? 0n).toBe(0n);
      expect(head?.resume_lsn).toBeUndefined();
      expect((await db.op_id_sequence.findOne({ _id: 'main' }))!.op_id).toBe(4000n);
      expect(await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).countDocuments()).toBe(2);

      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.DELETE,
        beforeReplicaId: test_utils.rid('row')
      });
      const committing = writer.commit('1/2');
      release.resolve();
      await committing;
      expect(await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).countDocuments()).toBe(0);
      const checkpoint = await bucketStorage.getCheckpoint();
      const data = test_utils.getBatchData(
        await test_utils.fromAsync(
          bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(stream.syncConfigContent[0], 'global[]', 0n)], {
            limit: 5000,
            chunkLimitBytes: 16 * 1024 * 1024
          })
        )
      );
      expect(data).toHaveLength(4001);
      expect(data.at(-1)).toMatchObject({ op: 'REMOVE' });
      expect(checkpoint.checkpoint).toBe(4002n); // PUTs, REMOVE, and source-record tombstone.
    } finally {
      release.resolve();
    }
  });

  test('retries publication without uploading again or double counting', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { db, objectStorage, sourceTable, bucketStorage, definition } = context;
    const uploads = vi.spyOn(objectStorage, 'put');
    const update = db.sync_rules.updateOne.bind(db.sync_rules);
    let retried = false;
    const writes = vi.spyOn(db.sync_rules, 'updateOne').mockImplementation(async (...args) => {
      if (!retried && '$max' in args[1] && args[1].$max?.last_persisted_op != null) {
        retried = true;
        throw new mongo.MongoServerError({
          message: 'retry publication',
          code: 112,
          errorLabels: ['TransientTransactionError']
        });
      }
      return update(...args);
    });
    try {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'row', description: 'value' },
        afterReplicaId: test_utils.rid('row')
      });
      await writer.commit('1/2');
      expect(retried).toBe(true);
      expect(uploads).toHaveBeenCalledTimes(1);
      expect(await db.bucketData(bucketStorage.replicationStreamId, definition).countDocuments()).toBe(1);
      expect(await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).countDocuments()).toBe(0);
      const bucket = await db.bucketState(bucketStorage.replicationStreamId).findOne({});
      expect(bucket?.bucket_stats.count).toBe(1);
    } finally {
      writes.mockRestore();
    }
  });

  test('tracks a failed PUT as an orphan and restarts above its abandoned IDs', async () => {
    const context = await setup();
    await using factory = context.factory;
    const { db, objectStorage, sourceTable, bucketStorage, definition } = context;
    const put = objectStorage.put.bind(objectStorage);
    const uploads = vi.spyOn(objectStorage, 'put').mockImplementationOnce(async (...args) => {
      await put(...args); // The server stored it, but the client did not receive success.
      throw new Error('upload response lost');
    });
    {
      await using writer = context.writer;
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'row', description: 'value' },
        afterReplicaId: test_utils.rid('row')
      });
      const receipt = await writer.queueResumeLsn!('1/1.1');
      await expect(receipt.persisted).rejects.toThrow('upload response lost');
      expect(
        (await db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: bucketStorage.replicationStreamId }))
          ?.resume_lsn
      ).toBeUndefined();
      await expect(writer.commit('1/2')).rejects.toThrow('upload response lost');
    }
    const markers = db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId);
    expect(await markers.countDocuments()).toBe(1);
    expect(await db.bucketData(bucketStorage.replicationStreamId, definition).countDocuments()).toBe(0);
    const abandoned = (await db.op_id_sequence.findOne({ _id: 'main' }))!.op_id;
    uploads.mockRestore();
    await using next = await bucketStorage.createWriter({ ...test_utils.BATCH_OPTIONS, storeCurrentData: false });
    await next.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'row', description: 'value' },
      afterReplicaId: test_utils.rid('row')
    });
    await next.commit('1/2');
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBeGreaterThan(abandoned);
    expect(objectStorage.store.size).toBe(2);
    await markers.updateMany({}, { $set: { delete_after: new Date(0) } });
    await new ObjectStorageLifecycle(db, bucketStorage.replicationStreamId, objectStorage).cleanup(logger);
    expect(objectStorage.store.size).toBe(1);
    expect(await markers.countDocuments()).toBe(0);
  });

  test('a replaced owner cannot publish or release its successor lease', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { db } = context;
    await using first = await MongoReplicationLease.acquire(db);
    await db.locks.updateOne({ name: 'replication-writer' }, { $set: { 'active_lock.ts': new Date(0) } });
    await using second = await MongoReplicationLease.acquire(db);
    await expect(first.fence()).rejects.toThrow('lease was lost');
    await first[Symbol.asyncDispose]();
    await second.fence();
  });
});
