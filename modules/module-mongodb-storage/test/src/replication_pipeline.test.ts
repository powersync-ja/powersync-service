import { mongo } from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test, vi } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { ReplicationStreamDocumentV3 } from '../../src/storage/implementation/v3/models.js';
import { ObjectStorageLifecycle } from '../../src/storage/implementation/v3/object-storage/ObjectStorageLifecycle.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { SourceRecordStoreV3 } from '../../src/storage/implementation/v3/SourceRecordStoreV3.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { env } from './env.js';
import { createMemoryS3TestStorageSuite } from './helpers/s3TestFactory.js';

const rules = `bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

async function setup(syncRules = rules) {
  const { factoryGen, objectStorage } = createMemoryS3TestStorageSuite({ url: env.MONGO_TEST_URL, isCI: env.CI });
  const factory = await factoryGen.factory();
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(syncRules, { storageVersion: 4 }));
  const bucketStorage = (await test_utils.getTestStorage(factory, stream)) as MongoSyncBucketStorage;
  const writer = await bucketStorage.createWriter({ ...test_utils.BATCH_OPTIONS, storeCurrentData: false });
  const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
  await writer.markAllSnapshotDone('1/1');
  const db = bucketStorage.db as VersionedPowerSyncMongoV3;
  // clear() preserves S3 deletion markers. This setup creates a new in-memory
  // object store, so markers from previous test stores have no remaining objects.
  await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).deleteMany({});
  const definition = stream.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
  return { factoryGen, factory, writer, sourceTable, bucketStorage, stream, db, definition, objectStorage };
}

describe('replication pipeline', () => {
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
      for (const release of releases) {
        release.resolve();
      }
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
      if (++uploads === 3) {
        started.resolve();
      }
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
        if ((i + 1) % 2000 === 0) {
          await writer.queueResumeLsn!(`1/1.${i + 1}`);
        }
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
      if (calls >= 2) {
        secondUploaded.resolve();
      }
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
        if ((i + 1) % 2000 === 0) {
          await writer.queueResumeLsn!(`1/1.${i + 1}`);
        }
      }
      await firstStarted.promise;
      await secondUploaded.promise;
      expect(await db.bucketData(bucketStorage.replicationStreamId, definition).countDocuments()).toBe(0);
      const head = await db.sync_rules.findOne<ReplicationStreamDocumentV3>({ _id: bucketStorage.replicationStreamId });
      expect(head?.last_persisted_op ?? 0n).toBe(0n);
      expect(head?.resume_lsn).toBeUndefined();
      expect((await db.op_id_sequence.findOne({ _id: 'main' }))!.op_id).toBe(65536n);
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
    const flush = PersistedBatchV3.prototype.flush;
    let retried = false;
    const writes = vi.spyOn(PersistedBatchV3.prototype, 'flush').mockImplementation(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      const result = await flush.apply(this, args);
      if (!retried) {
        retried = true;
        throw new mongo.MongoServerError({
          message: 'retry publication',
          code: 112,
          errorLabels: ['TransientTransactionError']
        });
      }
      return result;
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
    const abandoned = 1n; // Assigned IDs are consumed even when publication fails; the unused tail is reusable.
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

  test.each(['truncate', 'drop', 'progress', 'snapshot', 'resolve', 'resume'])(
    '%s waits for pending publication',
    async (barrier) => {
      const context = await setup();
      await using factory = context.factory;
      await using writer = context.writer;
      const { sourceTable, objectStorage, bucketStorage, db } = context;
      const started = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const put = objectStorage.put.bind(objectStorage);
      vi.spyOn(objectStorage, 'put').mockImplementationOnce(async (...args) => {
        started.resolve();
        await release.promise;
        await put(...args);
      });
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'row', description: 'value' },
        afterReplicaId: test_utils.rid('row')
      });
      const receipt = await writer.queueResumeLsn!('1/1.1');
      await started.promise;
      let finished = false;
      const action = (async () => {
        if (barrier === 'truncate') {
          await writer.truncate([sourceTable]);
        }
        if (barrier === 'drop') {
          await writer.drop([sourceTable]);
        }
        if (barrier === 'progress') {
          await writer.updateTableProgress(sourceTable, { replicatedCount: 1 });
        }
        if (barrier === 'snapshot') {
          await writer.markTableSnapshotDone([sourceTable], '1/2');
        }
        if (barrier === 'resolve') {
          await test_utils.resolveTestTable(writer, 'items', ['id'], context.factoryGen, 1);
        }
        if (barrier === 'resume') {
          await writer.setResumeLsn('1/3');
        }
        finished = true;
      })();
      try {
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(finished).toBe(false);
      } finally {
        release.resolve();
        await receipt.persisted;
        await action;
      }
      expect(await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).countDocuments()).toBe(0);
    }
  );

  test('disposal joins a membership read and prevents its publication', async () => {
    const context = await setup();
    await using factory = context.factory;
    const { writer, sourceTable, objectStorage, db } = context;
    const started = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const load = SourceRecordStoreV3.prototype.loadDocuments;
    const reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      started.resolve();
      await release.promise;
      return load.apply(this, args);
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'row', description: 'value' },
      afterReplicaId: test_utils.rid('row')
    });
    const admission = writer.queueResumeLsn!('1/1.1');
    const rejected = expect(admission).rejects.toThrow();
    await started.promise;
    let disposed = false;
    const disposing = writer.dispose().then(() => {
      disposed = true;
    });
    try {
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(disposed).toBe(false);
    } finally {
      release.resolve();
      await rejected;
      await disposing;
      reads.mockRestore();
    }
    expect(objectStorage.store.size).toBe(0);
  });

  test('disposal aborts uploads and rejects pending progress receipts', async () => {
    const context = await setup();
    await using factory = context.factory;
    const { writer, sourceTable, objectStorage, db, bucketStorage } = context;
    const started = Promise.withResolvers<void>();
    vi.spyOn(objectStorage, 'put').mockImplementationOnce(async (_path, _data, _metadata, options) => {
      started.resolve();
      await new Promise<void>((_resolve, reject) => {
        options!.signal!.addEventListener('abort', () => reject(options!.signal!.reason), { once: true });
      });
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'row', description: 'value' },
      afterReplicaId: test_utils.rid('row')
    });
    const receipt = await writer.queueResumeLsn!('1/1.1');
    await started.promise;
    const rejected = expect(receipt.persisted).rejects.toThrow();
    await writer.dispose();
    await rejected;
    expect(await db.pendingObjectStorageDeletes(bucketStorage.replicationStreamId).countDocuments()).toBe(1);
  });

  test('range exhaustion retries only the unfinished row and preserves earlier prepared changes', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { sourceTable, bucketStorage, stream, db } = context;
    const allocator = factory.getOpIdAllocator(stream, stream.current_lock!);
    await allocator.reserve();
    allocator.consume(65_534n);
    const capacity = vi.spyOn(allocator, 'ensureCapacity').mockResolvedValue(undefined);
    const reservations = vi.spyOn(allocator, 'reserve');
    try {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'old', description: 'initial' },
        afterReplicaId: test_utils.rid('row')
      });
      // Replacing the output identity emits a PUT and REMOVE. Only one ID
      // remains after the first row, forcing a retry halfway through this row.
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'new', description: 'updated' },
        afterReplicaId: test_utils.rid('row')
      });
      await writer.commit('1/2');
      expect(reservations).toHaveBeenCalledTimes(1);
      expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(65_537n);
      expect((await db.bucketState(bucketStorage.replicationStreamId).findOne({}))?.bucket_stats.count).toBe(3);
    } finally {
      capacity.mockRestore();
      reservations.mockRestore();
    }
  });

  test.each(['takeover', 'same lease'])(
    '%s in another process rejects stale prepared membership before publishing references',
    async (mode) => {
      const context = await setup();
      await using factory = context.factory;
      await using writer = context.writer;
      const { sourceTable, bucketStorage, stream, db, objectStorage, definition } = context;
      const started = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const put = objectStorage.put.bind(objectStorage);
      vi.spyOn(objectStorage, 'put').mockImplementationOnce(async (...args) => {
        started.resolve();
        await release.promise;
        await put(...args);
      });
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'row', description: 'stale' },
        afterReplicaId: test_utils.rid('row')
      });
      const receipt = await writer.queueResumeLsn!('1/1.1');
      await started.promise;
      let successor = stream.current_lock!;
      try {
        if (mode === 'takeover') {
          await db.sync_rules.updateOne(
            { _id: stream.replicationStreamId },
            { $set: { 'lock.expires_at': new Date(0) } }
          );
          successor = await stream.lock();
        }
        // A separate factory supplies independent in-process coordination, as in another process.
        await using other = await context.factoryGen.factory({ doNotClear: true });
        await using next = await other.getInstance(stream, { replicationLock: successor }).createWriter({
          ...test_utils.BATCH_OPTIONS,
          storeCurrentData: false
        });
        const nextTable = await test_utils.resolveTestTable(next, 'items', ['id'], context.factoryGen, 1);
        await next.save({
          sourceTable: nextTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: 'row', description: 'current' },
          afterReplicaId: test_utils.rid('row')
        });
        // This must succeed while the old upload is stalled, with no global lease.
        await next.commit('1/2');
        const checkpoint = await bucketStorage.getCheckpoint();
        const failure = expect(receipt.persisted).rejects.toThrow(
          mode === 'takeover' ? 'no longer owns' : 'advanced during publication'
        );
        release.resolve();
        await failure;
        expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(checkpoint.checkpoint);
        expect(await db.bucketData(stream.replicationStreamId, definition).countDocuments()).toBe(1);
        expect(await db.pendingObjectStorageDeletes(stream.replicationStreamId).countDocuments()).toBe(1);
      } finally {
        release.resolve();
        if (mode === 'takeover') {
          await successor.release();
        }
      }
    }
  );

  test('another writer of the same stream seals an unfinished group before reading membership', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { sourceTable, bucketStorage, objectStorage, db } = context;
    const uploads = vi.spyOn(objectStorage, 'put');
    // Automatic application retains an unsealed group smaller than the publication target.
    for (let i = 0; i < 2000; i++) {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'old', description: `${i}` },
        afterReplicaId: test_utils.rid('row')
      });
    }
    expect(uploads).not.toHaveBeenCalled();
    await using next = await bucketStorage.createWriter({ ...test_utils.BATCH_OPTIONS, storeCurrentData: false });
    await next.save({
      sourceTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'new', description: 'updated' },
      afterReplicaId: test_utils.rid('row')
    });
    await next.commit('1/2');
    expect((await db.bucketState(bucketStorage.replicationStreamId).findOne({}))?.bucket_stats.count).toBe(2002);
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(2002n);
    expect(uploads).toHaveBeenCalledTimes(2);
  });

  test('a later upload failure stops publication of the entire pending suffix', async () => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { sourceTable, objectStorage, db, stream, definition } = context;
    const release = Promise.withResolvers<void>();
    const put = objectStorage.put.bind(objectStorage);
    vi.spyOn(objectStorage, 'put')
      .mockImplementationOnce(async (...args) => {
        await release.promise;
        await put(...args);
      })
      .mockRejectedValueOnce(new Error('second upload failed'));
    try {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'one', description: 'one' },
        afterReplicaId: test_utils.rid('one')
      });
      const first = await writer.queueResumeLsn!('1/1.1');
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'two', description: 'two' },
        afterReplicaId: test_utils.rid('two')
      });
      const second = await writer.queueResumeLsn!('1/1.2');
      await expect.poll(() => objectStorage.put).toHaveBeenCalledTimes(2);
      const firstFailed = expect(first.persisted).rejects.toThrow('second upload failed');
      const secondFailed = expect(second.persisted).rejects.toThrow('second upload failed');
      release.resolve();
      await Promise.all([firstFailed, secondFailed]);
      expect(await db.bucketData(stream.replicationStreamId, definition).countDocuments()).toBe(0);
      expect(await db.pendingObjectStorageDeletes(stream.replicationStreamId).countDocuments()).toBe(2);
    } finally {
      release.resolve();
    }
  });

  test('an upload failure cancels and joins the other uploads in its group', async () => {
    const context = await setup(`bucket_definitions:
  by_user:
    accept_potentially_dangerous_queries: true
    parameters: SELECT value AS user_id FROM json_each(request.parameter('users'))
    data:
      - SELECT id FROM items WHERE owner = bucket.user_id
`);
    await using factory = context.factory;
    await using writer = context.writer;
    const { sourceTable, objectStorage, db, stream } = context;
    const started = Promise.withResolvers<void>();
    let cancelled = false;
    vi.spyOn(objectStorage, 'put')
      .mockImplementationOnce(async (_path, _data, _metadata, options) => {
        started.resolve();
        await new Promise<void>((_resolve, reject) => {
          options!.signal!.addEventListener(
            'abort',
            () => {
              cancelled = true;
              reject(options!.signal!.reason);
            },
            { once: true }
          );
        });
      })
      .mockImplementationOnce(async () => {
        await started.promise;
        throw new Error('upload failed');
      });
    for (const owner of ['first', 'second']) {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: owner, owner },
        afterReplicaId: test_utils.rid(owner)
      });
    }
    await expect(writer.commit('1/2')).rejects.toThrow('upload failed');
    expect(cancelled).toBe(true);
    expect(await db.pendingObjectStorageDeletes(stream.replicationStreamId).countDocuments()).toBe(2);
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
  });

  test.each(['expired', 'missing'])('%s upload markers prevent publication', async (condition) => {
    const context = await setup();
    await using factory = context.factory;
    await using writer = context.writer;
    const { sourceTable, objectStorage, db, stream, definition } = context;
    const put = objectStorage.put.bind(objectStorage);
    vi.spyOn(objectStorage, 'put').mockImplementationOnce(async (...args) => {
      await put(...args);
      if (condition === 'missing') {
        await db.pendingObjectStorageDeletes(stream.replicationStreamId).deleteMany({});
      }
    });
    const expired =
      condition === 'expired'
        ? vi.spyOn(ObjectStorageLifecycle.prototype, 'canPublish').mockReturnValue(false)
        : undefined;
    try {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'row', description: 'row' },
        afterReplicaId: test_utils.rid('row')
      });
      await expect(writer.commit('1/2')).rejects.toThrow(condition === 'expired' ? 'expired' : 'Missing');
      expect(await db.bucketData(stream.replicationStreamId, definition).countDocuments()).toBe(0);
      expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    } finally {
      expired?.mockRestore();
    }
  });

  test('large parameter rows seal a group before the payload target', async () => {
    const context = await setup(`bucket_definitions:
  by_owner:
    parameters: SELECT description AS owner FROM items WHERE id = request.user_id()
    data:
      - SELECT id FROM items WHERE id = bucket.owner
`);
    await using factory = context.factory;
    await using writer = context.writer;
    const { sourceTable } = context;
    const publications = vi.spyOn(PersistedBatchV3.prototype, 'prepare');
    try {
      // The input block is only a few rows, but its BSON parameter results are
      // large enough to require publication independently of the total byte target.
      for (let i = 0; i < 10; i++) {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `${i}`, description: 'x'.repeat(1024 * 1024) },
          afterReplicaId: test_utils.rid(`${i}`)
        });
      }
      expect(publications).toHaveBeenCalled();
      await writer.commit('1/2');
      expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(20n);
    } finally {
      publications.mockRestore();
    }
  });
  test('high fanout splits publications without losing bucket operations', async () => {
    const context = await setup(`bucket_definitions:
  by_owner:
    accept_potentially_dangerous_queries: true
    parameters: SELECT value AS owner FROM json_each(request.parameter('owners'))
    data:
      - SELECT id FROM items WHERE bucket.owner IN items.owners
`);
    await using factory = context.factory;
    await using writer = context.writer;
    const { sourceTable, bucketStorage, db } = context;
    const owners = JSON.stringify(Array.from({ length: 100 }, (_, i) => `owner-${i}`));
    const publications = vi.spyOn(PersistedBatchV3.prototype, 'prepare');
    try {
      for (let i = 0; i < 900; i++) {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `${i}`, owners },
          afterReplicaId: test_utils.rid(`${i}`)
        });
      }
      await writer.commit('1/2');
      // prepare() is also called by flush(), so compare distinct batch objects.
      expect(new Set(publications.mock.instances).size).toBeGreaterThan(1);
      const buckets = await db.bucketState(bucketStorage.replicationStreamId).find({}).toArray();
      expect(buckets).toHaveLength(100);
      for (const bucket of buckets) {
        expect(bucket.bucket_stats.count).toBe(900);
      }
      expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(90000n);
    } finally {
      publications.mockRestore();
    }
  });
});
