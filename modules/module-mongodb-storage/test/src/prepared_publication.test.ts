import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test, vi } from 'vitest';
import { PreparedPublication } from '../../src/storage/implementation/common/PreparedPublication.js';
import { MongoWriteBatch } from '../../src/storage/implementation/MongoWriteBatch.js';
import { OperationBatch } from '../../src/storage/implementation/OperationBatch.js';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { ObjectStorageUsage } from '../../src/storage/implementation/v3/object-storage/ObjectStorageUsage.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { SourceRecordStoreV3 } from '../../src/storage/implementation/v3/SourceRecordStoreV3.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';

const SYNC_CONFIG = `
config:
  edition: 3
streams:
  items:
    query: SELECT * FROM items
`;

async function openStream(
  storageVersion: number,
  objectStorage: MemoryObjectStorage | undefined,
  inlineThresholdBytes = 0,
  syncConfig = SYNC_CONFIG
) {
  const factoryGen = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    objectStorage,
    inlineThresholdBytes
  });
  const factory = await factoryGen.factory();
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(syncConfig, { storageVersion }));
  const bucketStorage = (await getTestStorage(factory, stream)) as MongoSyncBucketStorageV3;
  const writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  const table = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
  await writer.markAllSnapshotDone('1/1');
  return { factory, factoryGen, stream, bucketStorage, writer, table };
}

describe.each([3, 4])('sealed publication v%s', (storageVersion) => {
  test.each([
    { enabled: false, offloaded: false, fallback: false },
    { enabled: false, offloaded: false, fallback: true },
    { enabled: true, offloaded: false, fallback: false },
    { enabled: true, offloaded: true, fallback: false },
    { enabled: true, offloaded: false, fallback: true },
    { enabled: true, offloaded: true, fallback: true }
  ])('replays writes after rollback without changing payloads: %j', async ({ enabled, offloaded, fallback }) => {
    const objectStorage = new MemoryObjectStorage();
    const context = await openStream(storageVersion, enabled ? objectStorage : undefined, offloaded ? 0 : 16 * 1024);
    await using factory = context.factory;
    await using writer = context.writer;
    const { table, bucketStorage, stream } = context;
    const db = bucketStorage.db;
    const createWriteBatch = factory.db.createWriteBatch.bind(factory.db);
    using _bulkMode = vi
      .spyOn(factory.db, 'createWriteBatch')
      .mockImplementation((session, options) =>
        fallback ? new MongoWriteBatch(factory.db.client, false, session, options) : createWriteBatch(session, options)
      );
    using put = vi.spyOn(objectStorage, 'put');
    let publication: PreparedPublication | undefined;
    let builder: PersistedBatchV3 | undefined;
    const aborted = new Error('abort after data writes');
    const originalPublish = PersistedBatchV3.prototype.publish;
    using _flush = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      builder = this;
      publication = this;
      const ready = publication.preparePayloads();
      expect(this.preparePayloads()).toBe(ready);
      await ready;
      await originalPublish.apply(this, args);
      throw aborted;
    });

    // Repeated records, partial updates, a source ID change and deletion must
    // retain their history and final membership when a sealed batch is replayed.
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'first' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: undefined, description: 'second' },
      beforeReplicaId: test_utils.rid('A'),
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'B', description: undefined },
      beforeReplicaId: test_utils.rid('A'),
      afterReplicaId: test_utils.rid('B')
    });
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.DELETE,
      beforeReplicaId: test_utils.rid('B')
    });
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'C', description: 'final' },
      afterReplicaId: test_utils.rid('C')
    });
    await expect(writer.commit('1/2')).rejects.toThrow(aborted);
    expect(publication).toBeDefined();
    expect(publication).toBe(builder);
    expect(writer.last_flushed_op).toBeNull();
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    const definitionId = stream.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const data = db.bucketData(stream.replicationStreamId, definitionId);
    expect(await data.countDocuments()).toBe(0);
    expect(await db.sourceRecords(stream.replicationStreamId, table.id as bson.ObjectId).countDocuments()).toBe(0);
    expect(await ObjectStorageUsage.readAllDefinitionUsage(db)).toEqual([]);
    const markers = db.pendingObjectStorageDeletes(stream.replicationStreamId);
    const markerFilter = { path: { $in: [...objectStorage.store.keys()] } };
    expect(await markers.countDocuments(markerFilter)).toBe(offloaded ? 1 : 0);
    expect(put).toHaveBeenCalledTimes(offloaded ? 1 : 0);
    const paths = [...objectStorage.store.keys()];
    const bytes = paths.map((path) => Buffer.from(objectStorage.store.get(path)!.data));

    // Mutators must not reclaim ownership or clear sealed writes.
    expect(() => builder!.seal()).toThrow('sealed');
    expect(() => builder!.hardDeleteCurrentData(table.id as bson.ObjectId, test_utils.rid('C'))).toThrow('sealed');
    await db.client.withSession(async (session) => {
      let attempts = 0;
      await publication!.preparePayloads();
      await session.withTransaction(async () => {
        expect(await publication!.publish(session)).toEqual({
          bucketDataCount: 6,
          parameterDataCount: 0,
          currentDataCount: 3,
          flushedAny: true
        });
        attempts += 1;
        if (attempts === 1) {
          throw new mongo.MongoServerError({
            message: 'retry sealed writes',
            code: 112,
            errorLabels: ['TransientTransactionError']
          });
        }
      });
      expect(attempts).toBe(2);
    });

    expect(put).toHaveBeenCalledTimes(offloaded ? 1 : 0);
    expect([...objectStorage.store.keys()]).toEqual(paths);
    expect(await markers.countDocuments(markerFilter)).toBe(0);
    expect(paths.map((path) => Buffer.from(objectStorage.store.get(path)!.data))).toEqual(bytes);
    const documents = await data.find({}).toArray();
    expect(documents).toHaveLength(1);
    const operations = offloaded
      ? bson.deserialize(
          objectStorage.store.get(documents[0].storage_ref!.path)!.data,
          storage.BSON_DESERIALIZE_INTERNAL_OPTIONS
        ).ops
      : documents[0].ops;
    expect(operations).toMatchObject([
      { o: 1n, op: 'PUT', row_id: 'A', data: '{"id":"A","description":"first"}' },
      { o: 2n, op: 'PUT', row_id: 'A', data: '{"id":"A","description":"second"}' },
      { o: 3n, op: 'REMOVE', row_id: 'A' },
      { o: 4n, op: 'PUT', row_id: 'B', data: '{"id":"B","description":"second"}' },
      { o: 6n, op: 'REMOVE', row_id: 'B' },
      { o: 8n, op: 'PUT', row_id: 'C', data: '{"id":"C","description":"final"}' }
    ]);
    expect(await db.bucketState(stream.replicationStreamId).findOne({ '_id.d': definitionId })).toMatchObject({
      last_op: 8n,
      bucket_stats: { count: 6, chunks: 1, bytes: BigInt(documents[0].size) }
    });
    const records = await db
      .sourceRecords(stream.replicationStreamId, table.id as bson.ObjectId)
      .find({})
      .toArray();
    expect(records).toHaveLength(3);
    for (const [id, op] of [
      ['A', 5n],
      ['B', 7n]
    ] as const) {
      expect(records.find((record) => record.pending_delete === op)).toMatchObject({
        _id: test_utils.rid(id),
        data: null,
        buckets: [],
        lookups: [],
        pending_delete: op
      });
    }
    const finalRecord = records.find((record) => record.data != null);
    expect(finalRecord!._id).toEqual(test_utils.rid('C'));
    expect(bson.deserialize(finalRecord!.data!.buffer)).toEqual({ id: 'C', description: 'final' });
    expect(finalRecord!.buckets).toHaveLength(1);
    const usage = await ObjectStorageUsage.readAllDefinitionUsage(db);
    expect(usage).toEqual(
      offloaded
        ? [
            {
              replication_stream_id: stream.replicationStreamId,
              definition_id: definitionId,
              active_bytes: BigInt(bytes[0].byteLength)
            }
          ]
        : []
    );
    // Publishing a payload alone never advances the scheduling envelope.
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    expect(writer.last_flushed_op).toBeNull();
  });

  test.each(['disabled', 'inline', 'offloaded'])(
    'writer uses sealed publications with %s payload storage',
    async (mode) => {
      const objectStorage = new MemoryObjectStorage();
      const context = await openStream(
        storageVersion,
        mode === 'disabled' ? undefined : objectStorage,
        mode === 'inline' ? 16 * 1024 : 0
      );
      await using _factory = context.factory;
      await using writer = context.writer;
      using seal = vi.spyOn(PersistedBatchV3.prototype, 'seal');
      using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish');
      using put = vi.spyOn(objectStorage, 'put');
      await writer.save({
        sourceTable: context.table,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'A', description: 'inline or offloaded' },
        afterReplicaId: test_utils.rid('A')
      });
      await writer.commit('1/2');
      expect(seal).toHaveBeenCalledTimes(1);
      expect(publish).toHaveBeenCalledTimes(1);
      expect(put).toHaveBeenCalledTimes(mode === 'offloaded' ? 1 : 0);
      expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(1n);
      const definitionId = context.stream.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
      const documents = await context.bucketStorage.db
        .bucketData(context.stream.replicationStreamId, definitionId)
        .find({})
        .toArray();
      expect(documents).toHaveLength(1);
      if (mode === 'offloaded') {
        expect(documents[0].storage_ref).toBeDefined();
        expect(documents[0].ops).toBeUndefined();
      } else {
        expect(documents[0].storage_ref).toBeUndefined();
        expect(documents[0].ops).toMatchObject([{ op: 'PUT', row_id: 'A' }]);
      }
    }
  );

  test.each([false, true])(
    'replays parameter history and final membership after an abort (object storage: %s)',
    async (enabled) => {
      const objectStorage = new MemoryObjectStorage();
      const context = await openStream(
        storageVersion,
        enabled ? objectStorage : undefined,
        0,
        `
config:
  edition: 3
streams:
  items:
    query: SELECT * FROM items WHERE id IN (SELECT item_id FROM memberships WHERE user_id = auth.user_id())
`
      );
      await using _factory = context.factory;
      await using writer = context.writer;
      const membership = await test_utils.resolveTestTable(writer, 'memberships', ['id'], context.factoryGen, 2);
      let publication: PreparedPublication | undefined;
      const abort = new Error('abort parameter writes');
      const originalPublish = PersistedBatchV3.prototype.publish;
      using _flush = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
        this: PersistedBatchV3,
        ...args
      ) {
        publication = this;
        await originalPublish.apply(this, args);
        throw abort;
      });
      for (const userId of ['old', 'new']) {
        await writer.save({
          sourceTable: membership,
          tag: storage.SaveOperationTag.UPDATE,
          after: { id: 'member', user_id: userId, item_id: 'item' },
          afterReplicaId: test_utils.rid('member')
        });
      }
      await expect(writer.commit('1/2')).rejects.toThrow(abort);
      const { db } = context.bucketStorage;
      const streamId = context.stream.replicationStreamId;
      const [indexId] = context.stream.syncConfigContent[0].mapping.allParameterIndexIds();
      expect(indexId).toBeDefined();
      const parameters = db.parameterIndex(streamId, indexId);
      expect(await parameters.countDocuments()).toBe(0);
      await publication!.preparePayloads();
      await db.client.withSession(async (session) => {
        await session.withTransaction(() => publication!.publish(session));
      });
      const documents = await parameters.find({}).sort({ _id: 1 }).toArray();
      expect(documents.map((doc) => doc._id)).toEqual([1n, 2n, 3n]);
      expect(documents[0].bucket_parameters).not.toEqual([]);
      expect(documents[1].bucket_parameters).toEqual(documents[0].bucket_parameters);
      expect(documents[2].bucket_parameters).toEqual([]);
      expect(documents[2].lookup).toEqual(documents[0].lookup);
      expect(documents[1].lookup).not.toEqual(documents[0].lookup);
      const records = await db
        .sourceRecords(streamId, membership.id as bson.ObjectId)
        .find({})
        .toArray();
      expect(records).toHaveLength(1);
      expect(records[0].lookups).toHaveLength(1);
      expect(records[0].lookups[0].l).toEqual(documents[1].lookup);
      expect(objectStorage.store.size).toBe(0);
    }
  );

  test('overlaps block preparation with two uploads but stops reading at capacity', async () => {
    const objectStorage = new MemoryObjectStorage();
    const context = await openStream(storageVersion, objectStorage);
    await using _factory = context.factory;
    await using writer = context.writer;
    // Force small read blocks without changing production input/batch targets.
    using _blocks = vi.spyOn(OperationBatch.prototype, 'batched').mockImplementation(function* (this: OperationBatch) {
      for (const op of this.batch) {
        yield [op];
      }
    });
    using reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish');
    const started = Array.from({ length: 3 }, () => Promise.withResolvers<void>());
    const release = Array.from({ length: 3 }, () => Promise.withResolvers<void>());
    const put = objectStorage.put.bind(objectStorage);
    let uploads = 0;
    using _put = vi.spyOn(objectStorage, 'put').mockImplementation(async (...args) => {
      const index = uploads++;
      started[index].resolve();
      await release[index].promise;
      return put(...args);
    });
    for (const id of ['A', 'B', 'C']) {
      await writer.save({
        sourceTable: context.table,
        tag: storage.SaveOperationTag.INSERT,
        after: { id, description: id },
        afterReplicaId: test_utils.rid(id)
      });
    }
    const commit = writer.commit('1/2');
    try {
      await started[1].promise;
      expect(reads).toHaveBeenCalledTimes(2);
      expect(publish).not.toHaveBeenCalled();
      expect(writer.last_flushed_op).toBeNull();
      release[0].resolve();
      await started[2].promise;
      expect(reads).toHaveBeenCalledTimes(3);
      expect(publish).not.toHaveBeenCalled();
    } finally {
      for (const gate of release) {
        gate.resolve();
      }
      await commit;
    }
    expect(publish).toHaveBeenCalledTimes(3);
    expect(uploads).toBe(3);
    expect(await context.bucketStorage.getCheckpoint()).toMatchObject({ checkpoint: 3n, lsn: '1/2' });
    const definitionId = context.stream.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const documents = await context.bucketStorage.db
      .bucketData(context.stream.replicationStreamId, definitionId)
      .find({})
      .sort({ first_op: 1 })
      .toArray();
    expect(documents).toHaveLength(3);
    expect(
      documents
        .map(
          (doc) =>
            bson.deserialize(
              objectStorage.store.get(doc.storage_ref!.path)!.data,
              storage.BSON_DESERIALIZE_INTERNAL_OPTIONS
            ).ops[0].row_id
        )
        .sort()
    ).toEqual(['A', 'B', 'C']);
  });

  test('retains partial-row state across eagerly prepared groups', async () => {
    const objectStorage = new MemoryObjectStorage();
    const context = await openStream(storageVersion, objectStorage);
    await using _factory = context.factory;
    await using writer = context.writer;
    using _blocks = vi.spyOn(OperationBatch.prototype, 'batched').mockImplementation(function* (this: OperationBatch) {
      for (const op of this.batch) {
        yield [op];
      }
    });
    for (const after of [
      { id: 'A', description: 'first' },
      { id: undefined, description: 'second' }
    ]) {
      await writer.save({
        sourceTable: context.table,
        tag: storage.SaveOperationTag.UPDATE,
        after,
        afterReplicaId: test_utils.rid('A')
      });
    }
    await writer.commit('1/2');
    const { db } = context.bucketStorage;
    const streamId = context.stream.replicationStreamId;
    const records = await db
      .sourceRecords(streamId, context.table.id as bson.ObjectId)
      .find({})
      .toArray();
    expect(records).toHaveLength(1);
    expect(bson.deserialize(records[0].data!.buffer)).toEqual({ id: 'A', description: 'second' });
    expect(records[0].buckets).toHaveLength(1);
    const definitionId = context.stream.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];
    const documents = await db.bucketData(streamId, definitionId).find({}).toArray();
    const operations = documents
      .flatMap(
        (doc) =>
          bson.deserialize(
            objectStorage.store.get(doc.storage_ref!.path)!.data,
            storage.BSON_DESERIALIZE_INTERNAL_OPTIONS
          ).ops
      )
      .sort((a, b) => Number(a.o - b.o));
    expect(operations).toMatchObject([
      { o: 1n, op: 'PUT', row_id: 'A', data: '{"id":"A","description":"first"}' },
      { o: 2n, op: 'PUT', row_id: 'A', data: '{"id":"A","description":"second"}' }
    ]);
    expect(await context.bucketStorage.getCheckpoint()).toMatchObject({ checkpoint: 2n, lsn: '1/2' });
  });

  test.each(['read', 'upload'])('joins all started groups after a %s failure', async (stage) => {
    const objectStorage = new MemoryObjectStorage();
    const context = await openStream(storageVersion, objectStorage);
    await using _factory = context.factory;
    await using writer = context.writer;
    using _blocks = vi.spyOn(OperationBatch.prototype, 'batched').mockImplementation(function* (this: OperationBatch) {
      for (const op of this.batch) {
        yield [op];
      }
    });
    const started = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const failed = Promise.withResolvers<void>();
    const failure = new Error('failed ' + stage);
    let finished = false;
    const put = objectStorage.put.bind(objectStorage);
    using uploads = vi.spyOn(objectStorage, 'put').mockImplementationOnce(async (...args) => {
      started.resolve();
      await release.promise;
      await put(...args);
      finished = true;
    });
    const load = SourceRecordStoreV3.prototype.loadDocuments;
    let reads = 0;
    using _read = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementation(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      if (++reads === 2 && stage === 'read') {
        await started.promise;
        failed.resolve();
        throw failure;
      }
      return load.apply(this, args);
    });
    if (stage === 'upload') {
      uploads.mockImplementationOnce(async () => {
        await started.promise;
        failed.resolve();
        throw failure;
      });
    }
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish');
    for (const id of ['A', 'B']) {
      await writer.save({
        sourceTable: context.table,
        tag: storage.SaveOperationTag.INSERT,
        after: { id },
        afterReplicaId: test_utils.rid(id)
      });
    }
    let settled = false;
    const commit = writer.commit('1/2').finally(() => {
      settled = true;
    });
    const rejected = expect(commit).rejects.toThrow(failure);
    try {
      await failed.promise;
      expect(settled).toBe(false);
      expect(finished).toBe(false);
      expect(publish).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      await rejected;
    }
    expect(finished).toBe(true);
    expect(writer.last_flushed_op).toBeNull();
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    expect(publish).not.toHaveBeenCalled();
    // The completed first upload remains protected as an orphan, with no
    // source progress or references pointing at this failed prefix.
    expect(
      await context.bucketStorage.db
        .pendingObjectStorageDeletes(context.stream.replicationStreamId)
        .countDocuments({ path: { $in: [...objectStorage.store.keys()] } })
    ).toBe(1);
  });

  test.each([false, true])('joins sibling uploads before rejecting readiness (cancellation: %s)', async (cancelled) => {
    const objectStorage = new MemoryObjectStorage();
    const context = await openStream(storageVersion, objectStorage);
    await using _factory = context.factory;
    await using writer = context.writer;
    let publication: PreparedPublication | undefined;
    const seal = PersistedBatchV3.prototype.seal;
    using _sealSpy = vi.spyOn(PersistedBatchV3.prototype, 'seal').mockImplementation(function (this: PersistedBatchV3) {
      publication = seal.call(this);
      return publication;
    });
    const failure = cancelled ? new DOMException('cancelled upload', 'AbortError') : new Error('failed upload');
    const siblingStarted = Promise.withResolvers<void>();
    const releaseSibling = Promise.withResolvers<void>();
    let siblingFinished = false;
    const put = objectStorage.put.bind(objectStorage);
    using uploadSpy = vi
      .spyOn(objectStorage, 'put')
      .mockRejectedValueOnce(failure)
      .mockImplementationOnce(async (...args) => {
        siblingStarted.resolve();
        await releaseSibling.promise;
        await put(...args);
        siblingFinished = true;
      });
    // A large first row forces two chunks without triggering input auto-flush.
    for (const [id, size] of [
      ['A', 1_100_000],
      ['B', 1000]
    ] as const) {
      await writer.save({
        sourceTable: context.table,
        tag: storage.SaveOperationTag.INSERT,
        after: { id, description: 'x'.repeat(size) },
        afterReplicaId: test_utils.rid(id)
      });
    }
    const commit = writer.commit('1/2');
    const rejected = expect(commit).rejects.toThrow(failure);
    await siblingStarted.promise;
    let readySettled = false;
    const readiness = publication!.preparePayloads().then(
      () => {
        readySettled = true;
      },
      () => {
        readySettled = true;
      }
    );
    await Promise.resolve();
    expect(readySettled).toBe(false);
    releaseSibling.resolve();
    await rejected;
    await readiness;
    expect(siblingFinished).toBe(true);
    expect(uploadSpy).toHaveBeenCalledTimes(2);
    await expect(publication!.preparePayloads()).rejects.toThrow(failure);
    expect(uploadSpy).toHaveBeenCalledTimes(2);
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
  });
});
