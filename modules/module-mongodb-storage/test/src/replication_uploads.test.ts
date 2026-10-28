import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test, vi } from 'vitest';
import { MongoWriteBatch } from '../../src/storage/implementation/MongoWriteBatch.js';
import { MongoBucketBatchV3 } from '../../src/storage/implementation/v3/MongoBucketBatchV3.js';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import {
  OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS,
  ObjectStorageLifecycle
} from '../../src/storage/implementation/v3/object-storage/ObjectStorageLifecycle.js';
import { ObjectStorageUsage } from '../../src/storage/implementation/v3/object-storage/ObjectStorageUsage.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';
import { PublicationCommandMonitor } from './helpers/PublicationCommandMonitor.js';

const CONFIG = `
config:
  edition: 3
streams:
  items:
    query: SELECT * FROM items
`;

async function open(version: number, options: Partial<storage.CreateWriterOptions> = {}) {
  const objectStorage = new MemoryObjectStorage();
  const factoryGen = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    monitorCommands: true,
    objectStorage,
    inlineThresholdBytes: 0
  });
  const factory = await factoryGen.factory();
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(CONFIG, { storageVersion: version }));
  const bucketStorage = (await getTestStorage(factory, stream)) as MongoSyncBucketStorageV3;
  const writer = (await bucketStorage.createWriter({ ...test_utils.BATCH_OPTIONS, ...options })) as MongoBucketBatchV3;
  const table = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
  await writer.markAllSnapshotDone('1/1');
  const db = bucketStorage.db;
  const markers = db.pendingObjectStorageDeletes(stream.replicationStreamId);
  // The generic test factory clears stream data but retains old outbox
  // collections; reset this test's outbox as well when stream IDs are reused.
  await markers.deleteMany({});
  const lifecycle = new ObjectStorageLifecycle(db, stream.replicationStreamId, objectStorage);
  const data = db.bucketData(
    stream.replicationStreamId,
    stream.syncConfigContent[0].mapping.allBucketDefinitionIds()[0]
  );
  const records = db.sourceRecords(stream.replicationStreamId, table.id as bson.ObjectId);
  await writer.save({
    sourceTable: table,
    tag: storage.SaveOperationTag.INSERT,
    after: { id: 'A', description: 'offloaded' },
    afterReplicaId: test_utils.rid('A')
  });
  return { factory, stream, bucketStorage, writer, table, db, objectStorage, markers, lifecycle, data, records };
}

describe.each([3, 4])('replication uploads v%s', (version) => {
  test('registers once with majority acknowledgement before PUT and reuses uploads on transaction retry', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using writer = context.writer;
    const { markers, objectStorage, db } = context;
    using monitor = new PublicationCommandMonitor(factory.db.client);
    using registered = vi.spyOn(ObjectStorageLifecycle.prototype, 'prepareUploads');
    const put = objectStorage.put.bind(objectStorage);
    using uploading = vi.spyOn(objectStorage, 'put').mockImplementation(async (...args) => {
      expect(monitor.activeReplicationAttempts).toHaveLength(0);
      expect(await markers.findOne({ path: args[0] })).toMatchObject({ path: args[0], delete_after: expect.any(Date) });
      await put(...args);
    });
    const registrations: { majority: boolean; transactional: boolean }[] = [];
    const command = (event: mongo.CommandStartedEvent) => {
      if (event.commandName === 'insert' && event.command.insert === markers.collectionName) {
        registrations.push({
          majority: event.command.writeConcern?.w === 'majority',
          transactional: event.command.autocommit === false
        });
      }
    };
    db.client.on('commandStarted', command);
    using _listener = { [Symbol.dispose]: () => db.client.off('commandStarted', command) };
    const publish = PersistedBatchV3.prototype.publish;
    using publishing = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      await publish.apply(this, args);
      throw new mongo.MongoServerError({
        code: 112,
        message: 'publication conflict',
        errorLabels: ['TransientTransactionError']
      });
    });
    await writer.commit('1/2');
    expect(publishing).toHaveBeenCalledTimes(2);
    expect(uploading).toHaveBeenCalledTimes(1);
    expect(registered).toHaveBeenCalledTimes(1);
    expect(registrations).toEqual([{ majority: true, transactional: false }]);
    expect(await markers.countDocuments()).toBe(0);
    expect(await context.data.countDocuments()).toBe(1);
    const [usage] = await ObjectStorageUsage.readAllDefinitionUsage(db);
    expect(usage.active_bytes).toBe(BigInt([...objectStorage.store.values()][0].data.byteLength));
    // The first commit also activates the config in its own fenced transaction.
    expect(monitor.replicationAttempts.map((attempt) => attempt.outcome)).toEqual([
      'aborted',
      'committed',
      'committed'
    ]);
  });

  test('does not PUT after ambiguous marker registration', async () => {
    const context = await open(version);
    await using _factory = context.factory;
    await using writer = context.writer;
    const prepare = ObjectStorageLifecycle.prototype.prepareUploads;
    const failure = new Error('lost marker registration response');
    using _registration = vi
      .spyOn(ObjectStorageLifecycle.prototype, 'prepareUploads')
      .mockImplementationOnce(async function (this: ObjectStorageLifecycle, ...args) {
        await prepare.apply(this, args);
        throw failure;
      });
    using put = vi.spyOn(context.objectStorage, 'put');
    await expect(writer.commit('1/2')).rejects.toThrow(failure);
    expect(put).not.toHaveBeenCalled();
    expect(await context.markers.countDocuments()).toBe(1);
    expect(await context.records.countDocuments()).toBe(0);
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    // A later explicit retry reevaluates the retained input using a fresh path.
    await writer.commit('1/2');
    expect(put).toHaveBeenCalledTimes(1);
    expect(await context.markers.countDocuments()).toBe(1);
    expect(await context.records.countDocuments()).toBe(1);
  });

  test('lost PUT responses retain orphan markers, and cleanup retries deletion failures', async () => {
    const context = await open(version);
    await using _factory = context.factory;
    await using writer = context.writer;
    const put = context.objectStorage.put.bind(context.objectStorage);
    using _put = vi.spyOn(context.objectStorage, 'put').mockImplementationOnce(async (...args) => {
      await put(...args);
      throw new Error('lost PUT response');
    });
    await expect(writer.commit('1/2')).rejects.toThrow('lost PUT response');
    expect(context.objectStorage.store.size).toBe(1);
    expect(await context.markers.countDocuments()).toBe(1);
    expect(await context.data.countDocuments()).toBe(0);
    expect(await ObjectStorageUsage.readAllDefinitionUsage(context.db)).toEqual([]);
    await context.lifecycle.cleanup(logger);
    expect(context.objectStorage.store.size).toBe(1);
    await context.markers.updateMany({}, { $set: { delete_after: new Date(0) } });
    using deleting = vi.spyOn(context.objectStorage, 'delete').mockRejectedValueOnce(new Error('DELETE failed'));
    await context.lifecycle.cleanup(logger);
    expect(await context.markers.countDocuments()).toBe(1);
    expect(context.objectStorage.store.size).toBe(1);
    await context.lifecycle.cleanup(logger);
    expect(deleting).toHaveBeenCalledTimes(2);
    expect(await context.markers.countDocuments()).toBe(0);
    expect(context.objectStorage.store.size).toBe(0);
  });

  test('rejects an uploaded generation whose assigned ID does not advance the persisted head', async () => {
    const context = await open(version);
    await using _factory = context.factory;
    await using writer = context.writer;
    const put = context.objectStorage.put.bind(context.objectStorage);
    using uploads = vi.spyOn(context.objectStorage, 'put').mockImplementationOnce(async (...args) => {
      await put(...args);
      await context.db.sync_rules.updateOne(
        { _id: context.stream.replicationStreamId },
        { $set: { last_persisted_op: 10n } },
        { writeConcern: { w: 'majority' } }
      );
    });
    await expect(writer.commit('1/2')).rejects.toThrow('does not advance persisted stream head');
    expect(uploads).toHaveBeenCalledTimes(1);
    const [abandoned] = uploads.mock.calls.map(([path]) => path);
    expect(await context.markers.find({}).toArray()).toMatchObject([{ path: abandoned }]);
    expect(await context.data.find({}).toArray()).toEqual([]);
    expect(writer.last_flushed_op).toBeNull();
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    expect(await context.db.sync_rules.findOne({ _id: context.stream.replicationStreamId })).toMatchObject({
      last_persisted_op: 10n
    });
    await context.markers.updateMany({}, { $set: { delete_after: new Date(0) } });
    await context.lifecycle.cleanup(logger);
    expect(context.objectStorage.store.size).toBe(0);
    expect(await context.markers.countDocuments()).toBe(0);
    expect(await ObjectStorageUsage.readAllDefinitionUsage(context.db)).toEqual([]);
  });

  test('stopped-stream termination clears abandoned replication objects and their markers', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using writer = context.writer;
    const put = context.objectStorage.put.bind(context.objectStorage);
    using _put = vi.spyOn(context.objectStorage, 'put').mockImplementationOnce(async (...args) => {
      await put(...args);
      throw new Error('lost PUT response');
    });
    await expect(writer.flush()).rejects.toThrow('lost PUT response');
    expect(context.objectStorage.store.size).toBe(1);
    await factory.db.sync_rules.updateOne(
      { _id: context.stream.replicationStreamId },
      { $set: { state: storage.SyncRuleState.STOP, 'sync_configs.$[].state': storage.SyncRuleState.STOP } }
    );
    // Supported termination runs after the replication job has stopped/drained
    // and released its lease. It removes the entire prefix, including orphans.
    await test_utils.releaseTestStorageLease(factory, context.stream.replicationStreamId);
    await context.bucketStorage.terminate();
    expect(context.objectStorage.store.size).toBe(0);
    expect(await context.markers.countDocuments()).toBe(0);
    expect(await factory.db.sync_rules.findOne({ _id: context.stream.replicationStreamId })).toMatchObject({
      state: storage.SyncRuleState.TERMINATED
    });
  });

  test.each([false, true])(
    'missing markers roll back references, usage and progress (fallback: %s)',
    async (fallback) => {
      const context = await open(version);
      await using factory = context.factory;
      await using writer = context.writer;
      const createWriteBatch = factory.db.createWriteBatch.bind(factory.db);
      using _bulk = vi
        .spyOn(factory.db, 'createWriteBatch')
        .mockImplementation((session, options) =>
          fallback
            ? new MongoWriteBatch(factory.db.client, false, session, options)
            : createWriteBatch(session, options)
        );
      const put = context.objectStorage.put.bind(context.objectStorage);
      using _put = vi.spyOn(context.objectStorage, 'put').mockImplementationOnce(async (...args) => {
        await put(...args);
        await context.markers.deleteMany({});
      });
      await expect(writer.commit('1/2')).rejects.toThrow('Missing object storage publication markers');
      expect(await context.data.countDocuments()).toBe(0);
      expect(await context.records.countDocuments()).toBe(0);
      expect(await ObjectStorageUsage.readAllDefinitionUsage(context.db)).toEqual([]);
      expect(writer.last_flushed_op).toBeNull();
      expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
      await writer.commit('1/2');
      expect(await context.data.countDocuments()).toBe(1);
    }
  );

  test('rechecks expiry on a retry and leaves the generation for cleanup', async () => {
    const context = await open(version);
    await using _factory = context.factory;
    await using writer = context.writer;
    using registered = vi.spyOn(ObjectStorageLifecycle.prototype, 'prepareUploads');
    using put = vi.spyOn(context.objectStorage, 'put');
    const publish = PersistedBatchV3.prototype.publish;
    let attempts = 0;
    using _publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementation(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      attempts++;
      if (attempts === 2) {
        const uploads = await registered.mock.results[0].value;
        uploads[0].deleteAfter = new Date(Date.now() + OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS);
      }
      await publish.apply(this, args);
      throw new mongo.MongoServerError({
        code: 112,
        message: 'publication conflict',
        errorLabels: ['TransientTransactionError']
      });
    });
    await expect(writer.commit('1/2')).rejects.toThrow('publication lease expired');
    expect(attempts).toBe(2);
    expect(put).toHaveBeenCalledTimes(1);
    expect(registered).toHaveBeenCalledTimes(1);
    expect(await context.markers.countDocuments()).toBe(1);
    expect(await context.data.countDocuments()).toBe(0);
    expect(await ObjectStorageUsage.readAllDefinitionUsage(context.db)).toEqual([]);
  });

  test.each([false, true])(
    'terminal commit uncertainty preserves the correct marker state (committed: %s)',
    async (committed) => {
      const context = await open(version);
      await using _factory = context.factory;
      await using writer = context.writer;
      await using other = await context.bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const failure = new mongo.MongoServerError({
        message: 'commit outcome unknown',
        errorLabels: ['UnknownTransactionCommitResult']
      });
      const session = writer.session;
      const transact = session.withTransaction.bind(session);
      using _transaction = vi.spyOn(session, 'withTransaction').mockImplementationOnce(async (...args) => {
        if (committed) {
          await transact(...args);
          throw failure;
        }
        return transact(async (...callbackArgs) => {
          await args[0](...callbackArgs);
          throw failure;
        }, args[1]);
      });
      await expect(writer.commit('1/2')).rejects.toThrow(failure);
      expect(await context.markers.countDocuments()).toBe(committed ? 0 : 1);
      expect(await context.data.countDocuments()).toBe(committed ? 1 : 0);
      expect(writer.last_flushed_op).toBeNull();
      await expect(other.setResumeLsn('1/3')).rejects.toThrow('restart replication');
      await context.markers.updateMany({}, { $set: { delete_after: new Date(0) } });
      await context.lifecycle.cleanup(logger);
      expect(context.objectStorage.store.size).toBe(committed ? 1 : 0);
    }
  );

  test('truncate PUTs complete before its transaction and publish their markers atomically', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using writer = context.writer;
    await writer.commit('1/2');
    using monitor = new PublicationCommandMonitor(factory.db.client);
    const put = context.objectStorage.put.bind(context.objectStorage);
    using uploading = vi.spyOn(context.objectStorage, 'put').mockImplementation(async (...args) => {
      expect(monitor.activeReplicationAttempts).toHaveLength(0);
      expect(await context.markers.countDocuments({ path: args[0] })).toBe(1);
      await put(...args);
    });
    await writer.truncate([context.table]);
    expect(uploading).toHaveBeenCalledTimes(1);
    expect(await context.markers.countDocuments()).toBe(0);
    expect(await context.records.countDocuments()).toBe(0);
    expect(await context.data.countDocuments()).toBe(2);
  });

  test('cancels queued PUTs and drains active requests before returning', async () => {
    const controller = new AbortController();
    const context = await open(version, { signal: controller.signal });
    await using _factory = context.factory;
    await using writer = context.writer;
    // Enough data for separate chunks, within a single bounded publication.
    await writer.save({
      sourceTable: context.table,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'x'.repeat(1_100_000) },
      afterReplicaId: test_utils.rid('B')
    });
    const active = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const queued = Promise.withResolvers<void>();
    let remoteCompletions = 0;
    using put = vi
      .spyOn(context.objectStorage, 'put')
      .mockImplementationOnce(async (path, data, metadata) => {
        active.resolve();
        await release.promise;
        // An accepted request can complete after local cancellation. Its marker
        // remains protected for an hour; cancellation must not delete it early.
        context.objectStorage.store.set(path, { data, metadata });
        remoteCompletions++;
      })
      .mockImplementationOnce(async (_path, _data, _metadata, options) => {
        queued.resolve();
        await active.promise;
        if (!options?.signal?.aborted) {
          await new Promise<void>((resolve) =>
            options!.signal!.addEventListener('abort', () => resolve(), { once: true })
          );
        }
        options?.signal?.throwIfAborted();
      });
    const failed = expect(writer.commit('1/2')).rejects.toThrow('cancel uploads');
    await queued.promise;
    controller.abort(new Error('cancel uploads'));
    expect(remoteCompletions).toBe(0);
    release.resolve();
    await failed;
    expect(remoteCompletions).toBe(1);
    expect(put).toHaveBeenCalledTimes(2);
    expect(await context.markers.countDocuments()).toBe(2);
    await context.markers.updateMany({}, { $set: { delete_after: new Date(0) } });
    await context.lifecycle.cleanup(logger);
    expect(context.objectStorage.store.size).toBe(0);
    expect(await context.markers.countDocuments()).toBe(0);
    expect(await context.records.countDocuments()).toBe(0);
  });
});
