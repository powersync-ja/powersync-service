import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test, vi } from 'vitest';
import { MongoWriteBatch } from '../../src/storage/implementation/MongoWriteBatch.js';
import { MongoBucketBatchV3 } from '../../src/storage/implementation/v3/MongoBucketBatchV3.js';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { ObjectStorageLifecycle } from '../../src/storage/implementation/v3/object-storage/ObjectStorageLifecycle.js';
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

async function save(writer: storage.BucketStorageBatch, table: storage.SourceTable, id: string) {
  await writer.save({
    sourceTable: table,
    tag: storage.SaveOperationTag.UPDATE,
    after: { id, description: id },
    afterReplicaId: test_utils.rid(id)
  });
}

async function open(version: number, mode = 'disabled') {
  const objectStorage = new MemoryObjectStorage();
  const factoryGen = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    monitorCommands: true,
    objectStorage: mode === 'disabled' ? undefined : objectStorage,
    inlineThresholdBytes: mode === 'inline' ? 16 * 1024 : 0
  });
  const factory = await factoryGen.factory();
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(CONFIG, { storageVersion: version }));
  const bucketStorage = (await getTestStorage(factory, stream)) as MongoSyncBucketStorageV3;
  const writer = (await bucketStorage.createWriter(test_utils.BATCH_OPTIONS)) as MongoBucketBatchV3;
  const table = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
  await writer.markAllSnapshotDone('1/1');
  // Reserve IDs, activate the config and finish the metadata barrier before
  // measuring steady-state publication. The second commit seeds a confirmed head.
  await save(writer, table, 'A');
  await writer.commit('1/1');
  await save(writer, table, 'B');
  await writer.commit('1/2');
  return { factory, factoryGen, stream, bucketStorage, writer, table, objectStorage };
}

function countStreamReads(client: mongo.MongoClient) {
  let count = 0;
  const observe = (event: mongo.CommandStartedEvent) => {
    if (event.commandName === 'find' && event.command.find === 'sync_rules' && event.command.autocommit !== false) {
      count++;
    }
  };
  client.on('commandStarted', observe);
  return {
    get count() {
      return count;
    },
    [Symbol.dispose]: () => client.off('commandStarted', observe)
  };
}

describe.each([3, 4])('publication batching v%s', (version) => {
  test.each([
    { mode: 'disabled', fallback: false },
    { mode: 'inline', fallback: false },
    { mode: 'offloaded', fallback: false },
    { mode: 'disabled', fallback: true },
    { mode: 'inline', fallback: true },
    { mode: 'offloaded', fallback: true }
  ])('measures steady-state wire commands: %j', async ({ mode, fallback }) => {
    const context = await open(version, mode);
    await using factory = context.factory;
    await using writer = context.writer;
    using _bulk = vi
      .spyOn(factory.db, 'createWriteBatch')
      .mockImplementation((session, options) => new MongoWriteBatch(factory.db.client, !fallback, session, options));
    using monitor = new PublicationCommandMonitor(factory.db.client);
    using reads = countStreamReads(factory.db.client);
    using registration = vi.spyOn(ObjectStorageLifecycle.prototype, 'prepareUploads');
    await save(writer, context.table, 'C');
    await writer.commit('1/3');
    expect(reads.count).toBe(0);
    expect(registration).toHaveBeenCalledTimes(mode === 'offloaded' ? 1 : 0);
    expect(monitor.summary().replicationAttempts.map((attempt) => attempt.commands)).toEqual([
      fallback
        ? {
            findAndModify: 1,
            // Offloaded storage also updates the writer's usage document.
            update: mode === 'offloaded' ? 5 : 4,
            insert: 1,
            ...(mode === 'offloaded' ? { delete: 1 } : {}),
            commitTransaction: 1
          }
        : { findAndModify: 1, bulkWrite: 1, commitTransaction: 1 }
    ]);
    const checkpoint = await context.bucketStorage.getCheckpoint();
    expect(checkpoint).toMatchObject({ checkpoint: 3n, lsn: '1/3' });
  });

  test('shares confirmed heads between writers without retaining writer-local progress', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using first = context.writer;
    await using second = (await context.bucketStorage.createWriter(test_utils.BATCH_OPTIONS)) as MongoBucketBatchV3;
    using reads = countStreamReads(factory.db.client);
    await save(first, context.table, 'C');
    await first.flush();
    await save(second, context.table, 'D');
    await second.commit('1/3');
    await first.commit('1/4');
    expect(reads.count).toBe(0);
    expect(first.last_flushed_op).toBe(3n);
    expect(second.last_flushed_op).toBe(4n);
    expect(await context.bucketStorage.getCheckpoint()).toMatchObject({ checkpoint: 4n, lsn: '1/4' });
  });

  test('does not cache aborted progress and restores the majority-read path after failure', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using writer = context.writer;
    using reads = countStreamReads(factory.db.client);
    const publish = PersistedBatchV3.prototype.publish;
    using _failure = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      await publish.apply(this, args);
      throw new Error('abort combined data and checkpoint');
    });
    await save(writer, context.table, 'C');
    await expect(writer.commit('1/3')).rejects.toThrow('abort combined data and checkpoint');
    expect(reads.count).toBe(0);
    await writer.commit('1/3');
    expect(reads.count).toBe(1);
    expect(await context.bucketStorage.getCheckpoint()).toMatchObject({ checkpoint: 4n, lsn: '1/3' });
  });

  test.each(['resume', 'snapshot', 'resolve'])('drops the head hint across a %s metadata barrier', async (barrier) => {
    const context = await open(version);
    await using factory = context.factory;
    await using writer = context.writer;
    if (barrier === 'resume') {
      await writer.setResumeLsn('1/2');
    } else if (barrier === 'snapshot') {
      await writer.markAllSnapshotDone('1/1');
    } else {
      await test_utils.resolveTestTable(writer, 'items', ['id'], context.factoryGen, 1);
    }
    using reads = countStreamReads(factory.db.client);
    await save(writer, context.table, 'C');
    await writer.commit('1/3');
    expect(reads.count).toBe(1);
    await save(writer, context.table, 'D');
    await writer.commit('1/4');
    expect(reads.count).toBe(1);
  });

  test.each(['disabled', 'offloaded'])('rebuilds a stale head hint using a fresh read (%s)', async (mode) => {
    const context = await open(version, mode);
    await using factory = context.factory;
    await using writer = context.writer;
    await factory.db.sync_rules.updateOne(
      { _id: context.stream.replicationStreamId },
      { $set: { last_persisted_op: 10n } },
      { writeConcern: { w: 'majority' } }
    );
    using reads = countStreamReads(factory.db.client);
    using put = vi.spyOn(context.objectStorage, 'put');
    await save(writer, context.table, 'C');
    await writer.commit('1/3');
    expect(reads.count).toBe(1);
    expect(writer.last_flushed_op).toBe(11n);
    expect(put).toHaveBeenCalledTimes(mode === 'offloaded' ? 2 : 0);
    await save(writer, context.table, 'D');
    await writer.commit('1/4');
    expect(reads.count).toBe(1);
    expect(writer.last_flushed_op).toBe(12n);
  });

  test('validates live configs from the fence even when preparation used a head hint', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using writer = context.writer;
    await factory.db.sync_rules.updateOne(
      { _id: context.stream.replicationStreamId },
      { $set: { 'sync_configs.$[].state': storage.SyncRuleState.STOP } },
      { writeConcern: { w: 'majority' } }
    );
    using reads = countStreamReads(factory.db.client);
    await save(writer, context.table, 'C');
    await expect(writer.flush()).rejects.toThrow('reload');
    expect(reads.count).toBe(0);
    expect(writer.last_flushed_op).toBe(2n);
    await factory.db.sync_rules.updateOne(
      { _id: context.stream.replicationStreamId },
      { $set: { 'sync_configs.$[].state': storage.SyncRuleState.ACTIVE } },
      { writeConcern: { w: 'majority' } }
    );
    await writer.commit('1/3');
    expect(reads.count).toBe(1);
  });

  test('never carries a confirmed head hint into a replacement lease', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using previous = context.writer;
    await test_utils.releaseTestStorageLease(factory, context.stream.replicationStreamId);
    const stream = (await factory.getReplicatingReplicationStreams())[0];
    const restarted = await getTestStorage(factory, stream);
    await using next = await restarted.createWriter(test_utils.BATCH_OPTIONS);
    const table = await test_utils.resolveTestTable(next, 'items', ['id'], context.factoryGen, 1);
    using reads = countStreamReads(factory.db.client);
    await save(next, table, 'C');
    await next.commit('1/3');
    expect(reads.count).toBe(1);
    await expect(previous.commit('1/4')).rejects.toThrow('Replication lock released');
  });

  test('measures checked-bulk result cursors for 1000 unique rows', async () => {
    const context = await open(version, 'offloaded');
    await using factory = context.factory;
    await using writer = context.writer;
    using monitor = new PublicationCommandMonitor(factory.db.client);
    using reads = countStreamReads(factory.db.client);
    using put = vi.spyOn(context.objectStorage, 'put');
    for (let i = 0; i < 1000; i++) {
      await save(writer, context.table, `row${i}`);
    }
    await writer.commit('1/3');
    expect(reads.count).toBe(0);
    expect(put).toHaveBeenCalledTimes(1);
    expect(monitor.summary().replicationAttempts.map((attempt) => attempt.commands)).toEqual([
      { findAndModify: 1, bulkWrite: 1, commitTransaction: 1 }
    ]);
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(1002n);
  });
});
