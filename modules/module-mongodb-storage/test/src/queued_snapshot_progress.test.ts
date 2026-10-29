import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test, vi } from 'vitest';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';

const CONFIG = `
config:
  edition: 3
streams:
  items:
    query: SELECT * FROM items
`;

async function open(version: number, offloaded = false) {
  const gen = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    objectStorage: offloaded ? new MemoryObjectStorage() : undefined,
    inlineThresholdBytes: 0
  });
  const factory = await gen.factory();
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(CONFIG, { storageVersion: version }));
  const buckets = (await getTestStorage(factory, stream)) as MongoSyncBucketStorageV3;
  const writer = await buckets.createWriter(test_utils.BATCH_OPTIONS);
  const table = await test_utils.resolveTestTable(writer, 'items', ['id'], gen, 1);
  return { factory, gen, stream, buckets, writer, table };
}

function save(writer: storage.BucketStorageBatch, table: storage.SourceTable, id: string) {
  return writer.save({
    sourceTable: table,
    tag: storage.SaveOperationTag.INSERT,
    after: { id, description: id },
    afterReplicaId: id
  });
}

function expectProgress(actual: storage.TableSnapshotStatus | undefined, count: number) {
  expect(actual).toMatchObject({ replicatedCount: count, totalEstimatedCount: 2 });
  expect(Array.from(actual!.lastKey!)).toEqual([count]);
}

function page(count: number): storage.TableSnapshotStatus {
  return { lastKey: new Uint8Array([count]), replicatedCount: count, totalEstimatedCount: 2 };
}

describe.each([3, 4])('queued snapshot progress v%s', (version) => {
  test.each([false, true])(
    'prepares the next page and commits each cursor with its rows (offloaded: %s)',
    async (offloaded) => {
      const c = await open(version, offloaded);
      await using _factory = c.factory;
      await using writer = c.writer;
      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const original = PersistedBatchV3.prototype.publish;
      using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
        this: PersistedBatchV3,
        ...args
      ) {
        entered.resolve();
        await release.promise;
        return original.apply(this, args);
      });
      await save(writer, c.table, 'A');
      const status = page(1);
      const first = await writer.queueTableProgress!(c.table, status);
      await entered.promise;
      let durable = false;
      void first.durable.then(() => {
        durable = true;
      });
      const tables = c.buckets.db.sourceTables(c.stream.replicationStreamId);
      const records = c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId);
      let second: storage.QueuedTableProgress | undefined;
      try {
        status.lastKey![0] = 99;
        status.replicatedCount = 99;
        await save(writer, c.table, 'B');
        second = await writer.queueTableProgress!(c.table, page(2));
        expect(publish).toHaveBeenCalledTimes(1);
        expect(durable).toBe(false);
        expect((await tables.findOne({ _id: c.table.id as bson.ObjectId }))!.snapshot_status).toBeNull();
        expect(await records.countDocuments()).toBe(0);
      } finally {
        release.resolve();
        await first.durable;
        await second?.durable;
      }
      expectProgress((await first.durable).snapshotStatus, 1);
      expectProgress((await second!.durable).snapshotStatus, 2);
      expect(await records.countDocuments()).toBe(2);
      expectProgress((await writer.getSourceTableStatus(c.table))!.snapshotStatus, 2);
      expect(c.table.snapshotStatus).toBeUndefined();
    }
  );

  test('receipt delivery reports durable progress and bounds the next source page', async () => {
    const c = await open(version);
    await using _factory = c.factory;
    await using writer = c.writer;
    const delivered = vi.fn();
    await using progress = new storage.SnapshotProgress(writer, delivered);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const original = PersistedBatchV3.prototype.publish;
    using _publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      entered.resolve();
      await release.promise;
      return original.apply(this, args);
    });
    await save(writer, c.table, 'A');
    await progress.update(c.table, page(1));
    await entered.promise;
    await save(writer, c.table, 'B');
    await progress.update(c.table, page(2));
    let canRead = false;
    const capacity = progress.waitForCapacity().then(() => {
      canRead = true;
    });
    try {
      expect(delivered).not.toHaveBeenCalled();
      expect(canRead).toBe(false);
    } finally {
      release.resolve();
      await capacity;
      await progress.drain();
    }
    expect(delivered).toHaveBeenCalledTimes(2);
    expectProgress(delivered.mock.calls[0][0].snapshotStatus, 1);
    expectProgress(delivered.mock.calls[1][0].snapshotStatus, 2);
  });

  test('receipt failure surfaces on drain when no further source page arrives', async () => {
    const c = await open(version);
    await using _factory = c.factory;
    await using writer = c.writer;
    const delivered = vi.fn();
    const progress = new storage.SnapshotProgress(writer, delivered);
    using _publish = vi
      .spyOn(PersistedBatchV3.prototype, 'publish')
      .mockRejectedValueOnce(new Error('failed last page'));
    await save(writer, c.table, 'A');
    await progress.update(c.table, page(1));
    await expect(progress.drain()).rejects.toThrow('failed last page');
    await expect(progress.waitForCapacity()).rejects.toThrow('failed last page');
    expect(delivered).not.toHaveBeenCalled();
  });

  test('rollback leaves the previous durable cursor for restart', async () => {
    const c = await open(version);
    await using _factory = c.factory;
    await using writer = c.writer;
    await writer.updateTableProgress(c.table, page(0));
    const original = PersistedBatchV3.prototype.publish;
    using _publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      await original.apply(this, args);
      throw new Error('failed page publication');
    });
    await save(writer, c.table, 'A');
    const receipt = await writer.queueTableProgress!(c.table, page(1));
    await expect(receipt.durable).rejects.toThrow('failed page publication');
    const records = c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId);
    expect(await records.countDocuments()).toBe(0);
    await test_utils.releaseTestStorageLease(c.factory, c.stream.replicationStreamId);
    const restarted = await getTestStorage(c.factory, c.stream);
    await using next = await restarted.createWriter({ ...test_utils.BATCH_OPTIONS, skipExistingRows: true });
    const table = await test_utils.resolveTestTable(next, 'items', ['id'], c.gen, 1);
    expectProgress(table.snapshotStatus, 0);
    await save(next, table, 'A');
    const replay = await next.queueTableProgress!(table, page(1));
    await replay.durable;
    expect(await records.countDocuments()).toBe(1);
    expectProgress((await next.getSourceTableStatus(table))!.snapshotStatus, 1);
  });

  test('empty pages are bounded and table completion drains queued progress', async () => {
    const c = await open(version);
    await using _factory = c.factory;
    await using writer = c.writer;
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const original = PersistedBatchV3.prototype.publish;
    using _publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      entered.resolve();
      await release.promise;
      return original.apply(this, args);
    });
    const first = await writer.queueTableProgress!(c.table, page(0));
    await entered.promise;
    const second = await writer.queueTableProgress!(c.table, page(0));
    let admitted = false;
    const third = writer.queueTableProgress!(c.table, page(0)).then((receipt) => {
      admitted = true;
      return receipt;
    });
    let completed = false;
    const completion = writer.markTableSnapshotDone([c.table]).then(() => {
      completed = true;
    });
    try {
      expect(admitted).toBe(false);
      expect(completed).toBe(false);
    } finally {
      release.resolve();
      await first.durable;
      await second.durable;
      await (
        await third
      ).durable;
      await completion;
    }
    const table = await writer.getSourceTableStatus(c.table);
    expect(table!.snapshotComplete).toBe(true);
    expect(table!.snapshotStatus).toBeUndefined();
  });
});

// Existing storage implementations keep their awaited persistence contract.
describe.each([1, 2])('snapshot progress fallback v%s', (version) => {
  test('flushes rows before persisting and delivering progress', async () => {
    const c = await open(version);
    await using _factory = c.factory;
    await using writer = c.writer;
    expect(writer.queueTableProgress).toBeUndefined();
    const delivered = vi.fn();
    await using progress = new storage.SnapshotProgress(writer, delivered);
    using flush = vi.spyOn(writer, 'flush');
    using update = vi.spyOn(writer, 'updateTableProgress');
    await save(writer, c.table, 'A');
    await progress.update(c.table, page(1));
    expect(flush).toHaveBeenCalledTimes(1);
    expect(update).toHaveBeenCalledTimes(1);
    expect(flush.mock.invocationCallOrder[0]).toBeLessThan(update.mock.invocationCallOrder[0]);
    expect(delivered).toHaveBeenCalledTimes(1);
    expectProgress((await writer.getSourceTableStatus(c.table))!.snapshotStatus, 1);
  });
});
