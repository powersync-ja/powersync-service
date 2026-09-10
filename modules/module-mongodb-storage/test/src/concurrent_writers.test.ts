import { mongo } from '@powersync/lib-service-mongodb';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { describe, expect, test, vi } from 'vitest';
import { PersistedBatch } from '../../src/storage/implementation/common/PersistedBatch.js';
import { MongoOpIdAllocator } from '../../src/storage/implementation/MongoOpIdAllocator.js';
import { MongoPersistedReplicationStream } from '../../src/storage/implementation/MongoPersistedReplicationStream.js';
import { mongoTestStorageFactoryGenerator } from '../../src/utils/test-utils.js';
import { env } from './env.js';

const factoryGen = mongoTestStorageFactoryGenerator({ url: env.MONGO_TEST_URL, isCI: env.CI });
const rules = `bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

async function openStream(factory: Awaited<ReturnType<typeof factoryGen.factory>>, version: number) {
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(rules, { storageVersion: version }));
  const bucketStorage = factory.getInstance(stream);
  const writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  const table = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, stream.replicationStreamId);
  await writer.markAllSnapshotDone('1/1');
  return { stream, bucketStorage, writer, table };
}

async function insert(writer: storage.BucketStorageBatch, table: storage.SourceTable, id: string) {
  await writer.save({
    sourceTable: table,
    tag: storage.SaveOperationTag.INSERT,
    after: { id, description: id },
    afterReplicaId: test_utils.rid(id)
  });
}

describe.each([1, 2, 4])('concurrent writers v%s', (version) => {
  test('another stream publishes while a transaction is stalled, without touching its reserved IDs', async () => {
    await using factory = await factoryGen.factory();
    const a = await openStream(factory, version);
    await using writerA = a.writer;
    // An independent factory/client has no shared allocator or in-process coordination.
    await using other = await factoryGen.factory({ doNotClear: true });
    const b = await openStream(other, version);
    await using writerB = b.writer;
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const flush = PersistedBatch.prototype.flush;
    const stalled = vi.spyOn(PersistedBatch.prototype, 'flush').mockImplementationOnce(async function (
      this: PersistedBatch,
      ...args
    ) {
      entered.resolve();
      await release.promise;
      return flush.apply(this, args);
    });
    await insert(writerA, a.table, 'a');
    const publishingA = writerA.flush();
    try {
      await entered.promise;
      await insert(writerB, b.table, 'b');
      await writerB.commit('1/2');
      expect((await b.bucketStorage.getCheckpoint()).checkpoint).toBeGreaterThan(1n);
      expect((await a.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    } finally {
      release.resolve();
      await publishingA;
      stalled.mockRestore();
    }
    expect(writerA.last_flushed_op).toBe(1n);
    // A can keep using its lower range: B's checkpoint belongs to another stream.
    await insert(writerA, a.table, 'a2');
    await writerA.flush();
    expect(writerA.last_flushed_op).toBe(2n);
  });

  test('another process can reserve and checkpoint while this process holds a stream transaction', async () => {
    await using factory = await factoryGen.factory();
    const a = await openStream(factory, version);
    await using writerA = a.writer;
    const b = await openStream(factory, version);
    await b.writer.dispose();
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const flush = PersistedBatch.prototype.flush;
    const stalled = vi.spyOn(PersistedBatch.prototype, 'flush').mockImplementationOnce(async function (
      this: PersistedBatch,
      ...args
    ) {
      entered.resolve();
      await release.promise;
      return flush.apply(this, args);
    });
    await insert(writerA, a.table, 'a');
    const pending = writerA.flush();
    try {
      await entered.promise;
      const child = await promisify(execFile)(
        process.execPath,
        [
          fileURLToPath(new URL('./helpers/concurrentWriter.mjs', import.meta.url)),
          env.MONGO_TEST_URL,
          `${b.stream.replicationStreamId}`
        ],
        { timeout: 10_000 }
      ).catch((error) => {
        throw new Error(`${error.message}\n${error.stdout}\n${error.stderr}`);
      });
      expect(child.stdout).toMatch(/checkpoint=[1-9][0-9]+/);
      expect((await a.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    } finally {
      release.resolve();
      await pending;
      stalled.mockRestore();
    }
  }, 15_000);

  test('reuses one reservation across writers and flushes and retries rolled-back writes', async () => {
    await using factory = await factoryGen.factory();
    const { writer: batch, table, bucketStorage } = await openStream(factory, version);
    await using writer = batch;
    const reservations = vi.spyOn(MongoOpIdAllocator.prototype, 'reserve');
    const flush = PersistedBatch.prototype.flush;
    const retry = vi.spyOn(PersistedBatch.prototype, 'flush').mockImplementationOnce(async function (
      this: PersistedBatch,
      ...args
    ) {
      await flush.apply(this, args);
      throw new mongo.MongoServerError({ message: 'retry', code: 112, errorLabels: ['TransientTransactionError'] });
    });
    try {
      for (let i = 0; i < 3; i++) {
        await insert(writer, table, `${i}`);
        await writer.flush();
      }
      await writer.commit('1/2');
      expect(reservations).toHaveBeenCalledTimes(1);
      expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(3n);
      await using nextWriter = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const nextTable = await test_utils.resolveTestTable(nextWriter, 'items', ['id'], factoryGen, 1);
      await insert(nextWriter, nextTable, 'next');
      await nextWriter.commit('1/3');
      expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(4n);
      expect(reservations).toHaveBeenCalledTimes(1);
      expect((await factory.db.op_id_sequence.findOne({ _id: 'main' }))!.op_id).toBeGreaterThan(3n);
    } finally {
      reservations.mockRestore();
      retry.mockRestore();
    }
  });

  test('independent writers of the same stream never publish behind its durable head', async () => {
    await using factory = await factoryGen.factory();
    const a = await openStream(factory, version);
    await using writerA = a.writer;
    await insert(writerA, a.table, 'a');
    await writerA.flush();
    await using other = await factoryGen.factory({ doNotClear: true });
    const reloaded = (await other.getReplicatingReplicationStreams()).find(
      (s) => s.replicationStreamId === a.stream.replicationStreamId
    )!;
    await using writerB = await other.getInstance(reloaded).createWriter(test_utils.BATCH_OPTIONS);
    const tableB = await test_utils.resolveTestTable(writerB, 'items', ['id'], factoryGen, 1);
    await insert(writerB, tableB, 'b');
    await writerB.flush();
    const higher = writerB.last_flushed_op!;
    await insert(writerA, a.table, 'c');
    await writerA.flush();
    expect(writerA.last_flushed_op).toBeGreaterThan(higher);
    await writerA.commit('1/2');
    expect((await a.bucketStorage.getCheckpoint()).checkpoint).toBe(writerA.last_flushed_op);
  });

  test('lease takeover fences stale row, metadata, and checkpoint writes', async () => {
    await using factory = await factoryGen.factory();
    const initial = await openStream(factory, version);
    await initial.writer.dispose();
    const stream = initial.stream as MongoPersistedReplicationStream;
    const first = await stream.lock();
    await using firstLifetime = { [Symbol.asyncDispose]: () => first.release() };
    await using oldWriter = await factory.getInstance(stream).createWriter(test_utils.BATCH_OPTIONS);
    const oldTable = await test_utils.resolveTestTable(oldWriter, 'items', ['id'], factoryGen, 1);
    await insert(oldWriter, oldTable, 'old');
    await oldWriter.flush();
    const abandonedEnd = (await factory.db.op_id_sequence.findOne({ _id: 'main' }))!.op_id;
    await factory.db.sync_rules.updateOne(
      { _id: stream.replicationStreamId },
      { $set: { 'lock.expires_at': new Date(0) } }
    );
    await using other = await factoryGen.factory({ doNotClear: true });
    const nextStream = (await other.getReplicatingReplicationStreams())[0];
    const next = await nextStream.lock();
    await using nextLifetime = { [Symbol.asyncDispose]: () => next.release() };
    await using nextWriter = await other.getInstance(nextStream).createWriter(test_utils.BATCH_OPTIONS);
    const nextTable = await test_utils.resolveTestTable(nextWriter, 'items', ['id'], factoryGen, 1);
    await insert(nextWriter, nextTable, 'new');
    await nextWriter.commit('1/2');
    expect(nextWriter.last_flushed_op).toBeGreaterThan(abandonedEnd);
    await insert(oldWriter, oldTable, 'stale');
    await expect(oldWriter.flush()).rejects.toThrow('no longer owns');
    await expect(oldWriter.setResumeLsn('9/9')).rejects.toThrow('no longer owns');
    await expect(oldWriter.markAllSnapshotDone('9/9')).rejects.toThrow('no longer owns');
    await expect(oldWriter.markTableSnapshotRequired(oldTable)).rejects.toThrow('no longer owns');
    await expect(oldWriter.commit('9/9')).rejects.toThrow('no longer owns');
    await first.release();
    expect((await factory.db.sync_rules.findOne({ _id: stream.replicationStreamId }))!.lock?.id).toBe(
      (next as typeof first).lock_id
    );
  });

  test('range exhaustion rolls back and restarts with a fresh sequence', async () => {
    await using factory = await factoryGen.factory();
    const { writer: batch, table, bucketStorage } = await openStream(factory, version);
    await using writer = batch;
    // Simulate the last ID of an existing reservation being consumed before this row.
    // Use the real allocator/transaction path with a deliberately nearly-exhausted range.
    const allocator = factory.getOpIdAllocator(bucketStorage.replicationStream);
    await allocator.reserve();
    allocator.committed(65_535n);
    await insert(writer, table, 'first');
    await insert(writer, table, 'second');
    await writer.commit('1/2');
    expect(writer.last_flushed_op).toBe(65_537n);
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(65_537n);
  });
});

describe('operation ID reservations', () => {
  test('initializes and reserves disjoint ranges concurrently in one command each', async () => {
    await using factory = await factoryGen.factory();
    const a = await openStream(factory, 4);
    await using writer = a.writer;
    const db = factory.db.versioned(a.bucketStorage.replicationStream.getStorageConfig());
    const allocators = Array.from({ length: 4 }, () => new MongoOpIdAllocator(db));
    const initialize = vi.spyOn(db.op_id_sequence, 'updateOne');
    const reserve = vi.spyOn(db.op_id_sequence, 'findOneAndUpdate');
    try {
      await Promise.all(allocators.map((allocator) => allocator.reserve()));
      const firstIds = allocators.map((allocator) => allocator.sequence(0n).next()).sort((a, b) => Number(a - b));
      expect(firstIds).toEqual([1n, 65_537n, 131_073n, 196_609n]);
      expect(initialize).not.toHaveBeenCalled();
      expect(reserve).toHaveBeenCalledTimes(4);
    } finally {
      initialize.mockRestore();
      reserve.mockRestore();
    }
  });

  test('refuses a reservation that would overflow without changing the watermark', async () => {
    await using factory = await factoryGen.factory();
    const a = await openStream(factory, 4);
    await using writer = a.writer;
    const max = (1n << 63n) - 1n;
    await factory.db.op_id_sequence.insertOne({ _id: 'main', op_id: max - 65_536n });
    const allocator = factory.getOpIdAllocator(a.bucketStorage.replicationStream);
    await allocator.reserve();
    expect(allocator.sequence(max - 1n).next()).toBe(max);
    await expect(allocator.reserve()).rejects.toThrow('Operation ID sequence exhausted');
    expect((await factory.db.op_id_sequence.findOne({ _id: 'main' }))!.op_id).toBe(max);
  });
});
