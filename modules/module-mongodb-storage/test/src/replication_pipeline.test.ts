import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test, vi } from 'vitest';
import { MongoPersistedReplicationStream } from '../../src/storage/implementation/MongoPersistedReplicationStream.js';
import { OperationBatch } from '../../src/storage/implementation/OperationBatch.js';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { SourceRecordStoreV3 } from '../../src/storage/implementation/v3/SourceRecordStoreV3.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';

const CONFIG = `
config:
  edition: 3
streams:
  items:
    query: SELECT * FROM items
`;

async function open(version: number, mode: string) {
  const objects = new MemoryObjectStorage();
  const gen = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    objectStorage: mode === 'disabled' ? undefined : objects,
    inlineThresholdBytes: mode === 'inline' ? 16 * 1024 : 0
  });
  const factory = await gen.factory();
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(CONFIG, { storageVersion: version }));
  const buckets = (await getTestStorage(factory, stream)) as MongoSyncBucketStorageV3;
  const writer = await buckets.createWriter(test_utils.BATCH_OPTIONS);
  const table = await test_utils.resolveTestTable(writer, 'items', ['id'], gen, 1);
  await writer.markAllSnapshotDone('1/1');
  const lock = (stream as MongoPersistedReplicationStream).current_lock!;
  return { factory, stream, buckets, writer, table, objects, lock, pipeline: lock.replicationPipeline! };
}

function save(writer: storage.BucketStorageBatch, table: storage.SourceTable, id: string, description?: string) {
  return writer.save({
    sourceTable: table,
    tag: storage.SaveOperationTag.UPDATE,
    afterReplicaId: test_utils.rid(id),
    after: { id, description }
  });
}

describe.each([3, 4])('replication pipeline v%s', (version) => {
  test.each(['disabled', 'inline', 'offloaded'])('prepares ahead of a blocked publisher (%s)', async (mode) => {
    const c = await open(version, mode);
    await using _factory = c.factory;
    await using writer = c.writer;
    // Keep integration fixtures small while exercising the real automatic path.
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const original = PersistedBatchV3.prototype.publish;
    const published: PersistedBatchV3[] = [];
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementation(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      published.push(this);
      if (published.length === 1) {
        entered.resolve();
        await release.promise;
      }
      return original.apply(this, args);
    });
    using reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    using uploads = vi.spyOn(c.objects, 'put');
    let third: Promise<storage.FlushedResult | null> | undefined;
    try {
      expect(await save(writer, c.table, 'A', 'first')).toBeNull();
      await entered.promise;
      expect(await save(writer, c.table, 'A', 'second')).toBeNull();
      expect(publish).toHaveBeenCalledTimes(1);
      expect(reads).toHaveBeenCalledTimes(1); // The second row uses pending state.
      expect(uploads).toHaveBeenCalledTimes(mode === 'offloaded' ? 1 : 0);
      expect(writer.last_flushed_op).toBeNull();
      expect(c.pipeline.pendingCount).toBe(2);
      const waiting = Promise.withResolvers<void>();
      const capacity = c.pipeline.capacity.bind(c.pipeline);
      using _capacity = vi.spyOn(c.pipeline, 'capacity').mockImplementation(async (...args) => {
        waiting.resolve();
        return capacity(...args);
      });
      third = save(writer, c.table, 'B', 'third');
      await waiting.promise;
      expect(reads).toHaveBeenCalledTimes(1);
      expect(publish).toHaveBeenCalledTimes(1);
    } finally {
      release.resolve();
      await third;
    }
    await writer.commit('1/2');
    expect(await c.buckets.getCheckpoint()).toMatchObject({ checkpoint: 3n, lsn: '1/2' });
    expect(c.pipeline.pendingCount).toBe(0);
    expect(c.pipeline.accountedBytes).toBe(0);
    expect(c.pipeline.overlayEntryCount).toBe(0);
    const records = await c.buckets.db
      .sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId)
      .find({})
      .toArray();
    expect(records).toHaveLength(2);
    expect(records.map((r) => bson.deserialize(r.data!.buffer)).sort((a, b) => a.id.localeCompare(b.id))).toEqual([
      { id: 'A', description: 'second' },
      { id: 'B', description: 'third' }
    ]);
  });

  test('prepares the successor while uploads remain serial', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const first = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const put = c.objects.put.bind(c.objects);
    let count = 0;
    using _put = vi.spyOn(c.objects, 'put').mockImplementation(async (...args) => {
      if (++count === 1) {
        first.resolve();
        await release.promise;
      }
      return put(...args);
    });
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish');
    await save(writer, c.table, 'A', 'first');
    await first.promise;
    try {
      await save(writer, c.table, 'B', 'second');
      expect(_put).toHaveBeenCalledTimes(1);
      expect(publish).not.toHaveBeenCalled();
      expect(c.pipeline.pendingCount).toBe(2);
    } finally {
      release.resolve();
      await writer.flush();
    }
    expect(publish).toHaveBeenCalledTimes(2);
    expect(c.pipeline.accountedBytes).toBe(0);
  });

  test.each([false, true])('snapshots can fill both pipeline slots (active: %s)', async (active) => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    // Open before activation: the policy must change for existing writers too.
    await using snapshot = await c.buckets.createWriter({ ...test_utils.BATCH_OPTIONS, skipExistingRows: true });
    if (active) await writer.commit('1/1');
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
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
    const insert = (id: string) =>
      snapshot.save({
        sourceTable: c.table,
        tag: storage.SaveOperationTag.INSERT,
        afterReplicaId: test_utils.rid(id),
        after: { id }
      });
    try {
      await insert('first');
      await entered.promise;
      await insert('second');
      expect(c.pipeline.pendingCount).toBe(2);
      expect(publish).toHaveBeenCalledTimes(1);
    } finally {
      release.resolve();
      await snapshot.flush();
    }
    await writer.commit('1/2');
    expect(
      await c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId).countDocuments()
    ).toBe(2);
  });

  test('ACTIVE snapshots resume when queued streaming advances to publication', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    await using snapshot = await c.buckets.createWriter({ ...test_utils.BATCH_OPTIONS, skipExistingRows: true });
    await writer.commit('1/1');
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const entered = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
    const release = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
    const publish = PersistedBatchV3.prototype.publish;
    let publications = 0;
    using _publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementation(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      const index = publications++;
      if (index < 2) {
        entered[index].resolve();
        await release[index].promise;
      }
      return publish.apply(this, args);
    });
    let next: Promise<unknown> | undefined;
    try {
      await save(writer, c.table, 'stream-1', 'streaming');
      await entered[0].promise;
      await save(writer, c.table, 'stream-2', 'streaming');
      const attempted = Promise.withResolvers<void>();
      const run = c.pipeline.runWriter.bind(c.pipeline);
      using _run = vi.spyOn(c.pipeline, 'runWriter').mockImplementation((snapshot, callback) => {
        if (snapshot) attempted.resolve();
        return run(snapshot, callback);
      });
      let finished = false;
      next = snapshot
        .save({
          sourceTable: c.table,
          tag: storage.SaveOperationTag.INSERT,
          afterReplicaId: test_utils.rid('snapshot'),
          after: { id: 'snapshot' }
        })
        .then(() => {
          finished = true;
        });
      await attempted.promise;
      // A paused snapshot does not hold the mutex. Merely buffered streaming
      // rows must neither block here nor keep the snapshot paused later.
      _limit.mockRestore();
      await save(writer, c.table, 'partial', 'buffered');
      expect(finished).toBe(false);
      expect(c.pipeline.pendingCount).toBe(2);
      release[0].resolve();
      await entered[1].promise;
      await next;
      expect(finished).toBe(true);
      expect(c.pipeline.pendingCount).toBe(2); // One streaming + one snapshot.
      expect(publications).toBe(2); // Streaming publication is still stalled.
    } finally {
      for (const gate of release) gate.resolve();
      await next;
      await snapshot.flush();
    }
    await writer.commit('1/2');
    expect(
      await c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId).countDocuments()
    ).toBe(4);
  });

  test('large ACTIVE snapshot inputs yield between publication transactions', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    await writer.commit('1/1');
    await using snapshot = await c.buckets.createWriter({ ...test_utils.BATCH_OPTIONS, skipExistingRows: true });
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockImplementation(function (
      this: OperationBatch
    ) {
      return this.batch[0].record.after?.id === 'stream' || this.length >= 3;
    });
    using _transaction = vi.spyOn(PersistedBatchV3.prototype, 'shouldFlushTransaction').mockReturnValue(true);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const publish = PersistedBatchV3.prototype.publish;
    using _publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      entered.resolve();
      await release.promise;
      return publish.apply(this, args);
    });
    const input = (async () => {
      for (let i = 0; i < 3; i++)
        await snapshot.save({
          sourceTable: c.table,
          tag: storage.SaveOperationTag.INSERT,
          afterReplicaId: test_utils.rid(`snapshot-${i}`),
          after: { id: `snapshot-${i}` }
        });
    })();
    let streaming: Promise<unknown> | undefined;
    try {
      await entered.promise;
      // A second snapshot may already be queued. The remaining input must
      // yield to a full streaming block before taking another available slot.
      const attempted = Promise.withResolvers<void>();
      const run = c.pipeline.runWriter.bind(c.pipeline);
      using _run = vi.spyOn(c.pipeline, 'runWriter').mockImplementation((snapshot, callback) => {
        if (!snapshot) attempted.resolve();
        return run(snapshot, callback);
      });
      streaming = save(writer, c.table, 'stream', 'streaming');
      await attempted.promise;
    } finally {
      release.resolve();
      await streaming;
      await input;
    }
    await snapshot.flush();
    expect(writer.last_flushed_op!).toBeLessThan(snapshot.last_flushed_op!);
    expect(snapshot.last_flushed_op).toBe(4n);
  });

  test.each([false, true])('streaming boundary priority is ACTIVE-only (%s)', async (active) => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    if (active) await writer.commit('1/1');
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const held = c.lock.writerMutex.exclusiveLock(async () => {
      entered.resolve();
      await release.promise;
    });
    await entered.promise;
    const order: string[] = [];
    const snapshot = c.pipeline.runWriter(true, async () => {
      order.push('snapshot');
    });
    const stream = c.pipeline.runWriter(false, async () => {
      order.push('stream');
    });
    release.resolve();
    await Promise.all([held, snapshot, stream]);
    expect(order).toEqual(active ? ['stream', 'snapshot'] : ['snapshot', 'stream']);
  });

  test('lease loss rejects a snapshot waiting outside the writer mutex', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    await writer.commit('1/1');
    await using snapshot = await c.buckets.createWriter({ ...test_utils.BATCH_OPTIONS, skipExistingRows: true });
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const publish = PersistedBatchV3.prototype.publish;
    using _publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      entered.resolve();
      await release.promise;
      return publish.apply(this, args);
    });
    await save(writer, c.table, 'stream', 'streaming');
    await entered.promise;
    await save(writer, c.table, 'queued-stream', 'streaming');
    const attempted = Promise.withResolvers<void>();
    const run = c.pipeline.runWriter.bind(c.pipeline);
    using _run = vi.spyOn(c.pipeline, 'runWriter').mockImplementation((snapshot, callback) => {
      if (snapshot) attempted.resolve();
      return run(snapshot, callback);
    });
    const pending = snapshot.save({
      sourceTable: c.table,
      tag: storage.SaveOperationTag.INSERT,
      afterReplicaId: test_utils.rid('snapshot'),
      after: { id: 'snapshot' }
    });
    const rejected = expect(pending).rejects.toThrow('lease lost');
    try {
      await attempted.promise;
      c.lock.invalidate(new Error('lease lost'));
      await rejected;
    } finally {
      release.resolve();
      await c.pipeline.join();
    }
  });

  test('interleaved snapshot and streaming saves retain normal input blocks', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    await using snapshot = await c.buckets.createWriter({ ...test_utils.BATCH_OPTIONS, skipExistingRows: true });
    using reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish');
    using uploads = vi.spyOn(c.objects, 'put');
    // Deliberately use the production 2,000-row input threshold, with alternating
    // writers. Switching after every save must not create tiny transactions/objects.
    for (let i = 0; i < 2000; i++) {
      await save(writer, c.table, `stream-${i}`, 'streaming');
      await snapshot.save({
        sourceTable: c.table,
        tag: storage.SaveOperationTag.INSERT,
        afterReplicaId: test_utils.rid(`snapshot-${i}`),
        after: { id: `snapshot-${i}`, description: 'snapshot' }
      });
      if (i === 1998) expect(reads).not.toHaveBeenCalled();
    }
    await snapshot.flush();
    await writer.commit('1/2');
    expect(reads).toHaveBeenCalledTimes(2);
    expect(publish).toHaveBeenCalledTimes(2);
    expect(uploads).toHaveBeenCalledTimes(2);
    expect(
      await c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId).countDocuments()
    ).toBe(4000);
    expect((await c.buckets.getCheckpoint()).lsn).toBe('1/2');
    expect(c.pipeline.accountedBytes).toBe(0);
  });

  test('an oversized prefix stops further admission until publication completes', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    // Exercise byte backpressure with a small real fixture, independent of the
    // ordinary two-prefix count limit.
    using _size = vi
      .spyOn(PersistedBatchV3.prototype, 'estimatedRetainedBytes', 'get')
      .mockReturnValue(65 * 1024 * 1024);
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
    using reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    await save(writer, c.table, 'A', 'first');
    await entered.promise;
    const attempted = Promise.withResolvers<void>();
    const capacity = c.pipeline.capacity.bind(c.pipeline);
    using _capacity = vi.spyOn(c.pipeline, 'capacity').mockImplementation(async (...args) => {
      attempted.resolve();
      return capacity(...args);
    });
    const next = save(writer, c.table, 'B', 'second');
    try {
      await attempted.promise;
      expect(c.pipeline.pendingCount).toBe(1);
      expect(reads).toHaveBeenCalledTimes(1);
    } finally {
      release.resolve();
      await next;
      await writer.flush();
    }
    expect(c.pipeline.accountedBytes).toBe(0);
  });

  test('shares pending state across writers and orders snapshot progress behind it', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    await using other = await c.buckets.createWriter(test_utils.BATCH_OPTIONS);
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
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
    await save(writer, c.table, 'A', 'first');
    await entered.promise;
    await other.save({
      sourceTable: c.table,
      tag: storage.SaveOperationTag.UPDATE,
      afterReplicaId: test_utils.rid('A'),
      after: { id: undefined, description: 'second' }
    });
    let persisted = false;
    const progress = other.updateTableProgress(c.table, { replicatedCount: 1 }).then((table) => {
      persisted = true;
      return table;
    });
    try {
      expect(c.pipeline.pendingCount).toBe(2);
      expect(persisted).toBe(false);
    } finally {
      release.resolve();
      await progress;
    }
    expect(persisted).toBe(true);
    const record = await c.buckets.db
      .sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId)
      .findOne({});
    expect(bson.deserialize(record!.data!.buffer)).toEqual({ id: 'A', description: 'second' });
    expect(c.pipeline.accountedBytes).toBe(0);
  });

  test('snapshot skip-existing sees a pending streaming tombstone', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    await save(writer, c.table, 'A', 'initial');
    await writer.flush(); // Keep PROCESSING to exercise speculative tombstones.
    await using snapshot = await c.buckets.createWriter({ ...test_utils.BATCH_OPTIONS, skipExistingRows: true });
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
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
    using uploads = vi.spyOn(c.objects, 'put');
    await writer.save({
      sourceTable: c.table,
      tag: storage.SaveOperationTag.DELETE,
      beforeReplicaId: test_utils.rid('A')
    });
    await entered.promise;
    try {
      await snapshot.save({
        sourceTable: c.table,
        tag: storage.SaveOperationTag.INSERT,
        afterReplicaId: test_utils.rid('A'),
        after: { id: 'A', description: 'stale snapshot' }
      });
      expect(publish).toHaveBeenCalledTimes(1);
    } finally {
      release.resolve();
      await snapshot.flush();
    }
    // Only the streaming REMOVE uploads; the stale snapshot generates no PUT.
    expect(uploads).toHaveBeenCalledTimes(1);
    await writer.commit('1/2');
    expect(
      await c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId).countDocuments()
    ).toBe(0);
  });

  test('a drained writer can dispose while another publisher is stalled', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    const other = await c.buckets.createWriter(test_utils.BATCH_OPTIONS);
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
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
    await save(writer, c.table, 'A', 'first');
    await entered.promise;
    try {
      await other.dispose();
      expect(c.lock.signal.aborted).toBe(false);
    } finally {
      release.resolve();
      await writer.flush();
    }
  });

  test('pins pending row versions through a stale read finishing after commit', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const publishing = Promise.withResolvers<void>();
    const releasePublication = Promise.withResolvers<void>();
    const original = PersistedBatchV3.prototype.publish;
    using _publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      publishing.resolve();
      await releasePublication.promise;
      return original.apply(this, args);
    });
    await save(writer, c.table, 'A', 'first');
    await publishing.promise;
    const reading = Promise.withResolvers<void>();
    const releaseRead = Promise.withResolvers<void>();
    const sizes = SourceRecordStoreV3.prototype.loadSizes;
    using _sizes = vi.spyOn(SourceRecordStoreV3.prototype, 'loadSizes').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      const result = await sizes.apply(this, args);
      reading.resolve();
      await releaseRead.promise;
      return result;
    });
    const next = writer.save({
      sourceTable: c.table,
      tag: storage.SaveOperationTag.UPDATE,
      afterReplicaId: test_utils.rid('A'),
      after: { id: undefined, description: 'second' }
    });
    try {
      await reading.promise;
      releasePublication.resolve();
      await c.pipeline.join();
      expect(c.pipeline.overlayEntryCount).toBe(1);
      expect(c.pipeline.accountedBytes).toBeGreaterThan(0);
    } finally {
      releaseRead.resolve();
      releasePublication.resolve();
      await next;
    }
    await writer.commit('1/2');
    const record = await c.buckets.db
      .sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId)
      .findOne({});
    expect(bson.deserialize(record!.data!.buffer)).toEqual({ id: 'A', description: 'second' });
    expect(c.pipeline.overlayEntryCount).toBe(0);
    expect(c.pipeline.accountedBytes).toBe(0);
  });

  test('automatic flush returns a durable result when a partial row needs repair', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using _writer = c.writer;
    const repairs = vi.fn();
    await using writer = await c.buckets.createWriter({ ...test_utils.BATCH_OPTIONS, markRecordUnavailable: repairs });
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const result = await writer.save({
      sourceTable: c.table,
      tag: storage.SaveOperationTag.UPDATE,
      afterReplicaId: 'missing',
      after: { id: 'missing', description: undefined }
    });
    expect(result).not.toBeNull();
    expect(result!.flushed_op).toBe(writer.last_flushed_op);
    expect(repairs).toHaveBeenCalledTimes(1);
    expect(c.pipeline.pendingCount).toBe(0);
    expect((await c.buckets.getCheckpoint()).lsn).toBeNull();
    // Simulate the connector's targeted repair before its source commit.
    expect(await save(writer, c.table, 'missing', 'repaired')).toBeNull();
    await writer.flush();
    await writer.commit('1/2');
    expect((await c.buckets.getCheckpoint()).lsn).toBe('1/2');
    expect(repairs).toHaveBeenCalledTimes(1);
  });

  test('publication failure rejects capacity waiters and stops the suffix', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async () => {
      entered.resolve();
      await release.promise;
      throw new Error('publication failed');
    });
    await save(writer, c.table, 'A', 'first');
    await entered.promise;
    await save(writer, c.table, 'B', 'second');
    const rejected = expect(save(writer, c.table, 'C', 'third')).rejects.toThrow('restart');
    release.resolve();
    await rejected;
    await expect(writer.flush()).rejects.toThrow('restart');
    await c.pipeline.join();
    expect(publish).toHaveBeenCalledTimes(1);
    expect(c.pipeline.accountedBytes).toBe(0);
    expect(writer.last_flushed_op).toBeNull();
    expect((await c.buckets.getCheckpoint()).checkpoint).toBe(0n);
  });
});
