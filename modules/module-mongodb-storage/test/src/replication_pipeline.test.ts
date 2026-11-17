import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test, vi } from 'vitest';
import { MongoPersistedReplicationStream } from '../../src/storage/implementation/MongoPersistedReplicationStream.js';
import { OperationBatch } from '../../src/storage/implementation/OperationBatch.js';
import { DEFAULT_REPLICATION_BATCH_LIMITS } from '../../src/storage/implementation/ReplicationBatchLimits.js';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { SourceRecordStoreV3 } from '../../src/storage/implementation/v3/SourceRecordStoreV3.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';

const { maxInputBatchCount: MAX_INPUT_BATCH_COUNT, maxPipelineBytes: MAX_PIPELINE_BYTES } =
  DEFAULT_REPLICATION_BATCH_LIMITS;

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
    const secondUpload = Promise.withResolvers<void>();
    const put = c.objects.put.bind(c.objects);
    using uploads = vi.spyOn(c.objects, 'put').mockImplementation(async (...args) => {
      const result = await put(...args);
      if (uploads.mock.calls.length === 2) {
        secondUpload.resolve();
      }
      return result;
    });
    let third: Promise<storage.FlushedResult | null> | undefined;
    try {
      expect(await save(writer, c.table, 'A', 'first')).toBeNull();
      await entered.promise;
      expect(await save(writer, c.table, 'A', 'second')).toBeNull();
      expect(publish).toHaveBeenCalledTimes(1);
      expect(reads).toHaveBeenCalledTimes(1); // The second row uses pending state.
      if (mode === 'offloaded') {
        await secondUpload.promise;
      }
      expect(uploads).toHaveBeenCalledTimes(mode === 'offloaded' ? 2 : 0);
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

  test('uploads the successor while its predecessor is still uploading', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const first = Promise.withResolvers<void>();
    const second = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const put = c.objects.put.bind(c.objects);
    let count = 0;
    using _put = vi.spyOn(c.objects, 'put').mockImplementation(async (...args) => {
      if (++count === 1) {
        first.resolve();
        await release.promise;
      }
      if (count === 2) {
        second.resolve();
      }
      return put(...args);
    });
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish');
    await save(writer, c.table, 'A', 'first');
    await first.promise;
    try {
      await save(writer, c.table, 'B', 'second');
      await second.promise;
      expect(_put).toHaveBeenCalledTimes(2);
      expect(publish).not.toHaveBeenCalled();
      expect(c.pipeline.pendingCount).toBe(2);
    } finally {
      release.resolve();
      await writer.flush();
    }
    expect(publish).toHaveBeenCalledTimes(2);
    expect(c.pipeline.accountedBytes).toBe(0);
  });

  test('interleaved snapshot and streaming saves retain normal input blocks', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    await using snapshot = await c.buckets.createWriter({ ...test_utils.BATCH_OPTIONS, skipExistingRows: true });
    using reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish');
    using uploads = vi.spyOn(c.objects, 'put');
    // Deliberately use the production input threshold, with alternating
    // writers. Switching after every save must not create tiny transactions/objects.
    for (let i = 0; i < MAX_INPUT_BATCH_COUNT; i++) {
      await save(writer, c.table, `stream-${i}`, 'streaming');
      await snapshot.save({
        sourceTable: c.table,
        tag: storage.SaveOperationTag.INSERT,
        afterReplicaId: test_utils.rid(`snapshot-${i}`),
        after: { id: `snapshot-${i}`, description: 'snapshot' }
      });
      if (i === MAX_INPUT_BATCH_COUNT - 2) {
        expect(reads).not.toHaveBeenCalled();
      }
    }
    await snapshot.flush();
    await writer.commit('1/2');
    expect(reads).toHaveBeenCalledTimes(2);
    expect(publish).toHaveBeenCalledTimes(2);
    expect(uploads.mock.calls.length).toBeGreaterThanOrEqual(2);
    expect(uploads.mock.calls.length).toBeLessThan(16);
    expect(
      await c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId).countDocuments()
    ).toBe(2 * MAX_INPUT_BATCH_COUNT);
    expect((await c.buckets.getCheckpoint()).lsn).toBe('1/2');
    expect(c.pipeline.accountedBytes).toBe(0);
  }, 30_000);

  test('metadata flushes every writer before truncating rows buffered below the input limit', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    await using snapshot = await c.buckets.createWriter({ ...test_utils.BATCH_OPTIONS, skipExistingRows: true });
    using reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    await save(writer, c.table, 'A', 'streaming');
    await snapshot.save({
      sourceTable: c.table,
      tag: storage.SaveOperationTag.INSERT,
      afterReplicaId: test_utils.rid('B'),
      after: { id: 'B', description: 'snapshot' }
    });
    // These are writer-local buffers, not admitted publication batches.
    expect(reads).not.toHaveBeenCalled();
    expect(c.pipeline.pendingCount).toBe(0);

    await writer.truncate([c.table]);
    // Later flushes must not resurrect input that was buffered before truncation.
    await snapshot.flush();
    await writer.commit('1/2');
    expect(
      await c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId).countDocuments()
    ).toBe(0);
    expect((await c.buckets.getCheckpoint()).lsn).toBe('1/2');
    expect(c.pipeline.pendingCount).toBe(0);
    expect(c.pipeline.accountedBytes).toBe(0);
  });

  test('buffers without locking while metadata publishes only its captured prefix', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    await using other = await c.buckets.createWriter(test_utils.BATCH_OPTIONS);
    await save(writer, c.table, 'A', 'before truncate');
    await save(other, c.table, 'B', 'before truncate');
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const load = SourceRecordStoreV3.prototype.loadDocuments;
    using _read = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      entered.resolve();
      await release.promise;
      return load.apply(this, args);
    });
    const truncating = writer.truncate([c.table]);
    let saving: Promise<unknown> | undefined;
    try {
      await entered.promise;
      using acquisitions = vi.spyOn(c.lock.writerMutex, 'exclusiveLock');
      let buffered = false;
      saving = Promise.all([
        save(writer, c.table, 'C', 'after truncate'),
        save(other, c.table, 'D', 'after truncate')
      ]).then(() => {
        buffered = true;
      });
      // Buffering is synchronous, even while preparation owns the writer mutex.
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(buffered).toBe(true);
      expect(acquisitions).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      await Promise.all([truncating, saving]);
    }
    const records = c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId);
    expect(await records.countDocuments()).toBe(0);
    // Neither the active writer's input nor the other writer's queued input may
    // absorb rows arriving after the metadata barrier was requested.
    await other.flush();
    await writer.commit('1/2');
    expect((await records.find({}).toArray()).map((row) => bson.deserialize(row.data!.buffer).id).sort()).toEqual([
      'C',
      'D'
    ]);
    expect((await c.buckets.getCheckpoint()).lsn).toBe('1/2');
  });

  test('a full input buffered after a metadata request waits and publishes after it', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    await using other = await c.buckets.createWriter(test_utils.BATCH_OPTIONS);
    await save(writer, c.table, 'A', 'before truncate');
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const holding = c.lock.writerMutex.exclusiveLock(async () => {
      entered.resolve();
      await release.promise;
    });
    await entered.promise;
    const truncating = writer.truncate([c.table]);
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    let submitted = false;
    const saving = save(other, c.table, 'B', 'after truncate').then(() => {
      submitted = true;
    });
    try {
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(submitted).toBe(false); // A full buffer still applies backpressure.
    } finally {
      release.resolve();
      await Promise.all([holding, truncating, saving]);
    }
    await other.commit('1/2');
    const records = await c.buckets.db
      .sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId)
      .find({})
      .toArray();
    expect(records.map((row) => bson.deserialize(row.data!.buffer).id)).toEqual(['B']);
    expect((await c.buckets.getCheckpoint()).lsn).toBe('1/2');
  });

  test('split flush remainders do not overwrite rows buffered during preparation', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    await save(writer, c.table, 'A', 'first');
    await save(writer, c.table, 'B', 'second');
    using _split = vi.spyOn(PersistedBatchV3.prototype, 'shouldFlushTransaction').mockReturnValue(true);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const load = SourceRecordStoreV3.prototype.loadDocuments;
    using _read = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      entered.resolve();
      await release.promise;
      return load.apply(this, args);
    });
    const flushing = writer.flush();
    try {
      await entered.promise;
      await save(writer, c.table, 'C', 'next input');
    } finally {
      release.resolve();
      await flushing;
    }
    const records = c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId);
    const ids = async () =>
      (await records.find({}).toArray()).map((row) => bson.deserialize(row.data!.buffer).id).sort();
    expect(await ids()).toEqual(['A', 'B']);
    await writer.commit('1/2');
    expect(await ids()).toEqual(['A', 'B', 'C']);
    expect(c.pipeline.accountedBytes).toBe(0);
  });

  test('disposing a writer with detached input waiting for the mutex invalidates the lease', async () => {
    const c = await open(version, 'disabled');
    await using _factory = c.factory;
    await using writer = c.writer;
    await save(writer, c.table, 'A', 'queued');
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const holding = c.lock.writerMutex.exclusiveLock(async () => {
      entered.resolve();
      await release.promise;
    });
    await entered.promise;
    const flushing = expect(writer.flush()).rejects.toThrow('restart');
    const disposing = writer.dispose();
    try {
      expect(c.lock.signal.aborted).toBe(true);
    } finally {
      release.resolve();
      await Promise.all([holding, flushing, disposing]);
    }
    expect(c.pipeline.pendingCount).toBe(0);
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
      .mockReturnValue(MAX_PIPELINE_BYTES + 1);
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
    await writer.commit('1/1');
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

  test('joins an active successor upload when predecessor publication fails', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const publishing = Promise.withResolvers<void>();
    const failPublication = Promise.withResolvers<void>();
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async () => {
      publishing.resolve();
      await failPublication.promise;
      throw new Error('publication failed');
    });
    const uploading = Promise.withResolvers<void>();
    const finishUpload = Promise.withResolvers<void>();
    const put = c.objects.put.bind(c.objects);
    let uploads = 0;
    using _put = vi.spyOn(c.objects, 'put').mockImplementation(async (...args) => {
      if (++uploads === 2) {
        uploading.resolve();
        await finishUpload.promise;
      }
      return put(...args);
    });
    let joined = false;
    let joining: Promise<void> | undefined;
    try {
      await save(writer, c.table, 'A', 'first');
      await publishing.promise;
      await save(writer, c.table, 'B', 'second');
      await uploading.promise;
      joining = c.pipeline.join().then(() => {
        joined = true;
      });
      failPublication.resolve();
      await expect(save(writer, c.table, 'C', 'third')).rejects.toThrow('restart');
      expect(joined).toBe(false);
      expect(c.pipeline.accountedBytes).toBeGreaterThan(0);
    } finally {
      failPublication.resolve();
      finishUpload.resolve();
      await joining;
    }
    expect(publish).toHaveBeenCalledTimes(1);
    expect(c.pipeline.pendingCount).toBe(0);
    expect(c.pipeline.accountedBytes).toBe(0);
    expect(
      await c.buckets.db.sourceRecords(c.stream.replicationStreamId, c.table.id as bson.ObjectId).countDocuments()
    ).toBe(0);
    expect((await c.buckets.getCheckpoint()).checkpoint).toBe(0n);
  });

  test('successor upload failure stops admission before predecessor publication finishes', async () => {
    const c = await open(version, 'offloaded');
    await using _factory = c.factory;
    await using writer = c.writer;
    using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
    const publishing = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const original = PersistedBatchV3.prototype.publish;
    using publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementationOnce(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      publishing.resolve();
      await release.promise;
      return original.apply(this, args);
    });
    const put = c.objects.put.bind(c.objects);
    let uploads = 0;
    using _put = vi.spyOn(c.objects, 'put').mockImplementation(async (...args) => {
      if (++uploads === 2) {
        throw new Error('successor upload failed');
      }
      return put(...args);
    });
    const aborted = Promise.withResolvers<void>();
    c.lock.signal.addEventListener('abort', () => aborted.resolve(), { once: true });
    try {
      await save(writer, c.table, 'A', 'first');
      await publishing.promise;
      await save(writer, c.table, 'B', 'second');
      await aborted.promise;
      await expect(save(writer, c.table, 'C', 'third')).rejects.toThrow('restart');
      // The failed successor must stay behind its predecessor through teardown.
      expect(c.pipeline.pendingCount).toBe(2);
      expect(c.pipeline.accountedBytes).toBeGreaterThan(0);
    } finally {
      release.resolve();
      await c.pipeline.join();
    }
    expect(publish).toHaveBeenCalledTimes(1);
    expect(c.pipeline.pendingCount).toBe(0);
    expect(c.pipeline.accountedBytes).toBe(0);
    expect((await c.buckets.getCheckpoint()).checkpoint).toBe(0n);
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
