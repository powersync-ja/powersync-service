import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { logger as defaultLogger } from '@powersync/lib-services-framework';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test, vi } from 'vitest';
import { MongoIdSequence } from '../../src/storage/implementation/MongoIdSequence.js';
import { OperationBatch } from '../../src/storage/implementation/OperationBatch.js';
import { MongoBucketBatchV3 } from '../../src/storage/implementation/v3/MongoBucketBatchV3.js';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { SourceRecordStoreV3 } from '../../src/storage/implementation/v3/SourceRecordStoreV3.js';
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

async function open(version: number, enabled = false, options: Partial<storage.CreateWriterOptions> = {}) {
  const objectStorage = new MemoryObjectStorage();
  const factoryGen = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    monitorCommands: true,
    objectStorage: enabled ? objectStorage : undefined,
    inlineThresholdBytes: 0
  });
  const factory = await factoryGen.factory();
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(CONFIG, { storageVersion: version }));
  const bucketStorage = (await getTestStorage(factory, stream)) as MongoSyncBucketStorageV3;
  const writer = (await bucketStorage.createWriter({ ...test_utils.BATCH_OPTIONS, ...options })) as MongoBucketBatchV3;
  const table = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
  await writer.markAllSnapshotDone('1/1');
  await writer.save({
    sourceTable: table,
    tag: storage.SaveOperationTag.INSERT,
    after: { id: 'A', description: 'initial', retained: 'old' },
    afterReplicaId: test_utils.rid('A')
  });
  await writer.flush();
  await writer.commit('1/1');
  return { factory, factoryGen, stream, bucketStorage, writer, table, objectStorage };
}

async function save(writer: storage.BucketStorageBatch, table: storage.SourceTable, description: string) {
  await writer.save({
    sourceTable: table,
    tag: storage.SaveOperationTag.UPDATE,
    after: { id: 'A', description, retained: 'old' },
    afterReplicaId: test_utils.rid('A')
  });
}

describe.each([3, 4])('eager replication preparation v%s', (version) => {
  test.each([false, true])(
    'reads/evaluates once and delivers repairs after commit across rollback (object storage: %s)',
    async (enabled) => {
      const unavailable = vi.fn();
      const context = await open(version, enabled, { markRecordUnavailable: unavailable });
      await using factory = context.factory;
      await using writer = context.writer;
      using commands = new PublicationCommandMonitor(factory.db.client);
      const read = SourceRecordStoreV3.prototype.loadDocuments;
      using documents = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementation(function (
        this: SourceRecordStoreV3,
        ...args
      ) {
        expect(args[0].inTransaction()).toBe(false);
        return read.apply(this, args);
      });
      const sizesRead = SourceRecordStoreV3.prototype.loadSizes;
      using sizes = vi.spyOn(SourceRecordStoreV3.prototype, 'loadSizes').mockImplementation(function (
        this: SourceRecordStoreV3,
        ...args
      ) {
        expect(args[0].inTransaction()).toBe(false);
        return sizesRead.apply(this, args);
      });
      using put = vi.spyOn(context.objectStorage, 'put');
      const publish = PersistedBatchV3.prototype.publish;
      let attempts = 0;
      using _retry = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementation(async function (
        this: PersistedBatchV3,
        ...args
      ) {
        expect(unavailable).not.toHaveBeenCalled();
        const result = await publish.apply(this, args);
        if (++attempts === 1) {
          throw new mongo.MongoServerError({
            message: 'retry prepared writes',
            code: 112,
            errorLabels: ['TransientTransactionError']
          });
        }
        return result;
      });
      await save(writer, context.table, 'updated');
      await writer.save({
        sourceTable: context.table,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'missing', description: undefined },
        afterReplicaId: test_utils.rid('missing')
      });
      await writer.flush();
      expect(attempts).toBe(2);
      expect(documents).toHaveBeenCalledTimes(1);
      expect(sizes).toHaveBeenCalledTimes(1);
      expect(unavailable).toHaveBeenCalledTimes(1);
      expect(put).toHaveBeenCalledTimes(enabled ? 1 : 0);
      expect(writer.last_flushed_op).toBe(2n);
      // The connector must be able to repair before advancing its source position.
      expect(await factory.db.sync_rules.findOne({ _id: context.stream.replicationStreamId })).toMatchObject({
        resume_lsn: '1/1',
        sync_configs: [{ last_checkpoint: 1n, last_checkpoint_lsn: '1/1' }]
      });
      for (const attempt of commands.replicationAttempts) {
        expect(attempt.commands.some((command) => ['find', 'aggregate'].includes(command.name))).toBe(false);
      }
    }
  );

  test('logs oversized output during preparation without repeating it on publication retry', async () => {
    const logger = defaultLogger.child({ test: 'oversized output diagnostics' });
    const context = await open(version, false, { logger });
    await using _factory = context.factory;
    await using writer = context.writer;
    using logged = vi.spyOn(logger, 'error');
    const publish = PersistedBatchV3.prototype.publish;
    let attempts = 0;
    using _retry = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementation(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      expect(logged).toHaveBeenCalledTimes(1);
      const result = await publish.apply(this, args);
      if (++attempts === 1) {
        throw new mongo.MongoServerError({
          message: 'retry oversized row removal',
          code: 112,
          errorLabels: ['TransientTransactionError']
        });
      }
      return result;
    });
    // BSON can store this source row, but JSON escaping doubles its output size
    // beyond the bucket-row limit. save() auto-flushes this large input.
    await writer.save({
      sourceTable: context.table,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'A', description: '"'.repeat(8 * 1024 * 1024), retained: 'old' },
      afterReplicaId: test_utils.rid('A')
    });
    // Automatic save admission is speculative; the explicit barrier waits for
    // the immutable publication and its transaction retry.
    await writer.flush();
    expect(attempts).toBe(2);
    expect(logged).toHaveBeenCalledTimes(1);
    expect(logged).toHaveBeenCalledWith(expect.stringMatching(/^Row .* too large: .* bytes\. Removing\.$/));
  });

  test('requires explicit flush/repair before commit, including split batches', async () => {
    const repairs: storage.SaveUpdate[] = [];
    const context = await open(version, false, {
      markRecordUnavailable: (record) => {
        repairs.push(record);
      }
    });
    await using factory = context.factory;
    await using writer = context.writer;
    using _split = vi.spyOn(PersistedBatchV3.prototype, 'shouldFlushTransaction').mockReturnValue(true);
    using reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    for (const id of ['missing1', 'missing2']) {
      await writer.save({
        sourceTable: context.table,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id, description: undefined },
        afterReplicaId: id
      });
    }
    await expect(writer.commit('1/2')).rejects.toThrow('Flush pending rows');
    expect(reads).not.toHaveBeenCalled();
    expect(repairs).toEqual([]);
    await writer.flush();
    expect(repairs.map((record) => record.after.id)).toEqual(['missing1', 'missing2']);
    expect(await factory.db.sync_rules.findOne({ _id: context.stream.replicationStreamId })).toMatchObject({
      resume_lsn: '1/1',
      sync_configs: [{ last_checkpoint: 1n }]
    });
    // Model WalStream's separate resnapshot phase, outside writer admission.
    for (const repair of repairs) {
      await writer.save({
        sourceTable: repair.sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: repair.after.id!, description: 'recovered' },
        afterReplicaId: repair.afterReplicaId
      });
    }
    await writer.flush();
    await writer.commit('1/2');
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(3n);
    expect(repairs).toHaveLength(2);
  });

  test.each(['before-commit', 'after-commit'])('failed %s flush does not lose repair on replay', async (phase) => {
    const unavailable = vi.fn();
    const context = await open(version, false, { markRecordUnavailable: unavailable });
    await using factory = context.factory;
    await using writer = context.writer;
    const failure = new Error('publication failed');
    const publish = PersistedBatchV3.prototype.publish;
    const withTransaction = writer.session.withTransaction.bind(writer.session);
    using aborted = vi.spyOn(PersistedBatchV3.prototype, 'publish');
    using uncertain = vi.spyOn(writer.session, 'withTransaction');
    if (phase === 'before-commit') {
      aborted.mockImplementationOnce(async function (this: PersistedBatchV3, ...args) {
        await publish.apply(this, args);
        throw failure;
      });
    } else {
      // Simulate a terminal lost commit response: the data committed, but the
      // writer cannot confirm it and therefore must not deliver repairs.
      uncertain.mockImplementationOnce(async (...args) => {
        await withTransaction(...args);
        throw failure;
      });
    }
    await writer.save({
      sourceTable: context.table,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'missing', description: undefined },
      afterReplicaId: 'missing'
    });
    await expect(writer.flush()).rejects.toThrow(failure);
    expect(unavailable).not.toHaveBeenCalled();
    const collection = context.bucketStorage.db.sourceRecords(
      context.stream.replicationStreamId,
      context.table.id as bson.ObjectId
    );
    const partial = (await collection.find({}).toArray()).find((record) => record._id === 'missing');
    expect(partial != null).toBe(phase === 'after-commit');
    expect(await factory.db.sync_rules.findOne({ _id: context.stream.replicationStreamId })).toMatchObject({
      resume_lsn: '1/1',
      sync_configs: [{ last_checkpoint: 1n }]
    });
    aborted.mockRestore();
    uncertain.mockRestore();
    await expect(writer.setResumeLsn('1/2')).rejects.toThrow('restart replication');
    await test_utils.releaseTestStorageLease(factory, context.stream.replicationStreamId);
    const replayStorage = await getTestStorage(factory, (await factory.getReplicatingReplicationStreams())[0]);
    const repairs = vi.fn();
    await using replay = await replayStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      markRecordUnavailable: repairs
    });
    const replayTable = await test_utils.resolveTestTable(replay, 'items', ['id'], context.factoryGen, 1);
    await replay.save({
      sourceTable: replayTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'missing', description: undefined },
      afterReplicaId: 'missing'
    });
    await replay.flush();
    expect(repairs).toHaveBeenCalledTimes(1);
    await replay.save({
      sourceTable: replayTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'missing', description: 'repaired' },
      afterReplicaId: 'missing'
    });
    await replay.flush();
    await replay.commit('1/2');
    expect((await context.bucketStorage.getCheckpoint()).lsn).toBe('1/2');
  });

  test('a throwing repair callback stops all writers sharing the lease, including queued progress', async () => {
    const failure = new Error('repair callback failed');
    const callback = () => {
      throw failure;
    };
    const context = await open(version, false, { markRecordUnavailable: callback });
    await using factory = context.factory;
    await using writer = context.writer;
    await using other = await context.bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const load = SourceRecordStoreV3.prototype.loadDocuments;
    using _pause = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      const records = await load.apply(this, args);
      entered.resolve();
      await release.promise;
      return records;
    });
    await writer.save({
      sourceTable: context.table,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'missing', description: undefined },
      afterReplicaId: 'missing'
    });
    const flushing = expect(writer.flush()).rejects.toThrow(failure);
    await entered.promise;
    const queued = expect(other.setResumeLsn('1/2')).rejects.toThrow('restart replication');
    release.resolve();
    await Promise.all([flushing, queued]);
    await expect(writer.commit('1/2')).rejects.toThrow('restart replication');
    await expect(writer.keepalive('1/2')).rejects.toThrow('restart replication');
    await expect(writer.setResumeLsn('1/2')).rejects.toThrow('restart replication');
    await expect(other.commit('1/2')).rejects.toThrow('restart replication');
    expect(await factory.db.sync_rules.findOne({ _id: context.stream.replicationStreamId })).toMatchObject({
      resume_lsn: '1/1',
      sync_configs: [{ last_checkpoint: 1n }]
    });
    const repairs = vi.fn();
    await test_utils.releaseTestStorageLease(factory, context.stream.replicationStreamId);
    const restarted = await getTestStorage(factory, context.stream);
    await using replay = await restarted.createWriter({
      ...test_utils.BATCH_OPTIONS,
      markRecordUnavailable: repairs
    });
    await replay.save({
      sourceTable: context.table,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'missing', description: undefined },
      afterReplicaId: 'missing'
    });
    await replay.flush();
    expect(repairs).toHaveBeenCalledTimes(1);
  });

  test('discards an unfinished row notification when operation IDs run out', async () => {
    const repairs = vi.fn();
    const context = await open(version, false, { markRecordUnavailable: repairs });
    await using factory = context.factory;
    await using writer = context.writer;
    const allocator = factory.getOpIdAllocator(
      context.bucketStorage.replicationStream,
      context.bucketStorage.replicationStream.current_lock!
    );
    using sequence = vi.spyOn(allocator, 'sequence').mockImplementationOnce((head) => new MongoIdSequence(head, []));
    await writer.save({
      sourceTable: context.table,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'new', description: undefined },
      beforeReplicaId: 'old',
      afterReplicaId: 'new'
    });
    await writer.flush();
    expect(sequence).toHaveBeenCalledTimes(2);
    expect(repairs).toHaveBeenCalledTimes(1);
    expect(repairs.mock.calls[0][0].afterReplicaId).toBe('new');
  });

  test('disposing one writer preserves the lease session for other writers', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using writer = context.writer;
    const disposable = (await context.bucketStorage.createWriter(test_utils.BATCH_OPTIONS)) as MongoBucketBatchV3;
    const session = writer.session;
    expect(disposable.session).toBe(session);
    await disposable.dispose();
    expect(session.hasEnded).toBe(false);
    await save(writer, context.table, 'after writer disposal');
    await writer.commit('1/2');
    expect((await context.bucketStorage.getCheckpoint()).lsn).toBe('1/2');
    await test_utils.releaseTestStorageLease(factory, context.stream.replicationStreamId);
    expect(session.hasEnded).toBe(true);
  });

  test('lease release waits for active session use and rejects queued operations', async () => {
    const context = await open(version);
    await using _factory = context.factory;
    await using writer = context.writer;
    await using other = await context.bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const lease = context.bucketStorage.replicationStream.current_lock!;
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const load = SourceRecordStoreV3.prototype.loadDocuments;
    using _pause = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      const result = await load.apply(this, args);
      entered.resolve();
      await release.promise;
      return result;
    });
    await save(writer, context.table, 'abandoned during release');
    const publishing = expect(writer.flush()).rejects.toThrow('Replication lock released');
    await entered.promise;
    const queued = expect(other.getSourceTableStatus(context.table)).rejects.toThrow('Replication lock released');
    const releasing = lease.release();
    try {
      expect(writer.session.hasEnded).toBe(false);
      release.resolve();
      await Promise.all([publishing, queued, releasing]);
      expect(writer.session.hasEnded).toBe(true);
    } finally {
      release.resolve();
      await Promise.allSettled([publishing, queued, releasing]);
    }
  });

  test('uses the shared causal session across writers and majority preparation reads', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using first = context.writer;
    await using second = (await context.bucketStorage.createWriter(test_utils.BATCH_OPTIONS)) as MongoBucketBatchV3;
    expect(second.session).toBe(first.session);
    await save(first, context.table, 'second publication');
    await first.commit('1/2');
    const committedTime = first.session.operationTime!;
    const reads: mongo.Document[] = [];
    const observe = (event: mongo.CommandStartedEvent) => {
      if (
        (event.commandName === 'find' && event.command.find.startsWith('source_records_')) ||
        event.commandName === 'aggregate'
      ) {
        reads.push(event.command);
      }
    };
    factory.db.client.on('commandStarted', observe);
    try {
      await second.save({
        sourceTable: context.table,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: undefined, description: undefined, retained: 'new' },
        afterReplicaId: test_utils.rid('A')
      });
      await second.commit('1/3');
    } finally {
      factory.db.client.off('commandStarted', observe);
    }
    expect(reads).toHaveLength(2);
    for (const read of reads) {
      expect(read.lsid).toEqual(first.session.id);
      expect(read.autocommit).toBeUndefined();
      expect(read.readConcern.level).toBe('majority');
      expect(read.readConcern.afterClusterTime.compare(committedTime)).toBeGreaterThanOrEqual(0);
    }
    const [record] = await context.bucketStorage.db
      .sourceRecords(context.stream.replicationStreamId, context.table.id as bson.ObjectId)
      .find({})
      .toArray();
    expect(bson.deserialize(record.data!.buffer)).toEqual({
      id: 'A',
      description: 'second publication',
      retained: 'new'
    });
  });

  test.each(['metadata', 'truncate', 'drop'])(
    '%s waits for prepared work, including physical table cleanup',
    async (mutation) => {
      const context = await open(version);
      await using factory = context.factory;
      await using writer = context.writer;
      await using other = (await context.bucketStorage.createWriter(test_utils.BATCH_OPTIONS)) as MongoBucketBatchV3;
      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const read = SourceRecordStoreV3.prototype.loadDocuments;
      using _pause = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
        this: SourceRecordStoreV3,
        ...args
      ) {
        const result = await read.apply(this, args);
        entered.resolve();
        await release.promise;
        return result;
      });
      using commands = new PublicationCommandMonitor(factory.db.client);
      await save(writer, context.table, 'prepared');
      const publishing = writer.flush();
      const pending: Promise<unknown>[] = [publishing];
      try {
        await entered.promise;
        expect(commands.activeReplicationAttempts).toHaveLength(0);
        using transactions = vi.spyOn(other.session, 'withTransaction');
        pending.push(
          mutation === 'metadata'
            ? other.markTableSnapshotDone([context.table])
            : mutation === 'truncate'
              ? other.truncate([context.table])
              : other.drop([context.table])
        );
        expect(transactions).not.toHaveBeenCalled();
        release.resolve();
        await Promise.all(pending);
        expect(transactions).toHaveBeenCalled();
      } finally {
        release.resolve();
        await Promise.allSettled(pending);
      }
      if (mutation === 'drop') {
        expect(
          await context.bucketStorage.db.db
            .listCollections({
              name: context.bucketStorage.db.sourceRecords(
                context.stream.replicationStreamId,
                context.table.id as bson.ObjectId
              ).collectionName
            })
            .hasNext()
        ).toBe(false);
      }
    }
  );

  test('soft-delete cleanup holds admission until its database work finishes', async () => {
    const context = await open(version);
    await using _factory = context.factory;
    await using writer = context.writer;
    await using other = await context.bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const cleanup = SourceRecordStoreV3.prototype.postCommitCleanup;
    using _pause = vi.spyOn(SourceRecordStoreV3.prototype, 'postCommitCleanup').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      entered.resolve();
      await release.promise;
      return cleanup.apply(this, args);
    });
    const cleaning = other.commit('1/2');
    const pending: Promise<unknown>[] = [cleaning];
    try {
      await entered.promise;
      using read = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
      pending.push(save(writer, context.table, 'after cleanup').then(() => writer.flush()));
      expect(read).not.toHaveBeenCalled();
      release.resolve();
      await Promise.all(pending);
      expect(read).toHaveBeenCalledTimes(1);
    } finally {
      release.resolve();
      await Promise.allSettled(pending);
    }
  });

  test.each(['config', 'lease'])('rejects stale %s after preparation without rereading records', async (change) => {
    const context = await open(version, true);
    await using factory = context.factory;
    await using writer = context.writer;
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const original = SourceRecordStoreV3.prototype.loadDocuments;
    using read = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      const result = await original.apply(this, args);
      entered.resolve();
      await release.promise;
      return result;
    });
    using put = vi.spyOn(context.objectStorage, 'put');
    await save(writer, context.table, 'stale');
    const publishing = writer.flush();
    const rejected = expect(publishing).rejects.toThrow(change === 'lease' ? 'no longer owns' : 'reload');
    try {
      await entered.promise;
      await factory.db.sync_rules.updateOne(
        { _id: context.stream.replicationStreamId },
        {
          $set:
            change === 'lease'
              ? { 'lock.id': 'replacement-owner' }
              : { 'sync_configs.$[].state': storage.SyncRuleState.STOP }
        },
        { writeConcern: { w: 'majority' } }
      );
      release.resolve();
      await rejected;
      expect(read).toHaveBeenCalledTimes(1);
      // Ownership/config are revalidated after uploads. Work abandoned at that
      // boundary retains its marker instead of publishing a stale reference.
      expect(put).toHaveBeenCalledTimes(1);
      expect(
        await context.bucketStorage.db
          .pendingObjectStorageDeletes(context.stream.replicationStreamId)
          .countDocuments({ path: put.mock.calls[0][0] })
      ).toBe(1);
      expect(writer.last_flushed_op).toBe(1n);
    } finally {
      release.resolve();
      await Promise.allSettled([publishing]);
    }
  });

  test('uses fresh fenced snapshot metadata for the checkpoint decision', async () => {
    const context = await open(version);
    await using factory = context.factory;
    await using writer = context.writer;
    const read = SourceRecordStoreV3.prototype.loadDocuments;
    using update = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      const records = await read.apply(this, args);
      await factory.db.sync_rules.updateOne(
        { _id: context.stream.replicationStreamId },
        { $set: { 'sync_configs.$[].snapshot_done': false } },
        { writeConcern: { w: 'majority' } }
      );
      return records;
    });
    await save(writer, context.table, 'not visible yet');
    expect((await writer.commit('1/2')).checkpointBlocked).toBe(true);
    expect(writer.last_flushed_op).toBe(2n);
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    expect(await factory.db.sync_rules.findOne({ _id: context.stream.replicationStreamId })).toMatchObject({
      last_persisted_op: 2n,
      sync_configs: [{ last_checkpoint: 1n, last_checkpoint_lsn: '1/1', snapshot_done: false }]
    });
    await writer.markAllSnapshotDone('1/2');
    expect((await writer.commit('1/3')).checkpointBlocked).toBe(false);
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(2n);
    expect(update).toHaveBeenCalledTimes(1);
  });

  test('preserves read partitions and repeated-row state when a later data bulk rolls back', async () => {
    const context = await open(version, true);
    await using factory = context.factory;
    await using writer = context.writer;
    using _blocks = vi.spyOn(OperationBatch.prototype, 'batched').mockImplementation(function* (this: OperationBatch) {
      for (const op of this.batch) {
        yield [op];
      }
    });
    using reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    using monitor = new PublicationCommandMonitor(factory.db.client);
    const put = context.objectStorage.put.bind(context.objectStorage);
    using uploads = vi.spyOn(context.objectStorage, 'put').mockImplementation(async (...args) => {
      // All groups in this prefix must finish uploading before its first fence,
      // even though each group later emits a separate data bulk on every retry.
      expect(monitor.activeReplicationAttempts).toHaveLength(0);
      await put(...args);
    });
    const publish = PersistedBatchV3.prototype.publish;
    let writes = 0;
    using _retry = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementation(async function (
      this: PersistedBatchV3,
      ...args
    ) {
      const result = await publish.apply(this, args);
      if (++writes === 2) {
        throw new mongo.MongoServerError({
          message: 'rollback both blocks',
          code: 112,
          errorLabels: ['TransientTransactionError']
        });
      }
      return result;
    });
    await save(writer, context.table, 'first block');
    await writer.save({
      sourceTable: context.table,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: undefined, description: undefined, retained: 'second block' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.commit('1/2');
    expect(reads).toHaveBeenCalledTimes(1);
    expect(writes).toBe(4);
    expect(uploads).toHaveBeenCalledTimes(2);
    const db = context.bucketStorage.db;
    const [record] = await db
      .sourceRecords(context.stream.replicationStreamId, context.table.id as bson.ObjectId)
      .find({})
      .toArray();
    expect(bson.deserialize(record.data!.buffer)).toEqual({
      id: 'A',
      description: 'first block',
      retained: 'second block'
    });
    expect(await db.bucketState(context.stream.replicationStreamId).findOne({})).toMatchObject({
      last_op: 3n,
      bucket_stats: { count: 3, chunks: 3 }
    });
    expect((await context.bucketStorage.getCheckpoint()).checkpoint).toBe(3n);
  });

  test('a replacement lease publishes while the previous owner is preparing', async () => {
    const context = await open(version, true);
    await using factory = context.factory;
    await using writer = context.writer;
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const load = SourceRecordStoreV3.prototype.loadDocuments;
    using read = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      const result = await load.apply(this, args);
      entered.resolve();
      await release.promise;
      return result;
    });
    using put = vi.spyOn(context.objectStorage, 'put');
    await save(writer, context.table, 'previous owner');
    const pending = writer.flush();
    const rejected = expect(pending).rejects.toThrow('no longer owns');
    try {
      await entered.promise;
      await factory.db.sync_rules.updateOne(
        { _id: context.stream.replicationStreamId },
        { $set: { 'lock.expires_at': new Date(0) } }
      );
      await using replacement = await context.factoryGen.factory({ doNotClear: true });
      const stream = (await replacement.getReplicatingReplicationStreams())[0];
      const lease = await stream.lock();
      await using _leaseLifetime = { [Symbol.asyncDispose]: () => lease.release() };
      const storage = replacement.getInstance(stream, { replicationLock: lease });
      await using next = await storage.createWriter(test_utils.BATCH_OPTIONS);
      const table = await test_utils.resolveTestTable(next, 'items', ['id'], context.factoryGen, 1);
      await save(next, table, 'replacement owner');
      await next.commit('1/2');
      expect(next.last_flushed_op).toBeGreaterThan(65_536n);
      expect(put).toHaveBeenCalledTimes(1);
      release.resolve();
      await rejected;
      expect(put).toHaveBeenCalledTimes(2);
      expect(read).toHaveBeenCalledTimes(2);
      expect(writer.last_flushed_op).toBe(1n);
      const records = await context.bucketStorage.db
        .sourceRecords(context.stream.replicationStreamId, context.table.id as bson.ObjectId)
        .find({})
        .toArray();
      expect(bson.deserialize(records[0].data!.buffer).description).toBe('replacement owner');
    } finally {
      release.resolve();
      await Promise.allSettled([pending]);
    }
  });

  test('retains a snapshot tombstone observation if previous-owner cleanup removes it during preparation', async () => {
    const context = await open(version);
    await using _factory = context.factory;
    await using _writer = context.writer;
    const db = context.bucketStorage.db;
    const streamId = context.stream.replicationStreamId;
    const sourceId = context.table.id as bson.ObjectId;
    const collection = db.sourceRecords(streamId, sourceId);
    await collection.insertOne({ _id: 'deleted', data: null, buckets: [], lookups: [], pending_delete: 1n });
    await db.sourceTables(streamId).updateOne({ _id: sourceId }, { $set: { latest_pending_delete: 1n } });
    await using snapshot = await context.bucketStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      skipExistingRows: true
    });
    const cleanup = new SourceRecordStoreV3(
      db,
      streamId,
      context.bucketStorage.getParsedSyncConfigSet(test_utils.BATCH_OPTIONS).mapping
    );
    const load = SourceRecordStoreV3.prototype.loadDocuments;
    using read = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementationOnce(async function (
      this: SourceRecordStoreV3,
      ...args
    ) {
      const records = await load.apply(this, args);
      // A previous process does not share this lease's local admission mutex.
      await cleanup.postCommitCleanup(1n, context.stream.logger);
      expect((await collection.find({}).toArray()).find((row) => row._id === 'deleted') ?? null).toBeNull();
      return records;
    });
    await snapshot.save({
      sourceTable: context.table,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'deleted', description: 'old snapshot' },
      afterReplicaId: 'deleted'
    });
    await snapshot.flush();
    expect(read).toHaveBeenCalledTimes(1);
    expect((await collection.find({}).toArray()).find((row) => row._id === 'deleted') ?? null).toBeNull();
    expect(snapshot.last_flushed_op).toBe(1n);
  });

  test('previous-owner cleanup cannot delete a newly live record', async () => {
    const context = await open(version);
    await using _factory = context.factory;
    await using writer = context.writer;
    const db = context.bucketStorage.db;
    const streamId = context.stream.replicationStreamId;
    const sourceId = context.table.id as bson.ObjectId;
    const collection = db.sourceRecords(streamId, sourceId);
    await collection.insertOne({ _id: 'resurrected', data: null, buckets: [], lookups: [], pending_delete: 1n });
    await db.sourceTables(streamId).updateOne({ _id: sourceId }, { $set: { latest_pending_delete: 1n } });
    using _collections = vi.spyOn(db, 'sourceRecords').mockReturnValue(collection);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const deleteMany = collection.deleteMany.bind(collection);
    using _deletion = vi.spyOn(collection, 'deleteMany').mockImplementationOnce(async (...args) => {
      entered.resolve();
      await release.promise;
      return deleteMany(...args);
    });
    const cleanup = new SourceRecordStoreV3(
      db,
      streamId,
      context.bucketStorage.getParsedSyncConfigSet(test_utils.BATCH_OPTIONS).mapping
    );
    const cleaning = cleanup.postCommitCleanup(1n, context.stream.logger);
    try {
      await entered.promise;
      await writer.save({
        sourceTable: context.table,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'resurrected', description: 'live' },
        afterReplicaId: 'resurrected'
      });
      await writer.flush();
      release.resolve();
      await cleaning;
      const record = (await collection.find({}).toArray()).find((row) => row._id === 'resurrected');
      expect(record!.pending_delete).toBeUndefined();
      expect(bson.deserialize(record!.data!.buffer)).toEqual({ id: 'resurrected', description: 'live' });
    } finally {
      release.resolve();
      await Promise.allSettled([cleaning]);
    }
  });
});
