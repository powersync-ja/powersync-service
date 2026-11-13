import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test, vi } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { OperationBatch } from '../../src/storage/implementation/OperationBatch.js';
import {
  MAX_INPUT_BATCH_BYTES,
  MAX_INPUT_BATCH_COUNT
} from '../../src/storage/implementation/ReplicationBatchLimits.js';
import { SourceRecordStoreV3 } from '../../src/storage/implementation/v3/SourceRecordStoreV3.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { env } from './env.js';

const CONFIG = /* yaml */ `
  # Sync config fixture.
  config:
    edition: 3
  streams:
    items:
      query: SELECT * FROM items
  event_definitions:
    write_checkpoints:
      payloads:
        - SELECT user_id, checkpoint FROM checkpoints
`;

async function open(storageVersion: number) {
  const gen = mongoTestStorageFactoryGenerator({ url: env.MONGO_TEST_URL, isCI: env.CI });
  const factory = await gen.factory();
  const stream = await factory.updateSyncRules(updateSyncRulesFromYaml(CONFIG, { storageVersion }));
  const buckets = (await getTestStorage(factory, stream)) as MongoSyncBucketStorage;
  const observed: bigint[] = [];
  // A repair-capable writer must still allow a checkpoint-only commit: there are no rows to repair.
  const writer = await buckets.createWriter({
    ...test_utils.BATCH_OPTIONS,
    markRecordUnavailable: () => {},
    hooks: {
      afterBatchFlush: async () => {
        const checkpoint = await checkpoints.findOne({ user_id: 'user-1' });
        if (checkpoint != null) {
          observed.push(checkpoint.checkpoint);
        }
      }
    }
  });
  const table = await test_utils.resolveTestTable(writer, 'checkpoints', ['id'], gen, 1);
  const eventId = table.eventDefinitionIds?.values().next().value;
  const checkpoints =
    storageVersion >= 3
      ? (buckets.db as VersionedPowerSyncMongoV3).customCheckpointRequests({
          replicationStreamId: stream.replicationStreamId,
          eventId: eventId!
        })
      : factory.db.custom_write_checkpoints;
  writer.registerListener({
    replicationEvent: ({ batch, data, event_id }) => {
      batch.addCustomWriteCheckpoint({
        user_id: String(data.after!.user_id),
        checkpoint: data.after!.checkpoint as bigint,
        event_id
      });
    }
  });
  await writer.markAllSnapshotDone('1/1');
  await writer.commit('1/1');
  const save = (checkpoint: number) =>
    writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.INSERT,
      afterReplicaId: String(checkpoint),
      after: { id: String(checkpoint), user_id: 'user-1', checkpoint: BigInt(checkpoint) }
    });
  return { factory, stream, buckets, writer, table, checkpoints, observed, save };
}

test('checkpoint-only input is bounded by bytes and produces no row lookup blocks', () => {
  const input = new OperationBatch();
  input.addCustomWriteCheckpoint({ sync_rules_id: 1, user_id: 'x'.repeat(MAX_INPUT_BATCH_BYTES), checkpoint: 1n });
  expect(input.hasData()).toBe(true);
  expect(input.shouldFlush()).toBe(true);
  expect([...input.batched(undefined)]).toEqual([]);
});

describe.each([1, 2, 3, 4])('input batch v%i', (storageVersion) => {
  test('automatically flushes event-only checkpoints and commits the next checkpoint-only input', async () => {
    const c = await open(storageVersion);
    await using _factory = c.factory;
    await using writer = c.writer;
    expect(c.table.syncData).toBe(false);
    expect(c.table.syncParameters).toBe(false);
    expect(c.table.syncEvent).toBe(true);
    for (let i = 1; i < MAX_INPUT_BATCH_COUNT; i++) {
      await c.save(i);
    }
    expect(await c.checkpoints.findOne({ user_id: 'user-1' })).toBeNull();
    await c.save(MAX_INPUT_BATCH_COUNT);
    await writer.flush();
    expect(await c.checkpoints.findOne({ user_id: 'user-1' })).toMatchObject({
      checkpoint: BigInt(MAX_INPUT_BATCH_COUNT)
    });
    // The first input has detached. A new checkpoint must create and belong to a new input.
    await c.save(MAX_INPUT_BATCH_COUNT + 1);
    await writer.commit('1/2');
    expect(await c.checkpoints.findOne({ user_id: 'user-1' })).toMatchObject({
      checkpoint: BigInt(MAX_INPUT_BATCH_COUNT + 1)
    });
    expect((await c.buckets.getCheckpoint()).lsn).toBe('1/2');
  });
});

describe.each([3, 4])('checkpoint input ownership v%i', (storageVersion) => {
  test('metadata includes a checkpoint-only writer that has never called save', async () => {
    const c = await open(storageVersion);
    await using _factory = c.factory;
    await using writer = c.writer;
    await using other = await c.buckets.createWriter(test_utils.BATCH_OPTIONS);
    const eventId = c.table.eventDefinitionIds!.values().next().value!;
    other.addCustomWriteCheckpoint({ user_id: 'user-1', checkpoint: 5n, event_id: eventId });
    expect(await c.checkpoints.findOne({ user_id: 'user-1' })).toBeNull();

    // A metadata read must see a consistent state across every live writer, including
    // input added directly without a preceding save() call.
    await writer.getSourceTableStatus(c.table);
    expect(await c.checkpoints.findOne({ user_id: 'user-1' })).toMatchObject({ checkpoint: 5n });
    expect(c.stream.current_lock!.replicationPipeline!.pendingCount).toBe(0);
  });

  test('metadata captures checkpoints before queued publication, leaving later events in a fresh input', async () => {
    const c = await open(storageVersion);
    await using _factory = c.factory;
    await using writer = c.writer;
    await c.save(1);
    const session = c.stream.current_lock!.replicationPipeline!.publicationSession;
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const transaction = session.withTransaction.bind(session);
    using _transaction = vi.spyOn(session, 'withTransaction').mockImplementationOnce(async (...args) => {
      entered.resolve();
      await release.promise;
      return transaction(...args);
    });
    const metadata = writer.getSourceTableStatus(c.table);
    try {
      await entered.promise;
      await c.save(2);
    } finally {
      release.resolve();
      await metadata;
    }
    expect(await c.checkpoints.findOne({ user_id: 'user-1' })).toMatchObject({ checkpoint: 1n });
    await writer.commit('1/2');
    expect(await c.checkpoints.findOne({ user_id: 'user-1' })).toMatchObject({ checkpoint: 2n });
    expect(c.observed).toEqual([1n, 2n]);
  });

  test('keeps the admitted checkpoints stable through retry while the next input fills', async () => {
    const c = await open(storageVersion);
    await using _factory = c.factory;
    await using writer = c.writer;
    const pipeline = c.stream.current_lock!.replicationPipeline!;
    const session = pipeline.publicationSession;
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const transaction = session.withTransaction.bind(session);
    let attempts = 0;
    using _transaction = vi.spyOn(session, 'withTransaction').mockImplementationOnce(async (callback, options) => {
      entered.resolve();
      await release.promise;
      return transaction(async (activeSession) => {
        const result = await callback(activeSession);
        if (++attempts == 1) {
          throw new mongo.MongoServerError({
            message: 'retry checkpoint publication',
            code: 112,
            errorLabels: ['TransientTransactionError']
          });
        }
        return result;
      }, options);
    });
    using _reads = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments');
    let flushing: Promise<unknown> | undefined;
    try {
      for (let i = 1; i <= MAX_INPUT_BATCH_COUNT; i++) {
        await c.save(i);
      }
      await entered.promise;
      await c.save(MAX_INPUT_BATCH_COUNT + 1);
      expect(await c.checkpoints.findOne({ user_id: 'user-1' })).toBeNull();
      // Start the barrier while the next input is already populated.
      flushing = writer.flush();
    } finally {
      release.resolve();
      await flushing;
    }
    expect(attempts).toBe(2);
    expect(c.observed).toEqual([BigInt(MAX_INPUT_BATCH_COUNT), BigInt(MAX_INPUT_BATCH_COUNT + 1)]);
    expect(await c.checkpoints.findOne({ user_id: 'user-1' })).toMatchObject({
      checkpoint: BigInt(MAX_INPUT_BATCH_COUNT + 1)
    });
    expect(await c.checkpoints.countDocuments()).toBe(1);
    expect(_reads).not.toHaveBeenCalled();
    expect(pipeline.pendingCount).toBe(0);
  });
});
