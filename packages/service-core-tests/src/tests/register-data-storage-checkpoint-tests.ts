import { storage } from '@powersync/service-core';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';

/**
 * @example
 * ```TypeScript
 *
 * describe('store - mongodb', function () {
 *  registerDataStorageCheckpointTests(MONGO_STORAGE_FACTORY);
 * });
 *
 * ```
 */
export function registerDataStorageCheckpointTests(generateStorageFactory: storage.TestStorageFactory) {
  test('managed write checkpoints - checkpoint after write', async (context) => {
    await using factory = await generateStorageFactory();
    const r = await factory.configureSyncRules({
      content: `
bucket_definitions:
  mybucket:
    data: []
    `,
      validate: false
    });
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
    });

    const writeCheckpoint = await bucketStorage.createManagedWriteCheckpoint({
      heads: { '1': '5/0' },
      user_id: 'user1'
    });

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.keepalive('5/0');
    });

    const result = await iter.next();
    expect(result).toMatchObject({
      done: false,
      value: {
        base: {
          checkpoint: 0n,
          lsn: '5/0'
        },
        writeCheckpoint: writeCheckpoint
      }
    });
  });

  test('managed write checkpoints - write after checkpoint', async (context) => {
    await using factory = await generateStorageFactory();
    const r = await factory.configureSyncRules({
      content: `
bucket_definitions:
  mybucket:
    data: []
    `,
      validate: false
    });
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
    });

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.keepalive('5/0');
    });

    const result = await iter.next();
    expect(result).toMatchObject({
      done: false,
      value: {
        base: {
          checkpoint: 0n,
          lsn: '5/0'
        },
        writeCheckpoint: null
      }
    });

    const writeCheckpoint = await bucketStorage.createManagedWriteCheckpoint({
      heads: { '1': '6/0' },
      user_id: 'user1'
    });
    // We have to trigger a new keepalive after the checkpoint, at least to cover postgres storage.
    // This is what is effetively triggered with RouteAPI.createReplicationHead().
    // MongoDB storage doesn't explicitly need this anymore.
    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.keepalive('6/0');
    });

    let result2 = await iter.next();
    if (result2.value?.base?.lsn == '5/0') {
      // Events could arrive in a different order in some cases - this caters for it
      result2 = await iter.next();
    }
    expect(result2).toMatchObject({
      done: false,
      value: {
        base: {
          checkpoint: 0n,
          lsn: '6/0'
        },
        writeCheckpoint: writeCheckpoint
      }
    });
  });

  test('custom write checkpoints - checkpoint after write', async (context) => {
    await using factory = await generateStorageFactory();
    const r = await factory.configureSyncRules({
      content: `
bucket_definitions:
  mybucket:
    data: []
    `,
      validate: false
    });
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);
    bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
    });

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.addCustomWriteCheckpoint({
        checkpoint: 5n,
        user_id: 'user1'
      });
      await batch.flush();
      await batch.keepalive('5/0');
    });

    const result = await iter.next();
    expect(result).toMatchObject({
      done: false,
      value: {
        base: {
          lsn: '5/0'
        },
        writeCheckpoint: 5n
      }
    });
  });

  test('custom write checkpoints - standalone checkpoint', async (context) => {
    await using factory = await generateStorageFactory();
    const r = await factory.configureSyncRules({
      content: `
bucket_definitions:
  mybucket:
    data: []
    `,
      validate: false
    });
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);
    bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
    });

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Flush to clear state
      await batch.flush();

      await batch.addCustomWriteCheckpoint({
        checkpoint: 5n,
        user_id: 'user1'
      });
      await batch.flush();
      await batch.keepalive('5/0');
    });

    const result = await iter.next();
    expect(result).toMatchObject({
      done: false,
      value: {
        base: {
          lsn: '5/0'
        },
        writeCheckpoint: 5n
      }
    });
  });

  test('custom write checkpoints - write after checkpoint', async (context) => {
    await using factory = await generateStorageFactory();
    const r = await factory.configureSyncRules({
      content: `
bucket_definitions:
  mybucket:
    data: []
    `,
      validate: false
    });
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);
    bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
    });

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.keepalive('5/0');
    });

    const result = await iter.next();
    expect(result).toMatchObject({
      done: false,
      value: {
        base: {
          lsn: '5/0'
        },
        writeCheckpoint: null
      }
    });

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      batch.addCustomWriteCheckpoint({
        checkpoint: 6n,
        user_id: 'user1'
      });
      await batch.flush();
      await batch.keepalive('6/0');
    });

    let result2 = await iter.next();
    expect(result2).toMatchObject({
      done: false,
      value: {
        base: {
          // can be 5/0 or 6/0 - actual value not relevant for custom write checkpoints
          // lsn: '6/0'
        },
        writeCheckpoint: 6n
      }
    });

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      batch.addCustomWriteCheckpoint({
        checkpoint: 7n,
        user_id: 'user1'
      });
      await batch.flush();
      await batch.keepalive('7/0');
    });

    let result3 = await iter.next();
    expect(result3).toMatchObject({
      done: false,
      value: {
        base: {
          // can be 5/0, 6/0 or 7/0 - actual value not relevant for custom write checkpoints
          // lsn: '7/0'
        },
        writeCheckpoint: 7n
      }
    });
  });
}
