import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
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
export function registerDataStorageCheckpointTests(config: storage.TestStorageConfig) {
  const generateStorageFactory = config.factory;
  const storageVersion = config.storageVersion;

  test('managed write checkpoints - checkpoint after write', async (context) => {
    await using factory = await generateStorageFactory();
    const r = await factory.configureSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    data: []
    `,
        {
          validate: false,
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');
    await writer.flush();

    const writeCheckpoint = await bucketStorage.createManagedWriteCheckpoint({
      heads: { '1': '5/0' },
      user_id: 'user1'
    });

    await using writer2 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer2.keepalive('5/0');
    await writer2.flush();

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
    const r = await factory.configureSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    data: []
    `,
        {
          validate: false,
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');
    await writer.flush();

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await using writer2 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer2.keepalive('5/0');
    await writer2.flush();

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
    await using writer3 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer3.keepalive('6/0');
    await writer3.flush();

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
    const r = await factory.configureSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    data: []
    `,
        {
          validate: false,
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);
    bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');
    await writer.flush();

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await using writer2 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer2.addCustomWriteCheckpoint({
      checkpoint: 5n,
      user_id: 'user1'
    });
    await writer2.flush();
    await writer2.keepalive('5/0');
    await writer2.flush();

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
    const r = await factory.configureSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    data: []
    `,
        {
          validate: false,
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);
    bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');
    await writer.flush();

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await using writer2 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    // Flush to clear state
    await writer2.flush();

    await writer2.addCustomWriteCheckpoint({
      checkpoint: 5n,
      user_id: 'user1'
    });
    await writer2.flush();
    await writer2.keepalive('5/0');
    await writer2.flush();

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
    const r = await factory.configureSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    data: []
    `,
        {
          validate: false,
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(r.persisted_sync_rules!);
    bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');
    await writer.flush();

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await using writer2 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer2.keepalive('5/0');
    await writer2.flush();

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

    await using writer3 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    writer3.addCustomWriteCheckpoint({
      checkpoint: 6n,
      user_id: 'user1'
    });
    await writer3.flush();
    await writer3.keepalive('6/0');
    await writer3.flush();

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

    await using writer4 = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    writer4.addCustomWriteCheckpoint({
      checkpoint: 7n,
      user_id: 'user1'
    });
    await writer4.flush();
    await writer4.keepalive('7/0');
    await writer4.flush();

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
