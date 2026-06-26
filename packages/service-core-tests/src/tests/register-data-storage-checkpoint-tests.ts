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

    const writeCheckpoint = await createManagedWriteCheckpoint(bucketStorage, {
      heads: { '1': '5/0' },
      user_id: 'user1'
    });

    await writer.keepalive('5/0');

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

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await writer.keepalive('5/0');

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

    const writeCheckpoint = await createManagedWriteCheckpoint(bucketStorage, {
      heads: { '1': '6/0' },
      user_id: 'user1'
    });
    // We have to trigger a new keepalive after the checkpoint, at least to cover postgres storage.
    // This is what is effetively triggered with RouteAPI.createReplicationHead().
    // MongoDB storage doesn't explicitly need this anymore.
    await writer.keepalive('6/0');

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

  test('managed write checkpoints - client supplied ids are monotonic', async () => {
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

    const first = await createManagedWriteCheckpoint(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '5/0' },
      checkpoint_request_id: 42n
    });
    expect(first).toEqual(42n);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '5/0' } })).resolves.toEqual(42n);

    const stale = await createManagedWriteCheckpoint(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '8/0' },
      checkpoint_request_id: 41n
    });
    expect(stale).toEqual(42n);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '6/0' } })).resolves.toEqual(42n);

    const retried = await createManagedWriteCheckpoint(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '8/0' },
      checkpoint_request_id: 42n
    });
    expect(retried).toEqual(42n);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '6/0' } })).resolves.toEqual(42n);

    const advanced = await createManagedWriteCheckpoint(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '8/0' },
      checkpoint_request_id: 43n
    });
    expect(advanced).toEqual(43n);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '7/0' } })).resolves.toBeNull();
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '8/0' } })).resolves.toEqual(43n);

    const generated = await createManagedWriteCheckpoint(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '9/0' }
    });
    expect(generated).toEqual(44n);
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

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    writer.addCustomWriteCheckpoint({
      checkpoint: 5n,
      user_id: 'user1'
    });
    await writer.flush();
    await writer.keepalive('5/0');

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

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    // Flush to clear state
    await writer.flush();

    writer.addCustomWriteCheckpoint({
      checkpoint: 5n,
      user_id: 'user1'
    });
    await writer.flush();
    await writer.keepalive('5/0');

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

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
      [Symbol.asyncIterator]();

    await writer.keepalive('5/0');

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

    writer.addCustomWriteCheckpoint({
      checkpoint: 6n,
      user_id: 'user1'
    });
    await writer.flush();
    await writer.keepalive('6/0');

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

    writer.addCustomWriteCheckpoint({
      checkpoint: 7n,
      user_id: 'user1'
    });
    await writer.flush();
    await writer.keepalive('7/0');

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

async function createManagedWriteCheckpoint(
  bucketStorage: storage.SyncRulesBucketStorage,
  checkpoint: storage.ManagedWriteCheckpointOptions
) {
  const checkpoints = await bucketStorage.createManagedWriteCheckpoints([checkpoint]);
  const writeCheckpoint = checkpoints.get(checkpoint.user_id);
  expect(writeCheckpoint).not.toBeUndefined();
  return writeCheckpoint!;
}
