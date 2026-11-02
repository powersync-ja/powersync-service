import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { expect, test } from 'vitest';
import { getTestStorage } from '../test-utils/leased-storage.js';
import * as test_utils from '../test-utils/test-utils-index.js';
import { compactActive } from './util.js';

const SNAPSHOT_SYNC_CONFIG = /* yaml */ `
  # sync config fixture.
  config:
    edition: 3
  streams:
    synced:
      query: SELECT * FROM synced_table
    dynamic:
      query: SELECT * FROM data_table WHERE owner_id IN (SELECT user_id FROM parameter_table WHERE user_id = auth.user_id())
    wildcard:
      query: SELECT * FROM "wildcard_%"
  event_definitions:
    changes:
      payloads:
        - SELECT * FROM event_table
`;

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

  test('snapshot completion - unused tables create no snapshot work', async () => {
    await using factory = await generateStorageFactory();
    const stream = await factory.updateSyncRules(
      updateSyncRulesFromYaml(SNAPSHOT_SYNC_CONFIG, { validate: true, storageVersion })
    );
    const bucketStorage = await getTestStorage(factory, stream);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

    // Discover an unused table without performing snapshot work for it.
    const unused = await writer.resolveTables({
      connection_id: 1,
      source: sourceDescriptor('unused_table'),
      idGenerator: () => {
        throw new Error('An unused table must not allocate a persisted source-table id');
      }
    });
    expect(unused).toEqual({ tables: [], dropTables: [] });
    await writer.markSnapshotDone('1/2');

    // Checkpoints must wait until replication reaches the snapshot's consistency boundary.
    expect((await writer.keepalive('1/1')).checkpointBlocked).toBe(true);
    expect((await writer.keepalive('1/2')).checkpointBlocked).toBe(false);
    expect((await bucketStorage.getStatus()).snapshotDone).toBe(true);
  });

  test.each([
    { tableName: 'synced_table', syncData: true, syncParameters: false, syncEvent: false },
    { tableName: 'wildcard_tasks', syncData: true, syncParameters: false, syncEvent: false },
    { tableName: 'parameter_table', syncData: false, syncParameters: true, syncEvent: false },
    { tableName: 'event_table', syncData: false, syncParameters: false, syncEvent: true }
  ])(
    'snapshot completion - keeps $tableName as a blocker',
    async ({ tableName, syncData, syncParameters, syncEvent }) => {
      await using factory = await generateStorageFactory();
      const stream = await factory.updateSyncRules(
        updateSyncRulesFromYaml(SNAPSHOT_SYNC_CONFIG, { validate: true, storageVersion })
      );
      const bucketStorage = await getTestStorage(factory, stream);
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

      // Discover an unused table and a required table whose snapshot is still pending.
      await writer.resolveTables({ connection_id: 1, source: sourceDescriptor('unused_table') });
      const pending = await writer.resolveTables({ connection_id: 1, source: sourceDescriptor(tableName) });
      expect(pending.tables).toHaveLength(1);
      expect(pending.tables[0]).toMatchObject({ syncData, syncParameters, syncEvent, snapshotComplete: false });

      // Both conflict modes must keep checkpoints blocked while required snapshot work remains.
      await writer.markSnapshotDone('1/2', { throwOnConflict: false });
      expect((await writer.keepalive('1/2')).checkpointBlocked).toBe(true);
      await expect(writer.markSnapshotDone('1/2')).rejects.toThrow(`Tables: public.${tableName}`);
      expect((await writer.keepalive('1/2')).checkpointBlocked).toBe(true);

      // Complete only the required table, then advance replication to the checkpoint boundary.
      await writer.markTableSnapshotDone(pending.tables);
      await writer.markSnapshotDone('1/2');
      expect((await writer.keepalive('1/2')).checkpointBlocked).toBe(false);
      expect((await bucketStorage.getStatus()).snapshotDone).toBe(true);
    }
  );

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
    const bucketStorage = await getTestStorage(factory, r.persisted_sync_rules!);

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({
        user_id: 'user1',
        signal: abortController.signal
      })
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
    const bucketStorage = await getTestStorage(factory, r.persisted_sync_rules!);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({
        user_id: 'user1',
        signal: abortController.signal
      })
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
    // This is what is effectively triggered by RouteAPI.createReplicationHead().
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
    const bucketStorage = await getTestStorage(factory, r.persisted_sync_rules!);

    const first = await createManagedWriteCheckpointResult(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '5/0' },
      checkpoint_request_id: 42n
    });
    expect(first.writeCheckpoints.get('user1')).toEqual(42n);
    expect(first.shouldAdvance).toBe(true);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '5/0' } })).resolves.toEqual(42n);

    const stale = await createManagedWriteCheckpointResult(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '8/0' },
      checkpoint_request_id: 41n
    });
    expect(stale.writeCheckpoints.get('user1')).toEqual(42n);
    // The stored checkpoint is unchanged but has not been processed by replication
    // yet, so the source marker must still be forced (e.g. for a retry after a
    // previous attempt failed to advance the marker).
    expect(stale.shouldAdvance).toBe(true);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '6/0' } })).resolves.toEqual(42n);

    const retried = await createManagedWriteCheckpointResult(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '8/0' },
      checkpoint_request_id: 42n
    });
    expect(retried.writeCheckpoints.get('user1')).toEqual(42n);
    // Same as above: the duplicate request still points at a pending checkpoint.
    expect(retried.shouldAdvance).toBe(true);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '6/0' } })).resolves.toEqual(42n);

    const advanced = await createManagedWriteCheckpointResult(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '8/0' },
      checkpoint_request_id: 43n
    });
    expect(advanced.writeCheckpoints.get('user1')).toEqual(43n);
    expect(advanced.shouldAdvance).toBe(true);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '7/0' } })).resolves.toBeNull();
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '8/0' } })).resolves.toEqual(43n);

    const generated = await createManagedWriteCheckpointResult(bucketStorage, {
      user_id: 'user1',
      heads: { '1': '9/0' }
    });
    expect(generated.writeCheckpoints.get('user1')).toEqual(44n);
    expect(generated.shouldAdvance).toBe(true);
  });

  test.runIf((storageVersion ?? 1) < 3)('custom write checkpoints - checkpoint after write', async (context) => {
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
    const bucketStorage = await getTestStorage(factory, r.persisted_sync_rules!);
    bucketStorage.setWriteCheckpointMode({
      mode: storage.WriteCheckpointMode.CUSTOM
    });

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({
        user_id: 'user1',
        signal: abortController.signal
      })
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

  test.runIf((storageVersion ?? 1) < 3)('custom write checkpoints - standalone checkpoint', async (context) => {
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
    const bucketStorage = await getTestStorage(factory, r.persisted_sync_rules!);
    bucketStorage.setWriteCheckpointMode({
      mode: storage.WriteCheckpointMode.CUSTOM
    });

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({
        user_id: 'user1',
        signal: abortController.signal
      })
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

  test.runIf((storageVersion ?? 1) < 3)('custom write checkpoints - write after checkpoint', async (context) => {
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
    const bucketStorage = await getTestStorage(factory, r.persisted_sync_rules!);
    bucketStorage.setWriteCheckpointMode({
      mode: storage.WriteCheckpointMode.CUSTOM
    });

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/1');

    const abortController = new AbortController();
    context.onTestFinished(() => abortController.abort());
    const iter = bucketStorage
      .watchCheckpointChanges({
        user_id: 'user1',
        signal: abortController.signal
      })
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

  test.runIf((storageVersion ?? 1) < 3)(
    'custom write checkpoints - checkpoint request markers are temporary',
    async () => {
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
      const bucketStorage = await getTestStorage(factory, r.persisted_sync_rules!);
      bucketStorage.setWriteCheckpointMode({
        mode: storage.WriteCheckpointMode.CUSTOM
      });

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      await writer.markAllSnapshotDone('1/1');

      writer.addCustomWriteCheckpoint({
        checkpoint: 5n,
        user_id: 'persistent'
      });
      writer.addCustomWriteCheckpoint({
        checkpoint: 6n,
        user_id: 'temporary',
        checkpoint_requested_at: new Date('2024-01-01T00:00:00.000Z')
      });
      await writer.flush();
      await writer.keepalive('1/1');

      await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'persistent' })).resolves.toEqual(5n);
      await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'temporary' })).resolves.toEqual(6n);

      await compactActive(factory, {
        compactBuckets: [],
        deleteCheckpointRequestsBefore: new Date(Date.now() + 1_000)
      });

      await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'persistent' })).resolves.toEqual(5n);
      await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'temporary' })).resolves.toBeNull();
    }
  );
}

function sourceDescriptor(name: string): storage.SourceEntityDescriptor {
  return {
    connectionTag: storage.SourceTable.DEFAULT_TAG,
    schema: 'public',
    name,
    objectId: name,
    replicaIdColumns: [{ name: 'id', type: 'text', typeId: 25 }]
  };
}

async function createManagedWriteCheckpoint(
  bucketStorage: storage.SyncRulesBucketStorage,
  checkpoint: storage.ManagedWriteCheckpointOptions
) {
  const result = await createManagedWriteCheckpointResult(bucketStorage, checkpoint);
  const writeCheckpoint = result.writeCheckpoints.get(checkpoint.user_id);
  expect(writeCheckpoint).not.toBeUndefined();
  return writeCheckpoint!;
}

async function createManagedWriteCheckpointResult(
  bucketStorage: storage.SyncRulesBucketStorage,
  checkpoint: storage.ManagedWriteCheckpointOptions
) {
  const result = await bucketStorage.createManagedWriteCheckpoints([checkpoint]);
  expect(result.writeCheckpoints.get(checkpoint.user_id)).not.toBeUndefined();
  return result;
}
