import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { eq } from 'drizzle-orm';
import { describe, expect, it } from 'vitest';
import type { DrizzleBucketStorageFactory } from '../../src/index.js';
import { DRIZZLE_SQLITE_STORAGE_FACTORY } from './util.js';

describe('Drizzle SyncRules storage', () => {
  const syncRules = updateSyncRulesFromYaml(
    `
bucket_definitions:
  mybucket:
    data: []
`,
    {
      validate: false
    }
  );

  it('stores and resolves managed write checkpoints', async () => {
    await using factory = (await DRIZZLE_SQLITE_STORAGE_FACTORY.factory()) as DrizzleBucketStorageFactory;
    const stream = await factory.updateSyncRules(syncRules);
    const bucketStorage = factory.getInstance(stream);

    const first = (
      await bucketStorage.createManagedWriteCheckpoints([{ user_id: 'user1', heads: { '1': '5/0' } }])
    ).get('user1')!;
    const second = (
      await bucketStorage.createManagedWriteCheckpoints([{ user_id: 'user1', heads: { '1': '6/0' } }])
    ).get('user1')!;

    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '4/0' } })).resolves.toBeNull();
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '5/0' } })).resolves.toBe(first);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '6/0' } })).resolves.toBe(second);
  });

  it('watches checkpoint and managed write checkpoint changes', async () => {
    await using factory = (await DRIZZLE_SQLITE_STORAGE_FACTORY.factory()) as DrizzleBucketStorageFactory;
    const stream = await factory.updateSyncRules(syncRules);
    const bucketStorage = factory.getInstance(stream);
    const abortController = new AbortController();

    try {
      const iterator = bucketStorage
        .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
        [Symbol.asyncIterator]();

      const writeCheckpoint = (
        await bucketStorage.createManagedWriteCheckpoints([{ user_id: 'user1', heads: { '1': '5/0' } }])
      ).get('user1')!;

      factory.dialect.db
        .update(factory.dialect.tables.syncRules)
        .set({
          lastCheckpoint: 0n,
          lastCheckpointLsn: '5/0'
        })
        .where(eq(factory.dialect.tables.syncRules.id, stream.replicationStreamId))
        .run();
      factory.checkpointWatcher.notify();

      await expect(iterator.next()).resolves.toMatchObject({
        done: false,
        value: {
          base: {
            checkpoint: 0n,
            lsn: '5/0'
          },
          writeCheckpoint
        }
      });
    } finally {
      abortController.abort();
    }
  });

  it('resolves custom write checkpoints from the write checkpoint entity', async () => {
    await using factory = (await DRIZZLE_SQLITE_STORAGE_FACTORY.factory()) as DrizzleBucketStorageFactory;
    const stream = await factory.updateSyncRules(syncRules);
    const bucketStorage = factory.getInstance(stream);
    bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);

    factory.dialect.db
      .insert(factory.dialect.tables.writeCheckpoints)
      .values({
        id: 'custom-user1',
        syncRulesId: stream.replicationStreamId,
        userId: 'user1',
        checkpoint: 42n,
        heads: null,
        createdAt: new Date()
      })
      .run();

    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1' })).resolves.toBe(42n);
  });

  it('marks newly resolved source tables as requiring an initial snapshot', async () => {
    await using factory = (await DRIZZLE_SQLITE_STORAGE_FACTORY.factory()) as DrizzleBucketStorageFactory;
    const stream = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  global:
    data:
      - SELECT * FROM lists
`,
        {
          validate: false
        }
      )
    );
    const bucketStorage = factory.getInstance(stream);
    await using writer = await bucketStorage.createWriter({
      defaultSchema: 'public',
      zeroLSN: '0/0',
      storeCurrentData: true
    });

    const resolved = await writer.resolveTables({
      connection_id: 1,
      source: {
        connectionTag: storage.SourceTable.DEFAULT_TAG,
        objectId: 123,
        schema: 'public',
        name: 'lists',
        replicaIdColumns: [{ name: 'id', type: 'VARCHAR', typeId: 25 }]
      },
      idGenerator: () => 'lists-table'
    });

    expect(resolved.tables[0]?.snapshotComplete).toBe(false);

    await writer.markTableSnapshotDone(resolved.tables, '0/1');
    const resolvedAgain = await writer.resolveTables({
      connection_id: 1,
      source: {
        connectionTag: storage.SourceTable.DEFAULT_TAG,
        objectId: 123,
        schema: 'public',
        name: 'lists',
        replicaIdColumns: [{ name: 'id', type: 'VARCHAR', typeId: 25 }]
      }
    });

    expect(resolvedAgain.tables[0]?.snapshotComplete).toBe(true);
  });

  it('returns active and processing streams as replicating streams', async () => {
    await using factory = (await DRIZZLE_SQLITE_STORAGE_FACTORY.factory()) as DrizzleBucketStorageFactory;
    const stream = await factory.updateSyncRules(syncRules);

    await expect(factory.getReplicatingReplicationStreams()).resolves.toMatchObject([
      {
        replicationStreamId: stream.replicationStreamId,
        state: storage.SyncRuleState.PROCESSING
      }
    ]);

    factory.dialect.db
      .update(factory.dialect.tables.syncRules)
      .set({ state: storage.SyncRuleState.ACTIVE })
      .where(eq(factory.dialect.tables.syncRules.id, stream.replicationStreamId))
      .run();

    await expect(factory.getReplicatingReplicationStreams()).resolves.toMatchObject([
      {
        replicationStreamId: stream.replicationStreamId,
        state: storage.SyncRuleState.ACTIVE
      }
    ]);
  });
});
