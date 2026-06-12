import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { describe, expect, it } from 'vitest';
import type { MikroOrmBucketStorageFactory } from '../../src/index.js';
import { MIKRO_ORM_SQLITE_STORAGE_FACTORY } from './util.js';

describe('MikroORM SyncRules storage', () => {
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
    await using factory = (await MIKRO_ORM_SQLITE_STORAGE_FACTORY.factory()) as MikroOrmBucketStorageFactory;
    const stream = await factory.updateSyncRules(syncRules);
    const bucketStorage = factory.getInstance(stream);

    const first = await bucketStorage.createManagedWriteCheckpoint({
      user_id: 'user1',
      heads: { '1': '5/0' }
    });
    const second = await bucketStorage.createManagedWriteCheckpoint({
      user_id: 'user1',
      heads: { '1': '6/0' }
    });

    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '4/0' } })).resolves.toBeNull();
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '5/0' } })).resolves.toBe(first);
    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1', heads: { '1': '6/0' } })).resolves.toBe(second);
  });

  it('watches checkpoint and managed write checkpoint changes', async () => {
    await using factory = (await MIKRO_ORM_SQLITE_STORAGE_FACTORY.factory()) as MikroOrmBucketStorageFactory;
    const stream = await factory.updateSyncRules(syncRules);
    const bucketStorage = factory.getInstance(stream);
    const abortController = new AbortController();

    try {
      const iterator = bucketStorage
        .watchCheckpointChanges({ user_id: 'user1', signal: abortController.signal })
        [Symbol.asyncIterator]();

      const writeCheckpoint = await bucketStorage.createManagedWriteCheckpoint({
        user_id: 'user1',
        heads: { '1': '5/0' }
      });

      const em = factory.orm.em.fork();
      const row = await em.findOneOrFail(factory.dialect.syncRulesEntity, {
        id: stream.replicationStreamId
      });
      em.assign(row, {
        lastCheckpoint: 0n,
        lastCheckpointLsn: '5/0'
      });
      await em.flush();
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
    await using factory = (await MIKRO_ORM_SQLITE_STORAGE_FACTORY.factory()) as MikroOrmBucketStorageFactory;
    const stream = await factory.updateSyncRules(syncRules);
    const bucketStorage = factory.getInstance(stream);
    bucketStorage.setWriteCheckpointMode(storage.WriteCheckpointMode.CUSTOM);

    const em = factory.orm.em.fork();
    const row = em.create(factory.dialect.writeCheckpointEntity, {
      id: 'custom-user1',
      syncRulesId: stream.replicationStreamId,
      userId: 'user1',
      checkpoint: 42n,
      heads: null,
      createdAt: new Date()
    });
    em.persist(row);
    await em.flush();

    await expect(bucketStorage.lastWriteCheckpoint({ user_id: 'user1' })).resolves.toBe(42n);
  });

  it('marks newly resolved source tables as requiring an initial snapshot', async () => {
    await using factory = (await MIKRO_ORM_SQLITE_STORAGE_FACTORY.factory()) as MikroOrmBucketStorageFactory;
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
    await using factory = (await MIKRO_ORM_SQLITE_STORAGE_FACTORY.factory()) as MikroOrmBucketStorageFactory;
    const stream = await factory.updateSyncRules(syncRules);

    await expect(factory.getReplicatingReplicationStreams()).resolves.toMatchObject([
      {
        replicationStreamId: stream.replicationStreamId,
        state: storage.SyncRuleState.PROCESSING
      }
    ]);

    const em = factory.orm.em.fork();
    const row = await em.findOneOrFail(factory.dialect.syncRulesEntity, {
      id: stream.replicationStreamId
    });
    em.assign(row, { state: storage.SyncRuleState.ACTIVE });
    await em.flush();

    await expect(factory.getReplicatingReplicationStreams()).resolves.toMatchObject([
      {
        replicationStreamId: stream.replicationStreamId,
        state: storage.SyncRuleState.ACTIVE
      }
    ]);
  });
});
