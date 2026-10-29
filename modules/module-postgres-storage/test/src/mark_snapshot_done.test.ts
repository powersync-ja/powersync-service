import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import { randomUUID } from 'node:crypto';
import { describe, expect, test } from 'vitest';
import { PostgresBucketStorageFactory } from '../../src/storage/PostgresBucketStorageFactory.js';
import { POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

const SYNC_CONFIG = /* yaml */ `
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

const SOURCE_CONNECTION_ID = 1;

async function deployTestStorage(factory: PostgresBucketStorageFactory, storageVersion: number) {
  const stream = await factory.updateSyncRules(
    updateSyncRulesFromYaml(SYNC_CONFIG, { validate: true, storageVersion })
  );
  return getTestStorage(factory, stream);
}

/**
 * Seed the pending state left by older versions, without resolving the unused table again.
 */
async function seedPendingTable(factory: PostgresBucketStorageFactory, streamId: number, tableName: string) {
  const id = randomUUID();
  await factory.db.sql`
    INSERT INTO source_tables (id, group_id, connection_id, schema_name, table_name, snapshot_done)
    VALUES (
      ${{ type: 'varchar', value: id }},
      ${{ type: 'int4', value: streamId }},
      ${{ type: 'int4', value: SOURCE_CONNECTION_ID }},
      'public',
      ${{ type: 'varchar', value: tableName }},
      FALSE
    )
  `.execute();
  return id;
}

async function globalSnapshotDone(factory: PostgresBucketStorageFactory, streamId: number) {
  const row = await factory.db.sql`
    SELECT snapshot_done FROM sync_rules WHERE id = ${{ type: 'int4', value: streamId }}
  `.first<{ snapshot_done: boolean }>();
  return row!.snapshot_done;
}

for (const storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`Postgres snapshot completion - v${storageVersion}`, () => {
    test('ignores a persisted unused table while preserving the checkpoint boundary', async () => {
      await using factory = await POSTGRES_STORAGE_FACTORY.factory();
      const bucketStorage = await deployTestStorage(factory, storageVersion);
      const tableId = await seedPendingTable(factory, bucketStorage.replicationStreamId, 'unused_table');
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

      await writer.markSnapshotDone('1/2');
      expect(await globalSnapshotDone(factory, bucketStorage.replicationStreamId)).toBe(true);
      expect((await writer.keepalive('1/1')).checkpointBlocked).toBe(true);
      expect((await writer.keepalive('1/2')).checkpointBlocked).toBe(false);
      expect((await bucketStorage.getStatus()).snapshotDone).toBe(true);

      const row = await factory.db.sql`
        SELECT snapshot_done FROM source_tables WHERE id = ${{ type: 'varchar', value: tableId }}
      `.first<{ snapshot_done: boolean }>();
      expect(row!.snapshot_done).toBe(false);
    });

    test.each([
      { tableName: 'synced_table', syncData: true, syncParameters: false, syncEvent: false },
      { tableName: 'wildcard_tasks', syncData: true, syncParameters: false, syncEvent: false },
      { tableName: 'parameter_table', syncData: false, syncParameters: true, syncEvent: false },
      { tableName: 'event_table', syncData: false, syncParameters: false, syncEvent: true }
    ])('keeps $tableName as a blocker', async ({ tableName, syncData, syncParameters, syncEvent }) => {
      await using factory = await POSTGRES_STORAGE_FACTORY.factory();
      const bucketStorage = await deployTestStorage(factory, storageVersion);
      await seedPendingTable(factory, bucketStorage.replicationStreamId, 'unused_table');
      const id = await seedPendingTable(factory, bucketStorage.replicationStreamId, tableName);
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

      const pending = await writer.getSourceTableStatus(
        new storage.SourceTable({
          id,
          ref: { connectionTag: storage.SourceTable.DEFAULT_TAG, schema: 'public', name: tableName },
          objectId: undefined,
          replicaIdColumns: [],
          snapshotComplete: false,
          bucketDataSources: [],
          parameterLookupSources: []
        })
      );
      expect(pending).toMatchObject({ syncData, syncParameters, syncEvent });
      await writer.markSnapshotDone('1/2', { throwOnConflict: false });
      expect(await globalSnapshotDone(factory, bucketStorage.replicationStreamId)).toBe(false);
      await expect(writer.markSnapshotDone('1/2')).rejects.toThrow(`Tables: public.${tableName}`);
      expect(await globalSnapshotDone(factory, bucketStorage.replicationStreamId)).toBe(false);

      await writer.markTableSnapshotDone([pending!]);
      await writer.markSnapshotDone('1/2');
      expect(await globalSnapshotDone(factory, bucketStorage.replicationStreamId)).toBe(true);
    });
  });
}
