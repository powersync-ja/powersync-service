import { MongoSyncBucketStorageV3 } from '@module/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { ReplicationStreamDocumentV3 } from '../../src/storage/implementation/v3/models.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

function sourceDescriptor(name: string, options: { objectId?: string } = {}): storage.SourceEntityDescriptor {
  return {
    connectionTag: storage.SourceTable.DEFAULT_TAG,
    objectId: options.objectId ?? name,
    schema: 'public',
    name,
    replicaIdColumns: [
      {
        name: 'id',
        type: 'VARCHAR',
        typeId: 25
      }
    ]
  };
}

function objectIdGenerator(id: string) {
  let used = false;
  return () => {
    if (used) {
      throw new Error(`Can only generate a single id using ${id}`);
    }
    used = true;
    return new bson.ObjectId(id);
  };
}

async function collectionExists(db: VersionedPowerSyncMongoV3, name: string) {
  return await db.db.listCollections({ name }, { nameOnly: true }).hasNext();
}

async function cleanupStoppedSyncConfigs(bucketStorage: MongoSyncBucketStorageV3) {
  return bucketStorage.cleanupStoppedSyncConfigs({
    defaultSchema: 'public',
    sourceConnectionTag: storage.SourceTable.DEFAULT_TAG
  });
}

describe('cleanupStoppedSyncConfigs - mongodb', () => {
  test('cleans up unused stopped sync config storage', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();

    const first = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
config:
  edition: 3

streams:
  by_project:
    query: |
      SELECT id, project FROM "Scene"
      WHERE project IN (
        SELECT project FROM "ProjectInvitation"
        WHERE project = subscription.parameter('project')
      )
`,
        { storageVersion: 3 }
      )
    );
    const firstStorage = factory.getInstance(first) as MongoSyncBucketStorageV3;
    const db = firstStorage.db as VersionedPowerSyncMongoV3;
    await using firstWriter = await firstStorage.createWriter(test_utils.BATCH_OPTIONS);

    const sceneTable = (
      await firstWriter.resolveTables({
        connection_id: 1,
        source: sourceDescriptor('Scene', { objectId: 'scene-relation' }),
        idGenerator: objectIdGenerator('6544e3899293153fa7b38350')
      })
    ).tables[0];
    const invitationTable = (
      await firstWriter.resolveTables({
        connection_id: 1,
        source: sourceDescriptor('ProjectInvitation', { objectId: 'project-invitation-relation' }),
        idGenerator: objectIdGenerator('6544e3899293153fa7b38351')
      })
    ).tables[0];
    await firstWriter.save({
      sourceTable: sceneTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'scene-1',
        project: 'project-1'
      },
      afterReplicaId: test_utils.rid('scene-1')
    });
    await firstWriter.save({
      sourceTable: invitationTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'invitation-1',
        project: 'project-1'
      },
      afterReplicaId: test_utils.rid('invitation-1')
    });
    await firstWriter.markAllSnapshotDone('1/1');
    await firstWriter.commit('1/1');

    const stoppedMapping = first.syncConfigContent[0].mapping;
    const stoppedBucketDefinitionId = stoppedMapping.allBucketDefinitionIds()[0];
    const stoppedParameterIndexId = stoppedMapping.allParameterIndexIds()[0];
    expect(stoppedBucketDefinitionId).toBeDefined();
    expect(stoppedParameterIndexId).toBeDefined();
    const bucketDataCollection = db.bucketData(first.replicationStreamId, stoppedBucketDefinitionId!).collectionName;
    const parameterIndexCollection = db.parameterIndex(
      first.replicationStreamId,
      stoppedParameterIndexId!
    ).collectionName;
    const sceneRecordsCollection = db.sourceRecords(
      first.replicationStreamId,
      sceneTable.id as bson.ObjectId
    ).collectionName;
    const invitationRecordsCollection = db.sourceRecords(
      first.replicationStreamId,
      invitationTable.id as bson.ObjectId
    ).collectionName;
    expect(await collectionExists(db, bucketDataCollection)).toBe(true);
    expect(await collectionExists(db, parameterIndexCollection)).toBe(true);
    expect(await collectionExists(db, sceneRecordsCollection)).toBe(true);
    expect(await collectionExists(db, invitationRecordsCollection)).toBe(true);

    const second = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
config:
  edition: 3

streams:
  by_owner:
    query: SELECT * FROM todos WHERE owner_id = subscription.parameter('owner_id')
`,
        { storageVersion: 3 }
      )
    );
    expect(second.replicationStreamId).toBe(first.replicationStreamId);
    const replicatingStreams = await factory.getReplicatingReplicationStreams();
    const secondStorage = factory.getInstance(replicatingStreams[0]) as MongoSyncBucketStorageV3;
    await using secondWriter = await secondStorage.createWriter(test_utils.BATCH_OPTIONS);
    await secondWriter.markAllSnapshotDone('2/1');
    await secondWriter.commit('2/1');

    const result = await cleanupStoppedSyncConfigs(
      (await factory.getActiveSyncConfig())!.storage as MongoSyncBucketStorageV3
    );

    expect(result).toEqual({
      stoppedSyncConfigsRemoved: 1,
      bucketDataCollectionsDropped: 1,
      parameterIndexCollectionsDropped: 1,
      sourceRecordCollectionsDropped: 2,
      sourceTablesUpdated: 0,
      sourceTablesDeleted: 2
    });
    expect(await collectionExists(db, bucketDataCollection)).toBe(false);
    expect(await collectionExists(db, parameterIndexCollection)).toBe(false);
    expect(await collectionExists(db, sceneRecordsCollection)).toBe(false);
    expect(await collectionExists(db, invitationRecordsCollection)).toBe(false);
    expect(
      await db.sourceTables(first.replicationStreamId).countDocuments({ _id: sceneTable.id as bson.ObjectId })
    ).toBe(0);
    expect(
      await db.sourceTables(first.replicationStreamId).countDocuments({ _id: invitationTable.id as bson.ObjectId })
    ).toBe(0);

    const streamDoc = (await db.sync_rules.findOne({ _id: first.replicationStreamId })) as ReplicationStreamDocumentV3;
    expect(streamDoc.sync_configs.map((config) => config.state)).toEqual([storage.SyncRuleState.ACTIVE]);
  });

  test('keeps source table membership still used by a live sync config', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();

    const first = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
config:
  edition: 3

streams:
  by_owner:
    query: SELECT * FROM todos WHERE owner_id = subscription.parameter('owner_id')
`,
        { storageVersion: 3 }
      )
    );
    const firstStorage = factory.getInstance(first) as MongoSyncBucketStorageV3;
    const db = firstStorage.db as VersionedPowerSyncMongoV3;
    await using firstWriter = await firstStorage.createWriter(test_utils.BATCH_OPTIONS);
    const resolved = await firstWriter.resolveTables({
      connection_id: 1,
      source: sourceDescriptor('todos', { objectId: 'todos-relation' }),
      idGenerator: objectIdGenerator('6544e3899293153fa7b38352')
    });
    await firstWriter.markAllSnapshotDone('1/1');
    await firstWriter.commit('1/1');

    const sourceTableId = resolved.tables[0].id as bson.ObjectId;
    const sourceRecordsCollection = db.sourceRecords(first.replicationStreamId, sourceTableId).collectionName;
    const firstDefinitionId = first.syncConfigContent[0].mapping.allBucketDefinitionIds()[0];

    const second = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
config:
  edition: 3

streams:
  by_project:
    query: SELECT * FROM todos WHERE project_id = subscription.parameter('project_id')
`,
        { storageVersion: 3 }
      )
    );
    expect(second.replicationStreamId).toBe(first.replicationStreamId);
    const secondDefinitionId = second.syncConfigContent[1].mapping.allBucketDefinitionIds()[0];
    expect(secondDefinitionId).not.toBe(firstDefinitionId);

    const replicatingStreams = await factory.getReplicatingReplicationStreams();
    const secondStorage = factory.getInstance(replicatingStreams[0]) as MongoSyncBucketStorageV3;
    await using secondWriter = await secondStorage.createWriter(test_utils.BATCH_OPTIONS);
    await secondWriter.resolveTables({
      connection_id: 1,
      source: sourceDescriptor('todos', { objectId: 'todos-relation' }),
      idGenerator: objectIdGenerator('6544e3899293153fa7b38353')
    });
    await secondWriter.markAllSnapshotDone('2/1');
    await secondWriter.commit('2/1');
    await db.sourceTables(first.replicationStreamId).updateOne(
      { _id: sourceTableId },
      {
        $set: {
          bucket_data_source_ids: [firstDefinitionId, secondDefinitionId]
        }
      }
    );

    const result = await cleanupStoppedSyncConfigs(
      (await factory.getActiveSyncConfig())!.storage as MongoSyncBucketStorageV3
    );

    expect(result).toEqual({
      stoppedSyncConfigsRemoved: 1,
      bucketDataCollectionsDropped: 1,
      parameterIndexCollectionsDropped: 0,
      sourceRecordCollectionsDropped: 0,
      sourceTablesUpdated: 1,
      sourceTablesDeleted: 0
    });
    const sourceTable = await db.sourceTables(first.replicationStreamId).findOne({ _id: sourceTableId });
    expect(sourceTable?.bucket_data_source_ids).toEqual([secondDefinitionId]);
    expect(await collectionExists(db, sourceRecordsCollection)).toBe(true);
  });
});
