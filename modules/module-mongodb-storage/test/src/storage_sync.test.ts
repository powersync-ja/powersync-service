import { deserializeParameterLookup, JwtPayload, storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, register, test_utils } from '@powersync/service-core-tests';
import { RequestParameters } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { MongoBucketStorage } from '../../src/storage/MongoBucketStorage.js';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { SourceRecordStoreV3 } from '../../src/storage/implementation/v3/SourceRecordStoreV3.js';
import type { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import {
  CurrentBucketV3,
  ReplicationStreamDocumentV3,
  SyncConfigDefinition
} from '../../src/storage/implementation/v3/models.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

function registerSyncStorageTests(storageConfig: storage.TestStorageConfig, storageVersion: number) {
  register.registerSyncTests(storageConfig.factory, {
    storageVersion,
    tableIdStrings: storageConfig.tableIdStrings
  });
  // The split of returned results can vary depending on storage drivers
  test('large batch (2)', async () => {
    // Test syncing a batch of data that is small in count,
    // but large enough in size to be split over multiple returned chunks.
    // Similar to the above test, but splits over 1MB chunks.
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      global:
        data:
          - SELECT id, description FROM "%"
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

    const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);

    const largeDescription = '0123456789'.repeat(2_000_00);

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'test1',
        description: 'test1'
      },
      afterReplicaId: test_utils.rid('test1')
    });

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'large1',
        description: largeDescription
      },
      afterReplicaId: test_utils.rid('large1')
    });

    // Large enough to split the returned batch
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'large2',
        description: largeDescription
      },
      afterReplicaId: test_utils.rid('large2')
    });

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'test3',
        description: 'test3'
      },
      afterReplicaId: test_utils.rid('test3')
    });

    const flushResult = await writer.flush();

    const checkpoint = flushResult!.flushed_op;

    const options: storage.BucketDataBatchOptions = {};
    const batch1 = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRules, 'global[]', 0n)], options)
    );
    expect(test_utils.getBatchData(batch1)).toEqual([
      { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 },
      { op_id: '2', op: 'PUT', object_id: 'large1', checksum: 1178768505 }
    ]);
    expect(test_utils.getBatchMeta(batch1)).toEqual({
      after: '0',
      has_more: true,
      next_after: '2'
    });

    const batch2 = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(
        checkpoint,
        [bucketRequest(syncRules, 'global[]', batch1[0].chunkData.next_after)],
        options
      )
    );
    expect(test_utils.getBatchData(batch2)).toEqual([
      { op_id: '3', op: 'PUT', object_id: 'large2', checksum: 1607205872 }
    ]);
    expect(test_utils.getBatchMeta(batch2)).toEqual({
      after: '2',
      has_more: true,
      next_after: '3'
    });

    const batch3 = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(
        checkpoint,
        [bucketRequest(syncRules, 'global[]', batch2[0].chunkData.next_after)],
        options
      )
    );
    expect(test_utils.getBatchData(batch3)).toEqual([
      { op_id: '4', op: 'PUT', object_id: 'test3', checksum: 1359888332 }
    ]);
    expect(test_utils.getBatchMeta(batch3)).toEqual({
      after: '3',
      has_more: false,
      next_after: '4'
    });

    // Test that the checksum type is correct.
    // Specifically, test that it never persisted as double.
    const mongoFactory = factory as MongoBucketStorage;
    const checksumTypes =
      storageVersion >= 3
        ? (
            await Promise.all(
              (
                await mongoFactory.db.db
                  .listCollections({ name: new RegExp(`^bucket_data_${syncRules.id}_`) }, { nameOnly: true })
                  .toArray()
              ).map((collection: { name: string }) =>
                mongoFactory.db.db
                  .collection(collection.name)
                  .aggregate([{ $group: { _id: { $type: '$checksum' }, count: { $sum: 1 } } }])
                  .toArray()
              )
            )
          ).flat()
        : await mongoFactory.db.bucket_data
            .aggregate([{ $group: { _id: { $type: '$checksum' }, count: { $sum: 1 } } }])
            .toArray();
    expect(checksumTypes).toEqual([{ _id: 'long', count: 4 }]);
  });

  test.runIf(storageVersion >= 3)('uses v3 mongodb model shapes', async () => {
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      global:
        parameters:
          - SELECT owner_id FROM test WHERE id = token_parameters.test
        data:
          - SELECT id, description, owner_id FROM test WHERE id = bucket.owner_id
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'shape-check',
        description: 'shape',
        owner_id: 'user-1'
      },
      afterReplicaId: test_utils.rid('shape-check')
    });
    await writer.markAllSnapshotDone('1/1');
    await writer.commit('1/1');

    const checkpoint = await bucketStorage.getCheckpoint();
    const parameters = new RequestParameters(new JwtPayload({ sub: 'u1', parameters: { test: 'shape-check' } }), {});
    const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;
    const buckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        expect(lookups.map((l) => l.indexKey)).toEqual([['shape-check']]);
        expect(lookups[0].indexId).toEqual('1');

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        expect(parameter_sets).toEqual([{ owner_id: 'user-1' }]);
        return parameter_sets;
      }
    });
    expect(buckets.map((b) => b.bucket)).toEqual([bucketRequest(syncRules, 'global["user-1"]').bucket]);

    const mongoFactory = factory as MongoBucketStorage;
    const db = (bucketStorage as MongoSyncBucketStorage).db as VersionedPowerSyncMongoV3;
    const currentDataCollections = await db.listSourceRecordCollectionsV3(syncRules.id);
    const currentData = await currentDataCollections[0]?.findOne({});
    const firstBucket: CurrentBucketV3 | undefined = currentData?.buckets[0] as CurrentBucketV3 | undefined;
    expect(firstBucket?.def).toMatch(/^[0-9a-f]+$/);

    const bucketCollections = await mongoFactory.db.db
      .listCollections({ name: new RegExp(`^bucket_data_${syncRules.id}_`) }, { nameOnly: true })
      .toArray();
    expect(
      bucketCollections.some((collection) => collection.name === `bucket_data_${syncRules.id}_${firstBucket?.def}`)
    ).toBe(true);

    const syncRule = (await mongoFactory.db.sync_rules.findOne({ _id: syncRules.id })) as ReplicationStreamDocumentV3;
    const syncConfig = await db.syncConfigDefinitions.findOne({ _id: syncRule.sync_configs[0]._id });
    const ruleMapping: SyncConfigDefinition['rule_mapping'] | undefined = syncConfig?.rule_mapping;
    expect(Object.keys(ruleMapping?.definitions ?? {})).not.toHaveLength(0);

    const parameterIndexId = Object.values(ruleMapping?.parameter_indexes ?? {})[0] as string | undefined;
    expect(parameterIndexId).toBeDefined();
    const parameterEntry = await db.parameterIndexV3(syncRules.id, parameterIndexId!).findOne({});
    expect(deserializeParameterLookup(parameterEntry!.lookup)).toEqual(['shape-check']);
  });

  test.runIf(storageVersion < 3)('uses a single current_data collection for v1 source records', async () => {
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      global:
        data:
          - SELECT id, description FROM test
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'shape-check',
        description: 'shape'
      },
      afterReplicaId: test_utils.rid('shape-check')
    });
    await writer.markAllSnapshotDone('1/1');
    await writer.commit('1/1');

    const mongoFactory = factory as MongoBucketStorage;
    expect(await mongoFactory.db.current_data.countDocuments({ '_id.g': syncRules.id })).toBe(1);

    const sourceRecordCollections = await mongoFactory.db.db
      .listCollections({ name: new RegExp(`^source_records_${syncRules.id}_`) }, { nameOnly: true })
      .toArray();
    expect(sourceRecordCollections).toEqual([]);
  });

  test.runIf(storageVersion < 3)('clear removes v1 current_data rows', async () => {
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      global:
        data:
          - SELECT id, description FROM test
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'clear-check',
        description: 'shape'
      },
      afterReplicaId: test_utils.rid('clear-check')
    });
    await writer.markAllSnapshotDone('1/1');
    await writer.commit('1/1');

    const mongoFactory = factory as MongoBucketStorage;
    expect(await mongoFactory.db.current_data.countDocuments({ '_id.g': syncRules.id })).toBe(1);

    await bucketStorage.clear();

    expect(await mongoFactory.db.current_data.countDocuments({ '_id.g': syncRules.id })).toBe(0);
  });

  test.runIf(storageVersion < 3)('storage metrics include v1 current_data', async () => {
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      global:
        data:
          - SELECT id, description FROM test
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);
    const metricsBefore = await factory.getStorageMetrics();

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'metric-check',
        description: 'shape'
      },
      afterReplicaId: test_utils.rid('metric-check')
    });
    await writer.markAllSnapshotDone('1/1');
    await writer.commit('1/1');

    const mongoFactory = factory as MongoBucketStorage;
    expect(await mongoFactory.db.current_data.countDocuments({ '_id.g': syncRules.id })).toBe(1);

    const metricsAfter = await factory.getStorageMetrics();
    expect(metricsAfter.replication_size_bytes).toBeGreaterThan(metricsBefore.replication_size_bytes);
  });

  test.runIf(storageVersion >= 3)(
    'loads parameter checkpoint changes across all v3 parameter index collections',
    async () => {
      await using factory = await storageConfig.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(
          `
    bucket_definitions:
      by_owner:
        parameters:
          - SELECT owner_id FROM test WHERE id = token_parameters.owner_lookup
        data:
          - SELECT id, owner_id FROM test WHERE owner_id = bucket.owner_id
      by_category:
        parameters:
          - SELECT category_id FROM test WHERE id = token_parameters.category_lookup
        data:
          - SELECT id, category_id FROM test WHERE category_id = bucket.category_id
    `,
          { storageVersion }
        )
      );
      const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
      const previousCheckpoint = await bucketStorage.getCheckpoint();

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);

      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'shape-check',
          owner_id: 'user-1',
          category_id: 'cat-1'
        },
        afterReplicaId: test_utils.rid('shape-check')
      });
      await writer.markAllSnapshotDone('1/1');
      await writer.commit('1/1');

      const nextCheckpoint = await bucketStorage.getCheckpoint();
      const changes = await bucketStorage.getCheckpointChanges({
        lastCheckpoint: previousCheckpoint,
        nextCheckpoint
      });

      expect(changes.invalidateParameterBuckets).toBe(false);
      expect(changes.updatedParameterLookups).toEqual(new Set(['["1","","shape-check"]', '["2","","shape-check"]']));
    }
  );

  test.runIf(storageVersion >= 3)('cleans pending deletes only for tracked v3 source tables', async () => {
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      global:
        data:
          - SELECT id, description FROM test
    `,
        { storageVersion }
      )
    );

    const mongoFactory = factory as MongoBucketStorage;
    const bucketStorage = mongoFactory.getInstance(syncRules) as any;
    const db = bucketStorage.db;
    await db.initializeStreamStorage(syncRules.id);

    const sourceTableA = new bson.ObjectId();
    const sourceTableB = new bson.ObjectId();
    await db.sourceTablesV3(syncRules.id).insertMany([
      {
        _id: sourceTableA,
        connection_id: 1,
        relation_id: 'a',
        schema_name: 'public',
        table_name: 'table_a',
        replica_id_columns: null,
        replica_id_columns2: [],
        snapshot_done: true,
        snapshot_status: undefined,
        bucket_data_source_ids: [],
        parameter_lookup_source_ids: [],
        latest_pending_delete: 9n
      },
      {
        _id: sourceTableB,
        connection_id: 1,
        relation_id: 'b',
        schema_name: 'public',
        table_name: 'table_b',
        replica_id_columns: null,
        replica_id_columns2: [],
        snapshot_done: true,
        snapshot_status: undefined,
        bucket_data_source_ids: [],
        parameter_lookup_source_ids: [],
        latest_pending_delete: 12n
      }
    ]);

    await db.sourceRecordsV3(syncRules.id, sourceTableA).insertMany([
      { _id: 'deleted-1', data: null, buckets: [], lookups: [], pending_delete: 5n },
      { _id: 'deleted-2', data: null, buckets: [], lookups: [], pending_delete: 9n },
      { _id: 'active', data: null, buckets: [], lookups: [] }
    ]);
    await db
      .sourceRecordsV3(syncRules.id, sourceTableB)
      .insertMany([{ _id: 'later-delete', data: null, buckets: [], lookups: [], pending_delete: 12n }]);

    const store = new SourceRecordStoreV3(db, syncRules.id, bucketStorage.sync_rules.mapping);
    const logger = { info() {} } as any;

    await store.postCommitCleanup(6n, logger);

    expect(await db.sourceRecordsV3(syncRules.id, sourceTableA).countDocuments({ pending_delete: 5n })).toBe(0);
    expect(await db.sourceRecordsV3(syncRules.id, sourceTableA).countDocuments({ pending_delete: 9n })).toBe(1);
    expect(await db.sourceRecordsV3(syncRules.id, sourceTableB).countDocuments({ pending_delete: 12n })).toBe(1);
    expect((await db.sourceTablesV3(syncRules.id).findOne({ _id: sourceTableA }))?.latest_pending_delete).toBe(9n);
    expect((await db.sourceTablesV3(syncRules.id).findOne({ _id: sourceTableB }))?.latest_pending_delete).toBe(12n);

    await store.postCommitCleanup(10n, logger);

    expect(
      await db.sourceRecordsV3(syncRules.id, sourceTableA).countDocuments({ pending_delete: { $exists: true } })
    ).toBe(0);
    expect(
      (await db.sourceTablesV3(syncRules.id).findOne({ _id: sourceTableA }))?.latest_pending_delete
    ).toBeUndefined();
    expect((await db.sourceTablesV3(syncRules.id).findOne({ _id: sourceTableB }))?.latest_pending_delete).toBe(12n);
  });
}

describe('sync - mongodb', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    describe(`storage v${storageVersion}`, () => {
      registerSyncStorageTests(INITIALIZED_MONGO_STORAGE_FACTORY, storageVersion);
    });
  }
});
