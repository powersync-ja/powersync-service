import { deserializeParameterLookup, JwtPayload, storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, register, test_utils } from '@powersync/service-core-tests';
import { DEFAULT_HYDRATION_STATE, RequestParameters, SqlSyncRules } from '@powersync/service-sync-rules';
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

function sourceDescriptor(
  name: string,
  options: {
    objectId?: string;
    replicaIdColumns?: string[];
  } = {}
): storage.SourceEntityDescriptor {
  return {
    connectionTag: storage.SourceTable.DEFAULT_TAG,
    objectId: options.objectId ?? name,
    schema: 'public',
    name,
    replicaIdColumns: (options.replicaIdColumns ?? ['id']).map((column) => ({
      name: column,
      type: 'VARCHAR',
      typeId: 25
    }))
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

function hydratedRulesFor(yaml: string) {
  const parsed = SqlSyncRules.fromYaml(yaml, test_utils.PARSE_OPTIONS);
  expect(parsed.errors).toEqual([]);
  return parsed.config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE });
}

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

  test('resolveTables populates matching data and parameter sources', async () => {
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      by_owner:
        parameters:
          - SELECT owner_id FROM test WHERE id = token_parameters.test_id
        data:
          - SELECT id, owner_id FROM test WHERE owner_id = bucket.owner_id
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);

    expect(sourceTable.bucketDataSources).toHaveLength(1);
    expect(sourceTable.parameterLookupSources).toHaveLength(1);
    expect(sourceTable.syncData).toBe(true);
    expect(sourceTable.syncParameters).toBe(true);
    expect(sourceTable.syncEvent).toBe(false);
  });

  test('resolveTables drops old table when table name changes for the same objectId', async () => {
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      global:
        data:
          - SELECT id FROM "%"
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const before = await writer.resolveTables({
      connection_id: 1,
      source: sourceDescriptor('orders', { objectId: 'orders-relation' }),
      idGenerator: objectIdGenerator('6544e3899293153fa7b38342')
    });
    const after = await writer.resolveTables({
      connection_id: 1,
      source: sourceDescriptor('renamed_orders', { objectId: 'orders-relation' }),
      idGenerator: objectIdGenerator('6544e3899293153fa7b38343')
    });

    expect(after.tables).toHaveLength(1);
    expect(after.tables[0].id).not.toEqual(before.tables[0].id);
    expect(after.tables[0].bucketDataSources).toHaveLength(1);
    expect(after.tables[0].parameterLookupSources).toHaveLength(0);
    expect(after.dropTables.map((table) => ({ id: table.id, name: table.name }))).toEqual([
      { id: before.tables[0].id, name: 'orders' }
    ]);
    expect(after.dropTables[0].bucketDataSources).toHaveLength(1);
    expect(after.dropTables[0].parameterLookupSources).toHaveLength(0);
  });

  test('resolveTables drops old table when objectId changes for the same table name', async () => {
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      global:
        data:
          - SELECT id FROM "%"
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const before = await writer.resolveTables({
      connection_id: 1,
      source: sourceDescriptor('accounts', { objectId: 'accounts-relation-old' }),
      idGenerator: objectIdGenerator('6544e3899293153fa7b38344')
    });
    const after = await writer.resolveTables({
      connection_id: 1,
      source: sourceDescriptor('accounts', { objectId: 'accounts-relation-new' }),
      idGenerator: objectIdGenerator('6544e3899293153fa7b38345')
    });

    expect(after.tables).toHaveLength(1);
    expect(after.tables[0].id).not.toEqual(before.tables[0].id);
    expect(after.tables[0].bucketDataSources).toHaveLength(1);
    expect(after.dropTables.map((table) => ({ id: table.id, objectId: table.objectId }))).toEqual([
      { id: before.tables[0].id, objectId: 'accounts-relation-old' }
    ]);
    expect(after.dropTables[0].bucketDataSources).toHaveLength(1);
  });

  test('resolveTables drops old table when replica id columns change', async () => {
    await using factory = await storageConfig.factory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
    bucket_definitions:
      global:
        data:
          - SELECT id FROM "%"
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const before = await writer.resolveTables({
      connection_id: 1,
      source: sourceDescriptor('items', { objectId: 'items-relation', replicaIdColumns: ['id'] }),
      idGenerator: objectIdGenerator('6544e3899293153fa7b38346')
    });
    const after = await writer.resolveTables({
      connection_id: 1,
      source: sourceDescriptor('items', { objectId: 'items-relation', replicaIdColumns: ['tenant_id', 'id'] }),
      idGenerator: objectIdGenerator('6544e3899293153fa7b38347')
    });

    expect(after.tables).toHaveLength(1);
    expect(after.tables[0].id).not.toEqual(before.tables[0].id);
    expect(after.tables[0].replicaIdColumns.map((column) => column.name)).toEqual(['tenant_id', 'id']);
    expect(after.tables[0].bucketDataSources).toHaveLength(1);
    expect(
      after.dropTables.map((table) => ({ id: table.id, columns: table.replicaIdColumns.map((c) => c.name) }))
    ).toEqual([{ id: before.tables[0].id, columns: ['id'] }]);
    expect(after.dropTables[0].bucketDataSources).toHaveLength(1);
  });

  test.runIf(storageVersion >= 3)(
    'resolveTables resolves v3 event-only tables without source memberships',
    async () => {
      await using factory = await storageConfig.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(
          `
    bucket_definitions:
      by_owner:
        data:
          - SELECT id FROM users

    event_definitions:
      write_checkpoints:
        payloads:
          - SELECT user_id, checkpoint FROM checkpoints
    `,
          { storageVersion }
        )
      );
      const bucketStorage = factory.getInstance(syncRules);

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const resolved = await writer.resolveTables({
        connection_id: 1,
        source: {
          connectionTag: storage.SourceTable.DEFAULT_TAG,
          objectId: 'checkpoints',
          schema: 'public',
          name: 'checkpoints',
          replicaIdColumns: [{ name: 'id', type: 'VARCHAR', typeId: 25 }]
        },
        idGenerator: () => new bson.ObjectId('6544e3899293153fa7b38341')
      });

      expect(resolved.tables).toHaveLength(1);
      expect(resolved.dropTables).toHaveLength(0);
      expect(resolved.tables[0].bucketDataSources).toEqual([]);
      expect(resolved.tables[0].parameterLookupSources).toEqual([]);
      expect(resolved.tables[0].syncData).toBe(false);
      expect(resolved.tables[0].syncParameters).toBe(false);
      expect(resolved.tables[0].syncEvent).toBe(true);
    }
  );

  test.runIf(storageVersion >= 3)('resolveTables handles v3 source membership additions and removals', async () => {
    // Tests the behavior of resolveTables when bucket data sources and parameter index creators are added or removed.
    // These are not end-to-end tests yet, since we don't have a full incremental reprocessing implementation.
    // This just tests the specific resolveTables behavior.

    // The same tests should work with sync streams, but legacy bucket_definitions make it easy
    // to see the distinction between the parameter index queries and the data sources.
    const fullRulesYaml = `
    bucket_definitions:
      by_owner:
        parameters:
          - SELECT owner_id FROM memberships WHERE id = token_parameters.test_id
        data:
          - SELECT id, owner_id FROM memberships WHERE owner_id = bucket.owner_id
    `;
    const dataOnlyRulesYaml = `
    bucket_definitions:
      by_owner:
        parameters:
          - SELECT token_parameters.owner_id as owner_id
        data:
          - SELECT id, owner_id FROM memberships WHERE owner_id = bucket.owner_id
    `;
    const parameterOnlyRulesYaml = `
    bucket_definitions:
      by_owner:
        parameters:
          - SELECT owner_id FROM memberships WHERE id = token_parameters.test_id
        data: []
    `;
    const eventOnlyRulesYaml = `
    bucket_definitions: {}

    event_definitions:
      write_checkpoints:
        payloads:
          - SELECT id, owner_id FROM memberships
    `;

    await using factory = await storageConfig.factory();
    // This does not quite match what actual API usage would look like.
    // Here we're persisting one sync config, then resolving tables with others.
    // We're also using the default hydration state for them all.
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(fullRulesYaml, { storageVersion }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const fullRules = hydratedRulesFor(fullRulesYaml);
    const dataOnlyRules = hydratedRulesFor(dataOnlyRulesYaml);
    const parameterOnlyRules = hydratedRulesFor(parameterOnlyRulesYaml);
    const eventOnlyRules = hydratedRulesFor(eventOnlyRulesYaml);
    const source = sourceDescriptor('memberships', { objectId: 'memberships-relation' });
    const dataOnlyTableId = new bson.ObjectId('6544e3899293153fa7b38348');
    const addedParameterTableId = new bson.ObjectId('6544e3899293153fa7b38349');
    const removedDataTableId = new bson.ObjectId('6544e3899293153fa7b3834a');

    const dataOnly = await writer.resolveTables({
      connection_id: 1,
      source,
      idGenerator: () => dataOnlyTableId,
      syncRules: dataOnlyRules
    });
    expect(dataOnly.tables.map((table) => table.id)).toEqual([dataOnlyTableId]);
    expect(dataOnly.dropTables.map((table) => table.id)).toEqual([]);
    expect(dataOnly.tables[0].bucketDataSources).toHaveLength(1);
    expect(dataOnly.tables[0].parameterLookupSources).toHaveLength(0);

    const addedParameter = await writer.resolveTables({
      connection_id: 1,
      source,
      idGenerator: () => addedParameterTableId,
      syncRules: fullRules
    });
    // Adding a definition always creates a new SourceTable
    expect(addedParameter.tables.map((table) => table.id)).toEqual([dataOnlyTableId, addedParameterTableId]);
    expect(addedParameter.tables.map((table) => table.bucketDataSources.length).sort()).toEqual([0, 1]);
    expect(addedParameter.tables.map((table) => table.parameterLookupSources.length).sort()).toEqual([0, 1]);
    expect(addedParameter.dropTables.map((table) => table.id)).toEqual([]);

    const removedParameter = await writer.resolveTables({
      connection_id: 1,
      source,
      idGenerator: () => {
        throw new Error('data-only resolve should reuse existing v3 source table');
      },
      syncRules: dataOnlyRules
    });
    expect(removedParameter.tables.map((table) => table.id)).toEqual([dataOnlyTableId]);
    // Now this sourceTable is unused & dropped
    expect(removedParameter.dropTables.map((table) => table.id)).toEqual([addedParameterTableId]);
    expect(removedParameter.tables[0].bucketDataSources).toHaveLength(1);
    expect(removedParameter.tables[0].parameterLookupSources).toHaveLength(0);
    await writer.drop(removedParameter.dropTables);

    const removedData = await writer.resolveTables({
      connection_id: 1,
      source,
      idGenerator: () => removedDataTableId,
      syncRules: parameterOnlyRules
    });

    // This goes from dataOnlyRules -> parameterOnlyRules, which adds one definition and removes another.
    // This generates a new SourceTable again, and removes all others.
    expect(removedData.tables.map((table) => table.id)).toEqual([removedDataTableId]);
    expect(removedData.dropTables.map((table) => table.id)).toEqual([dataOnlyTableId]);
    expect(removedData.tables[0].bucketDataSources).toHaveLength(0);
    expect(removedData.tables[0].parameterLookupSources).toHaveLength(1);
    await writer.drop(removedData.dropTables);

    const eventOnly = await writer.resolveTables({
      connection_id: 1,
      source,
      idGenerator: () => {
        throw new Error('resolve should reuse existing v3 source table');
      },
      syncRules: eventOnlyRules
    });

    // Event-only table can re-use any existing table.
    expect(eventOnly.tables.map((table) => table.id)).toEqual([removedDataTableId]);
    expect(eventOnly.dropTables.map((table) => table.id)).toEqual([]);
    expect(eventOnly.tables[0].bucketDataSources).toHaveLength(0);
    expect(eventOnly.tables[0].parameterLookupSources).toHaveLength(0);
    expect(eventOnly.tables[0].syncData).toBe(false);
    expect(eventOnly.tables[0].syncParameters).toBe(false);
    expect(eventOnly.tables[0].syncEvent).toBe(true);
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
