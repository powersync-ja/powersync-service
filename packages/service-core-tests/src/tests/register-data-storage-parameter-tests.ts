import { storage } from '@powersync/service-core';
import { RequestParameters, ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';

/**
 * @example
 * ```TypeScript
 *
 * describe('store - mongodb', function () {
 *  registerDataStorageTests(MONGO_STORAGE_FACTORY);
 * });
 *
 * ```
 */
export function registerDataStorageParameterTests(config: storage.TestStorageConfig) {
  const generateStorageFactory = config.factory;

  test('save and load parameters', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE id1 = token_parameters.user_id OR id2 = token_parameters.user_id
    data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    const hydrated = bucketStorage.getHydratedSyncRules(test_utils.PARSE_OPTIONS);

    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2',
        id1: 'user3',
        id2: 'user4',
        group_id: 'group2a'
      },
      afterReplicaId: test_utils.rid('t2')
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        id1: 'user1',
        id2: 'user2',
        group_id: 'group1a'
      },
      afterReplicaId: test_utils.rid('t1')
    });

    await writer.commit('1/1');

    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters = new RequestParameters({ sub: 'user1' }, {});
    const querier = hydrated.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;
    const parameter_sets = await querier.queryDynamicBucketDescriptions(checkpoint);
    expect(parameter_sets).toMatchObject([{ bucket: expect.stringMatching(/"group1a"/) }]);
  });

  test('it should use the latest version', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE id = token_parameters.user_id
    data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);
    const hydrated = bucketStorage.getHydratedSyncRules(test_utils.PARSE_OPTIONS);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'user1',
        group_id: 'group1'
      },
      afterReplicaId: test_utils.rid('user1')
    });
    await writer.commit('1/1');
    const checkpoint1 = await bucketStorage.getCheckpoint();
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'user1',
        group_id: 'group2'
      },
      afterReplicaId: test_utils.rid('user1')
    });
    await writer.commit('1/2');
    const checkpoint2 = await bucketStorage.getCheckpoint();

    const querier = hydrated.getBucketParameterQuerier(
      test_utils.querierOptions(new RequestParameters({ sub: 'user1' }, {}))
    ).querier;

    const buckets1 = await querier.queryDynamicBucketDescriptions(checkpoint2);
    expect(buckets1).toMatchObject([
      {
        bucket: expect.stringMatching(/"group2"/)
      }
    ]);

    // Use the checkpoint to get older data if relevant
    const buckets2 = await querier.queryDynamicBucketDescriptions(checkpoint1);
    expect(buckets2).toMatchObject([
      {
        bucket: expect.stringMatching(/"group1"/)
      }
    ]);
  });

  test('it should use the latest version after updates', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT id AS todo_id
        FROM todos
        WHERE list_id IN token_parameters.list_id
    data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const hydrated = bucketStorage.getHydratedSyncRules(test_utils.PARSE_OPTIONS);
    const table = await test_utils.resolveTestTable(writer, 'todos', ['id', 'list_id'], config);

    await writer.markAllSnapshotDone('1/1');
    // Create two todos which initially belong to different lists
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'todo1',
        list_id: 'list1'
      },
      afterReplicaId: test_utils.rid('todo1')
    });
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'todo2',
        list_id: 'list2'
      },
      afterReplicaId: test_utils.rid('todo2')
    });

    await writer.commit('1/1');

    // Update the second todo item to now belong to list 1
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.UPDATE,
      after: {
        id: 'todo2',
        list_id: 'list1'
      },
      afterReplicaId: test_utils.rid('todo2')
    });

    await writer.commit('1/1');

    // We specifically request the todo_ids for both lists.
    // There removal operation for the association of `list2`::`todo2` should not interfere with the new
    // association of `list1`::`todo2`
    const querier = hydrated.getBucketParameterQuerier(
      test_utils.querierOptions(
        new RequestParameters({ sub: 'user1', parameters: { list_id: ['list1', 'list2'] } }, {})
      )
    ).querier;
    const checkpoint = await bucketStorage.getCheckpoint();
    const buckets = await querier.queryDynamicBucketDescriptions(checkpoint);

    expect(buckets.sort((a, b) => a.bucket.localeCompare(b.bucket))).toMatchObject([
      {
        bucket: expect.stringMatching(/"todo1"/)
      },
      {
        bucket: expect.stringMatching(/"todo2"/)
      }
    ]);
  });

  test('save and load parameters with different number types', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE n1 = token_parameters.n1 and f2 = token_parameters.f2 and f3 = token_parameters.f3
    data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const hydrated = bucketStorage.getHydratedSyncRules(test_utils.PARSE_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        group_id: 'group1',
        n1: 314n,
        f2: 314,
        f3: 3.14
      },
      afterReplicaId: test_utils.rid('t1')
    });

    await writer.commit('1/1');

    const checkpoint = await bucketStorage.getCheckpoint();

    const querier1 = hydrated.getBucketParameterQuerier(
      test_utils.querierOptions(
        new RequestParameters({ sub: 'user1', parameters: { n1: 314n, f2: 314, f3: 3.14 } }, {})
      )
    ).querier;
    const buckets1 = await querier1.queryDynamicBucketDescriptions(checkpoint);
    expect(buckets1).toMatchObject([{ bucket: expect.stringMatching(/"group1"/), definition: 'mybucket' }]);

    const querier2 = hydrated.getBucketParameterQuerier(
      test_utils.querierOptions(
        new RequestParameters({ sub: 'user1', parameters: { n1: 314, f2: 314n, f3: 3.14 } }, {})
      )
    ).querier;
    const buckets2 = await querier2.queryDynamicBucketDescriptions(checkpoint);
    expect(buckets2).toMatchObject([{ bucket: expect.stringMatching(/"group1"/), definition: 'mybucket' }]);

    const querier3 = hydrated.getBucketParameterQuerier(
      test_utils.querierOptions(new RequestParameters({ sub: 'user1', parameters: { n1: 314n, f2: 314, f3: 3 } }, {}))
    ).querier;
    const buckets3 = await querier3.queryDynamicBucketDescriptions(checkpoint);
    expect(buckets3).toEqual([]);
  });

  test('save and load parameters with large numbers', async () => {
    // This ensures serialization / deserialization of "current_data" is done correctly.
    // This specific case tested here cannot happen with postgres in practice, but we still
    // test this to ensure correct deserialization.

    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE n1 = token_parameters.n1
    data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);
    const hydrated = bucketStorage.getHydratedSyncRules(test_utils.PARSE_OPTIONS);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        group_id: 'group1',
        n1: 1152921504606846976n // 2^60
      },
      afterReplicaId: test_utils.rid('t1')
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: {
        id: 't1',
        group_id: 'group1',
        // Simulate a TOAST value, even though it can't happen for values like this
        // in practice.
        n1: undefined
      },
      afterReplicaId: test_utils.rid('t1')
    });

    await writer.commit('1/1');

    const checkpoint = await bucketStorage.getCheckpoint();
    const querier = hydrated.getBucketParameterQuerier(
      test_utils.querierOptions(new RequestParameters({ sub: 'user1', parameters: { n1: 1152921504606846976n } }, {}))
    ).querier;
    const buckets = await querier.queryDynamicBucketDescriptions(checkpoint);
    expect(buckets.map(test_utils.removeSourceSymbol)).toMatchObject([
      {
        bucket: expect.stringMatching(/"group1"/),
        definition: 'mybucket'
      }
    ]);
  });

  test('save and load parameters with workspaceId', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
    by_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace."userId" = token_parameters.user_id
      data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);
    const hydrated = bucketStorage.getHydratedSyncRules(test_utils.PARSE_OPTIONS);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const workspaceTable = await test_utils.resolveTestTable(writer, 'workspace', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: workspaceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'workspace1',
        userId: 'u1'
      },
      afterReplicaId: test_utils.rid('workspace1')
    });
    await writer.commit('1/1');
    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters = new RequestParameters({ sub: 'u1' }, {});

    const querier = hydrated.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

    const buckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        // Lookups are not stable anymore
        // expect(lookups).toEqual([ScopedParameterLookup.direct({ lookupName: 'by_workspace', queryId: '1' }, ['u1'])]);

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }]);
        return parameter_sets;
      }
    });
    const cleanedBuckets = buckets.map(test_utils.removeSourceSymbol);
    expect(cleanedBuckets).toHaveLength(1);
    expect(cleanedBuckets[0]).toMatchObject({
      priority: 3,
      definition: 'by_workspace',
      inclusion_reasons: ['default']
    });
    expect(cleanedBuckets[0].bucket.endsWith('["workspace1"]')).toBe(true);
  });

  test('save and load parameters with dynamic global buckets', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
    by_public_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace.visibility = 'public'
      data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);
    const hydrated = bucketStorage.getHydratedSyncRules(test_utils.PARSE_OPTIONS);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const workspaceTable = await test_utils.resolveTestTable(writer, 'workspace', undefined, config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: workspaceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'workspace1',
        visibility: 'public'
      },
      afterReplicaId: test_utils.rid('workspace1')
    });

    await writer.save({
      sourceTable: workspaceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'workspace2',
        visibility: 'private'
      },
      afterReplicaId: test_utils.rid('workspace2')
    });

    await writer.save({
      sourceTable: workspaceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'workspace3',
        visibility: 'public'
      },
      afterReplicaId: test_utils.rid('workspace3')
    });

    await writer.commit('1/1');

    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters = new RequestParameters({ sub: 'unknown' }, {});

    const querier = hydrated.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

    const buckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        // Lookups are not stable anymore
        // expect(lookups).toEqual([
        //   ScopedParameterLookup.direct({ lookupName: 'by_public_workspace', queryId: '1' }, [])
        // ]);

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        parameter_sets.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
        expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }, { workspace_id: 'workspace3' }]);
        return parameter_sets;
      }
    });
    const cleanedBuckets = buckets.map(test_utils.removeSourceSymbol);
    expect(cleanedBuckets).toHaveLength(2);
    for (const bucket of cleanedBuckets) {
      expect(bucket).toMatchObject({
        priority: 3,
        definition: 'by_public_workspace',
        inclusion_reasons: ['default']
      });
    }
    const bucketSuffixes = cleanedBuckets.map((bucket) => bucket.bucket.slice(bucket.bucket.indexOf('['))).sort();
    expect(bucketSuffixes).toEqual(['["workspace1"]', '["workspace3"]']);
  });

  test('multiple parameter queries', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
    by_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace.visibility = 'public'
        - SELECT id as workspace_id FROM workspace WHERE
            workspace.user_id = token_parameters.user_id
      data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);
    const hydrated = bucketStorage.getHydratedSyncRules(test_utils.PARSE_OPTIONS);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const workspaceTable = await test_utils.resolveTestTable(writer, 'workspace', undefined, config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: workspaceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'workspace1',
        visibility: 'public'
      },
      afterReplicaId: test_utils.rid('workspace1')
    });

    await writer.save({
      sourceTable: workspaceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'workspace2',
        visibility: 'private'
      },
      afterReplicaId: test_utils.rid('workspace2')
    });

    await writer.save({
      sourceTable: workspaceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'workspace3',
        user_id: 'u1',
        visibility: 'private'
      },
      afterReplicaId: test_utils.rid('workspace3')
    });

    await writer.save({
      sourceTable: workspaceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'workspace4',
        user_id: 'u2',
        visibility: 'private'
      },
      afterReplicaId: test_utils.rid('workspace4')
    });

    await writer.commit('1/1');

    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters = new RequestParameters({ sub: 'u1' }, {});

    // Test intermediate values - could be moved to sync_rules.test.ts
    const querier = hydrated.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

    // Test final values - the important part
    const foundLookups: ScopedParameterLookup[] = [];
    const parameter_sets: SqliteJsonRow[] = [];
    const buckets = (
      await querier.queryDynamicBucketDescriptions({
        async getParameterSets(lookups) {
          foundLookups.push(...lookups);
          const output = await checkpoint.getParameterSets(lookups);
          parameter_sets.push(...output);
          return output;
        }
      })
    ).map((e) => e.bucket);
    // Lookups are not stable anymore
    // expect(foundLookups).toEqual([
    //   ScopedParameterLookup.direct({ lookupName: 'by_workspace', queryId: '1' }, []),
    //   ScopedParameterLookup.direct({ lookupName: 'by_workspace', queryId: '2' }, ['u1'])
    // ]);
    parameter_sets.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }, { workspace_id: 'workspace3' }]);

    const bucketSuffixes = buckets.map((bucket) => bucket.slice(bucket.indexOf('['))).sort();
    expect(bucketSuffixes).toEqual(['["workspace1"]', '["workspace3"]']);
  });

  test('truncate parameters', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE id1 = token_parameters.user_id OR id2 = token_parameters.user_id
    data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);
    const hydrated = bucketStorage.getHydratedSyncRules(test_utils.PARSE_OPTIONS);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2',
        id1: 'user3',
        id2: 'user4',
        group_id: 'group2a'
      },
      afterReplicaId: test_utils.rid('t2')
    });

    await writer.truncate([testTable]);
    await writer.flush();

    const checkpoint = await bucketStorage.getCheckpoint();

    const querier = hydrated.getBucketParameterQuerier(
      test_utils.querierOptions(new RequestParameters({ sub: 'user1' }, {}))
    ).querier;
    const parameters = await querier.queryDynamicBucketDescriptions(checkpoint);
    expect(parameters).toEqual([]);
  });

  test('invalidate cached parsed sync rules', async () => {
    await using bucketStorageFactory = await generateStorageFactory();
    const syncRules = await bucketStorageFactory.updateSyncRules({
      content: `
bucket_definitions:
    by_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace."userId" = token_parameters.user_id
      data: []
    `
    });
    const syncBucketStorage = bucketStorageFactory.getInstance(syncRules);

    const parsedSchema1 = syncBucketStorage.getHydratedSyncRules({
      defaultSchema: 'public'
    });

    const parsedSchema2 = syncBucketStorage.getHydratedSyncRules({
      defaultSchema: 'public'
    });

    // These should be cached, this will be the same instance
    expect(parsedSchema2).equals(parsedSchema1);
    expect(parsedSchema1.getSourceTables()[0].schema).equals('public');

    const parsedSchema3 = syncBucketStorage.getHydratedSyncRules({
      defaultSchema: 'databasename'
    });

    // The cache should not be used
    expect(parsedSchema3).not.equals(parsedSchema2);
    expect(parsedSchema3.getSourceTables()[0].schema).equals('databasename');
  });
}
