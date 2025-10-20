import { storage } from '@powersync/service-core';
import { ParameterLookup, RequestParameters } from '@powersync/service-sync-rules';
import { SqlBucketDescriptor } from '@powersync/service-sync-rules/src/SqlBucketDescriptor.js';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';
import { TEST_TABLE } from './util.js';

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
export function registerDataStorageParameterTests(generateStorageFactory: storage.TestStorageFactory) {
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

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2',
          id1: 'user3',
          id2: 'user4',
          group_id: 'group2a'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          id1: 'user1',
          id2: 'user2',
          group_id: 'group1a'
        },
        afterReplicaId: test_utils.rid('t1')
      });

      await batch.commit('1/1');
    });

    const checkpoint = await bucketStorage.getCheckpoint();
    const parameters = await checkpoint.getParameterSets([ParameterLookup.normalized('mybucket', '1', ['user1'])]);
    expect(parameters).toEqual([
      {
        group_id: 'group1a'
      }
    ]);
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

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'user1',
          group_id: 'group1'
        },
        afterReplicaId: test_utils.rid('user1')
      });
      await batch.commit('1/1');
    });
    const checkpoint1 = await bucketStorage.getCheckpoint();
    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'user1',
          group_id: 'group2'
        },
        afterReplicaId: test_utils.rid('user1')
      });
      await batch.commit('1/2');
    });
    const checkpoint2 = await bucketStorage.getCheckpoint();

    const parameters = await checkpoint2.getParameterSets([ParameterLookup.normalized('mybucket', '1', ['user1'])]);
    expect(parameters).toEqual([
      {
        group_id: 'group2'
      }
    ]);

    // Use the checkpoint to get older data if relevant
    const parameters2 = await checkpoint1.getParameterSets([ParameterLookup.normalized('mybucket', '1', ['user1'])]);
    expect(parameters2).toEqual([
      {
        group_id: 'group1'
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

    const table = test_utils.makeTestTable('todos', ['id', 'list_id']);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Create two todos which initially belong to different lists
      await batch.save({
        sourceTable: table,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'todo1',
          list_id: 'list1'
        },
        afterReplicaId: test_utils.rid('todo1')
      });
      await batch.save({
        sourceTable: table,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'todo2',
          list_id: 'list2'
        },
        afterReplicaId: test_utils.rid('todo2')
      });

      await batch.commit('1/1');
    });

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Update the second todo item to now belong to list 1
      await batch.save({
        sourceTable: table,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 'todo2',
          list_id: 'list1'
        },
        afterReplicaId: test_utils.rid('todo2')
      });

      await batch.commit('1/1');
    });

    // We specifically request the todo_ids for both lists.
    // There removal operation for the association of `list2`::`todo2` should not interfere with the new
    // association of `list1`::`todo2`
    const checkpoint = await bucketStorage.getCheckpoint();
    const parameters = await checkpoint.getParameterSets([
      ParameterLookup.normalized('mybucket', '1', ['list1']),
      ParameterLookup.normalized('mybucket', '1', ['list2'])
    ]);

    expect(parameters.sort((a, b) => (a.todo_id as string).localeCompare(b.todo_id as string))).toEqual([
      {
        todo_id: 'todo1'
      },
      {
        todo_id: 'todo2'
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

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
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

      await batch.commit('1/1');
    });

    const TEST_PARAMS = { group_id: 'group1' };

    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters1 = await checkpoint.getParameterSets([
      ParameterLookup.normalized('mybucket', '1', [314n, 314, 3.14])
    ]);
    expect(parameters1).toEqual([TEST_PARAMS]);
    const parameters2 = await checkpoint.getParameterSets([
      ParameterLookup.normalized('mybucket', '1', [314, 314n, 3.14])
    ]);
    expect(parameters2).toEqual([TEST_PARAMS]);
    const parameters3 = await checkpoint.getParameterSets([
      ParameterLookup.normalized('mybucket', '1', [314n, 314, 3])
    ]);
    expect(parameters3).toEqual([]);
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

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          group_id: 'group1',
          n1: 1152921504606846976n // 2^60
        },
        afterReplicaId: test_utils.rid('t1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
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

      await batch.commit('1/1');
    });

    const TEST_PARAMS = { group_id: 'group1' };

    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters1 = await checkpoint.getParameterSets([
      ParameterLookup.normalized('mybucket', '1', [1152921504606846976n])
    ]);
    expect(parameters1).toEqual([TEST_PARAMS]);
  });

  test('save and load parameters with workspaceId', async () => {
    const WORKSPACE_TABLE = test_utils.makeTestTable('workspace', ['id']);

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
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).sync_rules;
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'workspace1',
          userId: 'u1'
        },
        afterReplicaId: test_utils.rid('workspace1')
      });
      await batch.commit('1/1');
    });
    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters = new RequestParameters({ sub: 'u1' }, {});

    const q1 = (sync_rules.bucketSources[0] as SqlBucketDescriptor).parameterQueries[0];

    const lookups = q1.getLookups(parameters);
    expect(lookups).toEqual([ParameterLookup.normalized('by_workspace', '1', ['u1'])]);

    const parameter_sets = await checkpoint.getParameterSets(lookups);
    expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }]);

    const buckets = await sync_rules
      .getBucketParameterQuerier(test_utils.querierOptions(parameters))
      .querier.queryDynamicBucketDescriptions({
        getParameterSets(lookups) {
          return checkpoint.getParameterSets(lookups);
        }
      });
    expect(buckets).toEqual([
      { bucket: 'by_workspace["workspace1"]', priority: 3, definition: 'by_workspace', inclusion_reasons: ['default'] }
    ]);
  });

  test('save and load parameters with dynamic global buckets', async () => {
    const WORKSPACE_TABLE = test_utils.makeTestTable('workspace');

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
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).sync_rules;
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'workspace1',
          visibility: 'public'
        },
        afterReplicaId: test_utils.rid('workspace1')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'workspace2',
          visibility: 'private'
        },
        afterReplicaId: test_utils.rid('workspace2')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'workspace3',
          visibility: 'public'
        },
        afterReplicaId: test_utils.rid('workspace3')
      });

      await batch.commit('1/1');
    });

    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters = new RequestParameters({ sub: 'unknown' }, {});

    const q1 = (sync_rules.bucketSources[0] as SqlBucketDescriptor).parameterQueries[0];

    const lookups = q1.getLookups(parameters);
    expect(lookups).toEqual([ParameterLookup.normalized('by_public_workspace', '1', [])]);

    const parameter_sets = await checkpoint.getParameterSets(lookups);
    parameter_sets.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }, { workspace_id: 'workspace3' }]);

    const buckets = await sync_rules
      .getBucketParameterQuerier(test_utils.querierOptions(parameters))
      .querier.queryDynamicBucketDescriptions({
        getParameterSets(lookups) {
          return checkpoint.getParameterSets(lookups);
        }
      });
    buckets.sort((a, b) => a.bucket.localeCompare(b.bucket));
    expect(buckets).toEqual([
      {
        bucket: 'by_public_workspace["workspace1"]',
        priority: 3,
        definition: 'by_public_workspace',
        inclusion_reasons: ['default']
      },
      {
        bucket: 'by_public_workspace["workspace3"]',
        priority: 3,
        definition: 'by_public_workspace',
        inclusion_reasons: ['default']
      }
    ]);
  });

  test('multiple parameter queries', async () => {
    const WORKSPACE_TABLE = test_utils.makeTestTable('workspace');

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
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).sync_rules;
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'workspace1',
          visibility: 'public'
        },
        afterReplicaId: test_utils.rid('workspace1')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'workspace2',
          visibility: 'private'
        },
        afterReplicaId: test_utils.rid('workspace2')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'workspace3',
          user_id: 'u1',
          visibility: 'private'
        },
        afterReplicaId: test_utils.rid('workspace3')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'workspace4',
          user_id: 'u2',
          visibility: 'private'
        },
        afterReplicaId: test_utils.rid('workspace4')
      });

      await batch.commit('1/1');
    });

    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters = new RequestParameters({ sub: 'u1' }, {});

    // Test intermediate values - could be moved to sync_rules.test.ts
    const q1 = (sync_rules.bucketSources[0] as SqlBucketDescriptor).parameterQueries[0];
    const lookups1 = q1.getLookups(parameters);
    expect(lookups1).toEqual([ParameterLookup.normalized('by_workspace', '1', [])]);

    const parameter_sets1 = await checkpoint.getParameterSets(lookups1);
    parameter_sets1.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    expect(parameter_sets1).toEqual([{ workspace_id: 'workspace1' }]);

    const q2 = (sync_rules.bucketSources[0] as SqlBucketDescriptor).parameterQueries[1];
    const lookups2 = q2.getLookups(parameters);
    expect(lookups2).toEqual([ParameterLookup.normalized('by_workspace', '2', ['u1'])]);

    const parameter_sets2 = await checkpoint.getParameterSets(lookups2);
    parameter_sets2.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    expect(parameter_sets2).toEqual([{ workspace_id: 'workspace3' }]);

    // Test final values - the important part
    const buckets = (
      await sync_rules
        .getBucketParameterQuerier(test_utils.querierOptions(parameters))
        .querier.queryDynamicBucketDescriptions({
          getParameterSets(lookups) {
            return checkpoint.getParameterSets(lookups);
          }
        })
    ).map((e) => e.bucket);
    buckets.sort();
    expect(buckets).toEqual(['by_workspace["workspace1"]', 'by_workspace["workspace3"]']);
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

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2',
          id1: 'user3',
          id2: 'user4',
          group_id: 'group2a'
        },
        afterReplicaId: test_utils.rid('t2')
      });

      await batch.truncate([TEST_TABLE]);
    });

    const checkpoint = await bucketStorage.getCheckpoint();

    const parameters = await checkpoint.getParameterSets([ParameterLookup.normalized('mybucket', '1', ['user1'])]);
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

    const parsedSchema1 = syncBucketStorage.getParsedSyncRules({
      defaultSchema: 'public'
    });

    const parsedSchema2 = syncBucketStorage.getParsedSyncRules({
      defaultSchema: 'public'
    });

    // These should be cached, this will be the same instance
    expect(parsedSchema2).equals(parsedSchema1);
    expect(parsedSchema1.getSourceTables()[0].schema).equals('public');

    const parsedSchema3 = syncBucketStorage.getParsedSyncRules({
      defaultSchema: 'databasename'
    });

    // The cache should not be used
    expect(parsedSchema3).not.equals(parsedSchema2);
    expect(parsedSchema3.getSourceTables()[0].schema).equals('databasename');
  });
}
