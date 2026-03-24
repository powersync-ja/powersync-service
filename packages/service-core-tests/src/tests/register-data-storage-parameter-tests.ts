import { CURRENT_STORAGE_VERSION, JwtPayload, storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { RequestParameters, ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';
import { bucketRequest } from '../test-utils/test-utils-index.js';

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
  const storageVersion = config.storageVersion ?? CURRENT_STORAGE_VERSION;

  test('save and load parameters', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE id1 = token_parameters.user_id OR id2 = token_parameters.user_id
    data: []
    `,
        {
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();

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
    const parameters = new RequestParameters(new JwtPayload({ sub: 'user1' }), {});
    const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

    const buckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        expect(lookups.map((l) => l.indexKey)).toEqual([['user1']]);

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        expect(parameter_sets).toEqual([{ group_id: 'group1a' }]);
        return parameter_sets;
      }
    });

    expect(buckets.map((b) => b.bucket)).toEqual([bucketRequest(syncRules, 'mybucket["group1a"]').bucket]);
  });

  test('it should use the latest version', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE id = token_parameters.user_id
    data: []
    `,
        {
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();

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

    const parameters = new RequestParameters(new JwtPayload({ sub: 'user1' }), {});
    const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

    const buckets1 = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        expect(lookups.map((l) => l.indexKey)).toEqual([['user1']]);

        const parameter_sets = await checkpoint1.getParameterSets(lookups);
        expect(parameter_sets).toEqual([{ group_id: 'group1' }]);
        return parameter_sets;
      }
    });
    expect(buckets1.map((b) => b.bucket)).toEqual([bucketRequest(syncRules, 'mybucket["group1"]').bucket]);

    const buckets2 = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        expect(lookups.map((l) => l.indexKey)).toEqual([['user1']]);

        const parameter_sets = await checkpoint2.getParameterSets(lookups);
        expect(parameter_sets).toEqual([{ group_id: 'group2' }]);
        return parameter_sets;
      }
    });
    expect(buckets2.map((b) => b.bucket)).toEqual([bucketRequest(syncRules, 'mybucket["group2"]').bucket]);
  });

  test('it should use the latest version after updates', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT id AS todo_id
        FROM todos
        WHERE list_id IN token_parameters.list_id
    data: []
    `,
        { storageVersion }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
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
    const checkpoint = await bucketStorage.getCheckpoint();
    const parameters = new RequestParameters(
      new JwtPayload({ sub: 'u1', parameters: { list_id: ['list1', 'list2'] } }),
      {}
    );
    const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

    const buckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        expect(lookups.map((l) => JSON.stringify(l.indexKey)).sort()).toEqual(['["list1"]', '["list2"]']);

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        expect(parameter_sets.sort((a, b) => (a.todo_id as string).localeCompare(b.todo_id as string))).toEqual([
          { todo_id: 'todo1' },
          { todo_id: 'todo2' }
        ]);
        return parameter_sets;
      }
    });

    expect(buckets.map((b) => b.bucket).sort()).toEqual([
      bucketRequest(syncRules, 'mybucket["todo1"]').bucket,
      bucketRequest(syncRules, 'mybucket["todo2"]').bucket
    ]);
  });

  test('save and load parameters with different number types', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE n1 = token_parameters.n1 and f2 = token_parameters.f2 and f3 = token_parameters.f3
    data: []
    `,
        {
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
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
    const testQuery = async (jwtParameters: Record<string, any>, expectedParameterSets: SqliteJsonRow[]) => {
      const parameters = new RequestParameters(new JwtPayload({ sub: 'u1', parameters: jwtParameters }), {});
      const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

      return await querier.queryDynamicBucketDescriptions({
        async getParameterSets(lookups) {
          const parameter_sets = await checkpoint.getParameterSets(lookups);
          expect(parameter_sets).toEqual(expectedParameterSets);
          return parameter_sets;
        }
      });
    };

    expect(await testQuery({ n1: 314n, f2: 314, f3: 3.14 }, [{ group_id: 'group1' }])).toMatchObject([
      { bucket: bucketRequest(syncRules, 'mybucket["group1"]').bucket }
    ]);
    expect(await testQuery({ n1: 314, f2: 314n, f3: 3.14 }, [{ group_id: 'group1' }])).toMatchObject([
      { bucket: bucketRequest(syncRules, 'mybucket["group1"]').bucket }
    ]);
    expect(await testQuery({ n1: 314n, f2: 314, f3: 3 }, [])).toEqual([]);
  });

  test('save and load parameters with large numbers', async () => {
    // This ensures serialization / deserialization of "current_data" is done correctly.
    // This specific case tested here cannot happen with postgres in practice, but we still
    // test this to ensure correct deserialization.

    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE n1 = token_parameters.n1
    data: []
    `,
        {
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();

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

    const n1 = 1152921504606846976n;
    const parameters = new RequestParameters(new JwtPayload({ sub: 'u1', parameters: { n1 } }), {});

    const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;
    const buckets = await querier.queryDynamicBucketDescriptions({
      getParameterSets: async (lookups) => {
        expect(lookups.map((l) => l.indexKey)).toEqual([[n1]]);

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        expect(parameter_sets).toEqual([{ group_id: 'group1' }]);
        return parameter_sets;
      }
    });

    expect(buckets.map((b) => b.bucket)).toEqual([bucketRequest(syncRules, 'mybucket["group1"]').bucket]);
  });

  test('save and load parameters with workspaceId', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
    by_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace."userId" = token_parameters.user_id
      data: []
    `,
        {
          storageVersion
        }
      )
    );
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();
    const bucketStorage = factory.getInstance(syncRules);

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

    const parameters = new RequestParameters(new JwtPayload({ sub: 'u1' }), {});

    const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

    const buckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        expect(lookups.map((l) => l.indexKey)).toEqual([['u1']]);

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }]);
        return parameter_sets;
      }
    });
    expect(buckets).toEqual([
      {
        bucket: bucketRequest(syncRules, 'by_workspace["workspace1"]').bucket,
        priority: 3,
        definition: 'by_workspace',
        inclusion_reasons: ['default']
      }
    ]);
  });

  test('save and load parameters with dynamic global buckets', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
    by_public_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace.visibility = 'public'
      data: []
    `,
        {
          storageVersion
        }
      )
    );
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();
    const bucketStorage = factory.getInstance(syncRules);

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

    const parameters = new RequestParameters(new JwtPayload({ sub: 'unknown' }), {});

    const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

    const buckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        expect(lookups.map((l) => l.indexKey)).toEqual([[]]);

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        parameter_sets.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
        expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }, { workspace_id: 'workspace3' }]);
        return parameter_sets;
      }
    });
    buckets.sort((a, b) => a.bucket.localeCompare(b.bucket));
    expect(buckets).toEqual([
      {
        bucket: bucketRequest(syncRules, 'by_public_workspace["workspace1"]').bucket,
        priority: 3,
        definition: 'by_public_workspace',
        inclusion_reasons: ['default']
      },
      {
        bucket: bucketRequest(syncRules, 'by_public_workspace["workspace3"]').bucket,
        priority: 3,
        definition: 'by_public_workspace',
        inclusion_reasons: ['default']
      }
    ]);
  });

  test('multiple parameter queries', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
    by_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace.visibility = 'public'
        - SELECT id as workspace_id FROM workspace WHERE
            workspace.user_id = token_parameters.user_id
      data: []
    `,
        {
          storageVersion
        }
      )
    );
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();
    const bucketStorage = factory.getInstance(syncRules);

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

    const parameters = new RequestParameters(new JwtPayload({ sub: 'u1' }), {});

    // Test intermediate values - could be moved to sync_rules.test.ts
    const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

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
    // Not testing the scope anymore - the exact format depends on storage version
    expect(foundLookups.map((l) => l.indexKey)).toEqual([[], ['u1']]);
    parameter_sets.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }, { workspace_id: 'workspace3' }]);

    buckets.sort();
    expect(buckets).toEqual([
      bucketRequest(syncRules, 'by_workspace["workspace1"]').bucket,
      bucketRequest(syncRules, 'by_workspace["workspace3"]').bucket
    ]);
  });

  test('truncate parameters', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE id1 = token_parameters.user_id OR id2 = token_parameters.user_id
    data: []
    `,
        {
          storageVersion
        }
      )
    );
    const bucketStorage = factory.getInstance(syncRules);
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();

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
    const parameters = new RequestParameters(new JwtPayload({ sub: 'user1' }), {});
    const querier = sync_rules.getBucketParameterQuerier(test_utils.querierOptions(parameters)).querier;

    const buckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        expect(lookups.map((l) => l.indexKey)).toEqual([['user1']]);

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        expect(parameter_sets).toEqual([]);
        return parameter_sets;
      }
    });
    expect(buckets).toEqual([]);
  });

  test('invalidate cached parsed sync rules', async () => {
    await using bucketStorageFactory = await generateStorageFactory();
    const syncRules = await bucketStorageFactory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
    by_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace."userId" = token_parameters.user_id
      data: []
    `,
        {
          storageVersion
        }
      )
    );
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

  test('sync streams smoke test', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(`
config:
  edition: 3

streams:
  stream:
    query: |
      SELECT data.* FROM test AS data, test AS param
      WHERE data.foo = param.bar AND param.baz = auth.user_id()
    `)
    );
    const bucketStorage = factory.getInstance(syncRules);
    const sync_rules = syncRules.parsed(test_utils.PARSE_OPTIONS).hydratedSyncRules();

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        baz: 'baz',
        bar: 'bar'
      },
      afterReplicaId: test_utils.rid('t1')
    });

    await writer.commit('1/1');

    const checkpoint = await bucketStorage.getCheckpoint();
    const parameters = new RequestParameters(new JwtPayload({ sub: 'baz' }), {});
    const querier = sync_rules.getBucketParameterQuerier({
      ...test_utils.querierOptions(parameters),
      streams: {
        stream: [
          {
            parameters: null,
            opaque_id: 123
          }
        ]
      }
    }).querier;

    const buckets = await querier.queryDynamicBucketDescriptions({
      async getParameterSets(lookups) {
        expect(lookups.map((l) => l.indexKey)).toEqual([['baz']]);

        const parameter_sets = await checkpoint.getParameterSets(lookups);
        expect(parameter_sets).toEqual([{ '0': 'bar' }]);
        return parameter_sets;
      }
    });
    console.log('whatabuckets', buckets);
    expect(buckets).toHaveLength(1);
    expect(buckets).toMatchObject([
      {
        bucket: expect.stringMatching(/stream.*\["bar"\]$/),
        definition: 'stream',
        inclusion_reasons: [{ subscription: 123 }],
        priority: 3
      }
    ]);
  });
}
