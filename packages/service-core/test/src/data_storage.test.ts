import { BucketDataBatchOptions, SaveOperationTag } from '@/storage/BucketStorage.js';
import { getUuidReplicaIdentityBson } from '@/util/util-index.js';
import { RequestParameters } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import { fromAsync, oneFromAsync } from './stream_utils.js';
import {
  BATCH_OPTIONS,
  getBatchData,
  getBatchMeta,
  makeTestTable,
  MONGO_STORAGE_FACTORY,
  PARSE_OPTIONS,
  rid,
  StorageFactory,
  testRules
} from './util.js';

const TEST_TABLE = makeTestTable('test', ['id']);

describe('store - mongodb', function () {
  defineDataStorageTests(MONGO_STORAGE_FACTORY);
});

function defineDataStorageTests(factory: StorageFactory) {
  test('save and load parameters', async () => {
    const sync_rules = testRules(`
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE id1 = token_parameters.user_id OR id2 = token_parameters.user_id
    data: [] 
    `);

    const storage = (await factory()).getInstance(sync_rules);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 't2',
          id1: 'user3',
          id2: 'user4',
          group_id: 'group2a'
        },
        afterReplicaId: rid('t2')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 't1',
          id1: 'user1',
          id2: 'user2',
          group_id: 'group1a'
        },
        afterReplicaId: rid('t1')
      });
    });

    const parameters = await storage.getParameterSets(result!.flushed_op, [['mybucket', '1', 'user1']]);
    expect(parameters).toEqual([
      {
        group_id: 'group1a'
      }
    ]);
  });

  test('it should use the latest version', async () => {
    const sync_rules = testRules(
      `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE id = token_parameters.user_id
    data: [] 
    `
    );

    const storage = (await factory()).getInstance(sync_rules);

    const result1 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'user1',
          group_id: 'group1'
        },
        afterReplicaId: rid('user1')
      });
    });
    const result2 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'user1',
          group_id: 'group2'
        },
        afterReplicaId: rid('user1')
      });
    });

    const parameters = await storage.getParameterSets(result2!.flushed_op, [['mybucket', '1', 'user1']]);
    expect(parameters).toEqual([
      {
        group_id: 'group2'
      }
    ]);

    // Use the checkpoint to get older data if relevant
    const parameters2 = await storage.getParameterSets(result1!.flushed_op, [['mybucket', '1', 'user1']]);
    expect(parameters2).toEqual([
      {
        group_id: 'group1'
      }
    ]);
  });

  test('save and load parameters with different number types', async () => {
    const sync_rules = testRules(
      `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE n1 = token_parameters.n1 and f2 = token_parameters.f2 and f3 = token_parameters.f3
    data: []
    `
    );

    const storage = (await factory()).getInstance(sync_rules);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 't1',
          group_id: 'group1',
          n1: 314n,
          f2: 314,
          f3: 3.14
        },
        afterReplicaId: rid('t1')
      });
    });

    const TEST_PARAMS = { group_id: 'group1' };

    const checkpoint = result!.flushed_op;

    const parameters1 = await storage.getParameterSets(checkpoint, [['mybucket', '1', 314n, 314, 3.14]]);
    expect(parameters1).toEqual([TEST_PARAMS]);
    const parameters2 = await storage.getParameterSets(checkpoint, [['mybucket', '1', 314, 314n, 3.14]]);
    expect(parameters2).toEqual([TEST_PARAMS]);
    const parameters3 = await storage.getParameterSets(checkpoint, [['mybucket', '1', 314n, 314, 3]]);
    expect(parameters3).toEqual([]);
  });

  test('save and load parameters with large numbers', async () => {
    // This ensures serialization / deserialization of "current_data" is done correctly.
    // This specific case tested here cannot happen with postgres in practice, but we still
    // test this to ensure correct deserialization.

    const sync_rules = testRules(
      `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE n1 = token_parameters.n1
    data: []
    `
    );

    const storage = (await factory()).getInstance(sync_rules);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 't1',
          group_id: 'group1',
          n1: 1152921504606846976n // 2^60
        },
        afterReplicaId: rid('t1')
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.UPDATE,
        after: {
          id: 't1',
          group_id: 'group1',
          // Simulate a TOAST value, even though it can't happen for values like this
          // in practice.
          n1: undefined
        },
        afterReplicaId: rid('t1')
      });
    });

    const TEST_PARAMS = { group_id: 'group1' };

    const checkpoint = result!.flushed_op;

    const parameters1 = await storage.getParameterSets(checkpoint, [['mybucket', '1', 1152921504606846976n]]);
    expect(parameters1).toEqual([TEST_PARAMS]);
  });

  test('removing row', async () => {
    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    );
    const storage = (await factory()).getInstance(sync_rules);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1'
        },
        afterReplicaId: rid('test1')
      });
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.DELETE,
        beforeReplicaId: rid('test1')
      });
    });

    const checkpoint = result!.flushed_op;

    const batch = await fromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']])));
    const data = batch[0].batch.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        checksum: d.checksum
      };
    });

    const c1 = 2871785649;
    const c2 = 2872534815;

    expect(data).toEqual([
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'REMOVE', object_id: 'test1', checksum: c2 }
    ]);

    const checksums = [...(await storage.getChecksums(checkpoint, ['global[]'])).values()];
    expect(checksums).toEqual([
      {
        bucket: 'global[]',
        checksum: (c1 + c2) & 0xffffffff,
        count: 2
      }
    ]);
  });

  test('save and load parameters with workspaceId', async () => {
    const WORKSPACE_TABLE = makeTestTable('workspace', ['id']);

    const sync_rules_content = testRules(
      `
bucket_definitions:
    by_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace."userId" = token_parameters.user_id
      data: []
    `
    );
    const sync_rules = sync_rules_content.parsed(PARSE_OPTIONS).sync_rules;

    const storage = (await factory()).getInstance(sync_rules_content);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'workspace1',
          userId: 'u1'
        },
        afterReplicaId: rid('workspace1')
      });
    });

    const checkpoint = result!.flushed_op;

    const parameters = new RequestParameters({ sub: 'u1' }, {});

    const q1 = sync_rules.bucket_descriptors[0].parameter_queries[0];

    const lookups = q1.getLookups(parameters);
    expect(lookups).toEqual([['by_workspace', '1', 'u1']]);

    const parameter_sets = await storage.getParameterSets(checkpoint, lookups);
    expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }]);

    const buckets = await sync_rules.queryBucketIds({
      getParameterSets(lookups) {
        return storage.getParameterSets(checkpoint, lookups);
      },
      parameters
    });
    expect(buckets).toEqual(['by_workspace["workspace1"]']);
  });

  test('save and load parameters with dynamic global buckets', async () => {
    const WORKSPACE_TABLE = makeTestTable('workspace');

    const sync_rules_content = testRules(
      `
bucket_definitions:
    by_public_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace.visibility = 'public'
      data: []
    `
    );
    const sync_rules = sync_rules_content.parsed(PARSE_OPTIONS).sync_rules;

    const storage = (await factory()).getInstance(sync_rules_content);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'workspace1',
          visibility: 'public'
        },
        afterReplicaId: rid('workspace1')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'workspace2',
          visibility: 'private'
        },
        afterReplicaId: rid('workspace2')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'workspace3',
          visibility: 'public'
        },
        afterReplicaId: rid('workspace3')
      });
    });

    const checkpoint = result!.flushed_op;

    const parameters = new RequestParameters({ sub: 'unknown' }, {});

    const q1 = sync_rules.bucket_descriptors[0].parameter_queries[0];

    const lookups = q1.getLookups(parameters);
    expect(lookups).toEqual([['by_public_workspace', '1']]);

    const parameter_sets = await storage.getParameterSets(checkpoint, lookups);
    parameter_sets.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    expect(parameter_sets).toEqual([{ workspace_id: 'workspace1' }, { workspace_id: 'workspace3' }]);

    const buckets = await sync_rules.queryBucketIds({
      getParameterSets(lookups) {
        return storage.getParameterSets(checkpoint, lookups);
      },
      parameters
    });
    buckets.sort();
    expect(buckets).toEqual(['by_public_workspace["workspace1"]', 'by_public_workspace["workspace3"]']);
  });

  test('multiple parameter queries', async () => {
    const WORKSPACE_TABLE = makeTestTable('workspace');

    const sync_rules_content = testRules(
      `
bucket_definitions:
    by_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace.visibility = 'public'
        - SELECT id as workspace_id FROM workspace WHERE
            workspace.user_id = token_parameters.user_id
      data: []
    `
    );
    const sync_rules = sync_rules_content.parsed(PARSE_OPTIONS).sync_rules;

    const storage = (await factory()).getInstance(sync_rules_content);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'workspace1',
          visibility: 'public'
        },
        afterReplicaId: rid('workspace1')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'workspace2',
          visibility: 'private'
        },
        afterReplicaId: rid('workspace2')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'workspace3',
          user_id: 'u1',
          visibility: 'private'
        },
        afterReplicaId: rid('workspace3')
      });

      await batch.save({
        sourceTable: WORKSPACE_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'workspace4',
          user_id: 'u2',
          visibility: 'private'
        },
        afterReplicaId: rid('workspace4')
      });
    });

    const checkpoint = result!.flushed_op;

    const parameters = new RequestParameters({ sub: 'u1' }, {});

    // Test intermediate values - could be moved to sync_rules.test.ts
    const q1 = sync_rules.bucket_descriptors[0].parameter_queries[0];
    const lookups1 = q1.getLookups(parameters);
    expect(lookups1).toEqual([['by_workspace', '1']]);

    const parameter_sets1 = await storage.getParameterSets(checkpoint, lookups1);
    parameter_sets1.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    expect(parameter_sets1).toEqual([{ workspace_id: 'workspace1' }]);

    const q2 = sync_rules.bucket_descriptors[0].parameter_queries[1];
    const lookups2 = q2.getLookups(parameters);
    expect(lookups2).toEqual([['by_workspace', '2', 'u1']]);

    const parameter_sets2 = await storage.getParameterSets(checkpoint, lookups2);
    parameter_sets2.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    expect(parameter_sets2).toEqual([{ workspace_id: 'workspace3' }]);

    // Test final values - the important part
    const buckets = await sync_rules.queryBucketIds({
      getParameterSets(lookups) {
        return storage.getParameterSets(checkpoint, lookups);
      },
      parameters
    });
    buckets.sort();
    expect(buckets).toEqual(['by_workspace["workspace1"]', 'by_workspace["workspace3"]']);
  });

  test('changing client ids', async () => {
    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT client_id as id, description FROM "%"
`
    );
    const storage = (await factory()).getInstance(sync_rules);

    const sourceTable = TEST_TABLE;
    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          client_id: 'client1a',
          description: 'test1a'
        },
        afterReplicaId: rid('test1')
      });
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          client_id: 'client1b',
          description: 'test1b'
        },
        afterReplicaId: rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test2',
          client_id: 'client2',
          description: 'test2'
        },
        afterReplicaId: rid('test2')
      });
    });
    const checkpoint = result!.flushed_op;
    const batch = await fromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']])));
    const data = batch[0].batch.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id
      };
    });

    expect(data).toEqual([
      { op: 'PUT', object_id: 'client1a' },
      { op: 'PUT', object_id: 'client1b' },
      { op: 'REMOVE', object_id: 'client1a' },
      { op: 'PUT', object_id: 'client2' }
    ]);
  });

  test('re-apply delete', async () => {
    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    );
    const storage = (await factory()).getInstance(sync_rules);

    await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1'
        },
        afterReplicaId: rid('test1')
      });
    });

    await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.DELETE,
        beforeReplicaId: rid('test1')
      });
    });

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.DELETE,
        beforeReplicaId: rid('test1')
      });
    });

    const checkpoint = result!.flushed_op;

    const batch = await fromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']])));
    const data = batch[0].batch.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        checksum: d.checksum
      };
    });

    const c1 = 2871785649;
    const c2 = 2872534815;

    expect(data).toEqual([
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'REMOVE', object_id: 'test1', checksum: c2 }
    ]);

    const checksums = [...(await storage.getChecksums(checkpoint, ['global[]'])).values()];
    expect(checksums).toEqual([
      {
        bucket: 'global[]',
        checksum: (c1 + c2) & 0xffffffff,
        count: 2
      }
    ]);
  });

  test('re-apply update + delete', async () => {
    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    );
    const storage = (await factory()).getInstance(sync_rules);

    await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1'
        },
        afterReplicaId: rid('test1')
      });
    });

    await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          description: undefined
        },
        afterReplicaId: rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          description: undefined
        },
        afterReplicaId: rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.DELETE,
        beforeReplicaId: rid('test1')
      });
    });

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          description: undefined
        },
        afterReplicaId: rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        after: {
          id: 'test1',
          description: undefined
        },
        afterReplicaId: rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.DELETE,
        beforeReplicaId: rid('test1')
      });
    });

    const checkpoint = result!.flushed_op;

    const batch = await fromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']])));

    const data = batch[0].batch.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        checksum: d.checksum
      };
    });

    const c1 = 2871785649;
    const c2 = 2872534815;

    expect(data).toEqual([
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'PUT', object_id: 'test1', checksum: c1 },
      { op: 'REMOVE', object_id: 'test1', checksum: c2 }
    ]);

    const checksums = [...(await storage.getChecksums(checkpoint, ['global[]'])).values()];
    expect(checksums).toEqual([
      {
        bucket: 'global[]',
        checksum: (c1 + c1 + c1 + c2) & 0xffffffff,
        count: 4
      }
    ]);
  });

  test('truncate parameters', async () => {
    const sync_rules = testRules(
      `
bucket_definitions:
  mybucket:
    parameters:
      - SELECT group_id FROM test WHERE id1 = token_parameters.user_id OR id2 = token_parameters.user_id
    data: []
    `
    );

    const storage = (await factory()).getInstance(sync_rules);

    await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 't2',
          id1: 'user3',
          id2: 'user4',
          group_id: 'group2a'
        },
        afterReplicaId: rid('t2')
      });

      await batch.truncate([TEST_TABLE]);
    });

    const { checkpoint } = await storage.getCheckpoint();

    const parameters = await storage.getParameterSets(checkpoint, [['mybucket', '1', 'user1']]);
    expect(parameters).toEqual([]);
  });

  test('batch with overlapping replica ids', async () => {
    // This test checks that we get the correct output when processing rows with:
    // 1. changing replica ids
    // 2. overlapping with replica ids of other rows in the same transaction (at different times)
    // If operations are not processing in input order, this breaks easily.
    // It can break at two places:
    // 1. Not getting the correct "current_data" state for each operation.
    // 2. Output order not being correct.

    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test"
`
    );
    const storage = (await factory()).getInstance(sync_rules);

    // Pre-setup
    const result1 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1a'
        },
        afterReplicaId: rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test2',
          description: 'test2a'
        },
        afterReplicaId: rid('test2')
      });
    });

    const checkpoint1 = result1?.flushed_op ?? '0';

    // Test batch
    const result2 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;
      // b
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1b'
        },
        afterReplicaId: rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        before: {
          id: 'test1'
        },
        beforeReplicaId: rid('test1'),
        after: {
          id: 'test2',
          description: 'test2b'
        },
        afterReplicaId: rid('test2')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        before: {
          id: 'test2'
        },
        beforeReplicaId: rid('test2'),
        after: {
          id: 'test3',
          description: 'test3b'
        },

        afterReplicaId: rid('test3')
      });

      // c
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        after: {
          id: 'test2',
          description: 'test2c'
        },
        afterReplicaId: rid('test2')
      });

      // d
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test4',
          description: 'test4d'
        },
        afterReplicaId: rid('test4')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        before: {
          id: 'test4'
        },
        beforeReplicaId: rid('test4'),
        after: {
          id: 'test5',
          description: 'test5d'
        },
        afterReplicaId: rid('test5')
      });
    });

    const checkpoint2 = result2!.flushed_op;

    const batch = await fromAsync(storage.getBucketDataBatch(checkpoint2, new Map([['global[]', checkpoint1]])));
    const data = batch[0].batch.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        data: d.data
      };
    });

    // Operations must be in this order
    expect(data).toEqual([
      // b
      { op: 'PUT', object_id: 'test1', data: JSON.stringify({ id: 'test1', description: 'test1b' }) },
      { op: 'REMOVE', object_id: 'test1', data: null },
      { op: 'PUT', object_id: 'test2', data: JSON.stringify({ id: 'test2', description: 'test2b' }) },
      { op: 'REMOVE', object_id: 'test2', data: null },
      { op: 'PUT', object_id: 'test3', data: JSON.stringify({ id: 'test3', description: 'test3b' }) },

      // c
      { op: 'PUT', object_id: 'test2', data: JSON.stringify({ id: 'test2', description: 'test2c' }) },

      // d
      { op: 'PUT', object_id: 'test4', data: JSON.stringify({ id: 'test4', description: 'test4d' }) },
      { op: 'REMOVE', object_id: 'test4', data: null },
      { op: 'PUT', object_id: 'test5', data: JSON.stringify({ id: 'test5', description: 'test5d' }) }
    ]);
  });

  test('changed data with replica identity full', async () => {
    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test"
`
    );
    function rid2(id: string, description: string) {
      return getUuidReplicaIdentityBson({ id, description }, [
        { name: 'id', type: 'VARCHAR', typeId: 25 },
        { name: 'description', type: 'VARCHAR', typeId: 25 }
      ]);
    }
    const storage = (await factory()).getInstance(sync_rules);

    const sourceTable = makeTestTable('test', ['id', 'description']);

    // Pre-setup
    const result1 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1a'
        },
        afterReplicaId: rid2('test1', 'test1a')
      });
    });

    const checkpoint1 = result1?.flushed_op ?? '0';

    const result2 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      // Unchanged, but has a before id
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        before: {
          id: 'test1',
          description: 'test1a'
        },
        beforeReplicaId: rid2('test1', 'test1a'),
        after: {
          id: 'test1',
          description: 'test1b'
        },
        afterReplicaId: rid2('test1', 'test1b')
      });
    });

    const result3 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      // Delete
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.DELETE,
        before: {
          id: 'test1',
          description: 'test1b'
        },
        beforeReplicaId: rid2('test1', 'test1b'),
        after: undefined
      });
    });

    const checkpoint3 = result3!.flushed_op;

    const batch = await fromAsync(storage.getBucketDataBatch(checkpoint3, new Map([['global[]', checkpoint1]])));
    const data = batch[0].batch.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        data: d.data,
        subkey: d.subkey
      };
    });

    // Operations must be in this order
    expect(data).toEqual([
      // 2
      // The REMOVE is expected because the subkey changes
      {
        op: 'REMOVE',
        object_id: 'test1',
        data: null,
        subkey: '6544e3899293153fa7b38331/740ba9f2-8b0f-53e3-bb17-5f38a9616f0e'
      },
      {
        op: 'PUT',
        object_id: 'test1',
        data: JSON.stringify({ id: 'test1', description: 'test1b' }),
        subkey: '6544e3899293153fa7b38331/500e9b68-a2fd-51ff-9c00-313e2fb9f562'
      },
      // 3
      {
        op: 'REMOVE',
        object_id: 'test1',
        data: null,
        subkey: '6544e3899293153fa7b38331/500e9b68-a2fd-51ff-9c00-313e2fb9f562'
      }
    ]);
  });

  test('unchanged data with replica identity full', async () => {
    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test"
`
    );
    function rid2(id: string, description: string) {
      return getUuidReplicaIdentityBson({ id, description }, [
        { name: 'id', type: 'VARCHAR', typeId: 25 },
        { name: 'description', type: 'VARCHAR', typeId: 25 }
      ]);
    }

    const storage = (await factory()).getInstance(sync_rules);

    const sourceTable = makeTestTable('test', ['id', 'description']);

    // Pre-setup
    const result1 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1a'
        },
        afterReplicaId: rid2('test1', 'test1a')
      });
    });

    const checkpoint1 = result1?.flushed_op ?? '0';

    const result2 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      // Unchanged, but has a before id
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.UPDATE,
        before: {
          id: 'test1',
          description: 'test1a'
        },
        beforeReplicaId: rid2('test1', 'test1a'),
        after: {
          id: 'test1',
          description: 'test1a'
        },
        afterReplicaId: rid2('test1', 'test1a')
      });
    });

    const result3 = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      // Delete
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.DELETE,
        before: {
          id: 'test1',
          description: 'test1a'
        },
        beforeReplicaId: rid2('test1', 'test1a'),
        after: undefined
      });
    });

    const checkpoint3 = result3!.flushed_op;

    const batch = await fromAsync(storage.getBucketDataBatch(checkpoint3, new Map([['global[]', checkpoint1]])));
    const data = batch[0].batch.data.map((d) => {
      return {
        op: d.op,
        object_id: d.object_id,
        data: d.data,
        subkey: d.subkey
      };
    });

    // Operations must be in this order
    expect(data).toEqual([
      // 2
      {
        op: 'PUT',
        object_id: 'test1',
        data: JSON.stringify({ id: 'test1', description: 'test1a' }),
        subkey: '6544e3899293153fa7b38331/740ba9f2-8b0f-53e3-bb17-5f38a9616f0e'
      },
      // 3
      {
        op: 'REMOVE',
        object_id: 'test1',
        data: null,
        subkey: '6544e3899293153fa7b38331/740ba9f2-8b0f-53e3-bb17-5f38a9616f0e'
      }
    ]);
  });

  test('large batch', async () => {
    // Test syncing a batch of data that is small in count,
    // but large enough in size to be split over multiple returned batches.
    // The specific batch splits is an implementation detail of the storage driver,
    // and the test will have to updated when other implementations are added.
    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    );
    const storage = (await factory()).getInstance(sync_rules);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      const largeDescription = '0123456789'.repeat(12_000_00);

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1'
        },
        afterReplicaId: rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'large1',
          description: largeDescription
        },
        afterReplicaId: rid('large1')
      });

      // Large enough to split the returned batch
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'large2',
          description: largeDescription
        },
        afterReplicaId: rid('large2')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test3',
          description: 'test3'
        },
        afterReplicaId: rid('test3')
      });
    });

    const checkpoint = result!.flushed_op;

    const options: BucketDataBatchOptions = {
      chunkLimitBytes: 16 * 1024 * 1024
    };

    const batch1 = await fromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']]), options));
    expect(getBatchData(batch1)).toEqual([
      { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 },
      { op_id: '2', op: 'PUT', object_id: 'large1', checksum: 454746904 }
    ]);
    expect(getBatchMeta(batch1)).toEqual({
      after: '0',
      has_more: true,
      next_after: '2'
    });

    const batch2 = await fromAsync(
      storage.getBucketDataBatch(checkpoint, new Map([['global[]', batch1[0].batch.next_after]]), options)
    );
    expect(getBatchData(batch2)).toEqual([
      { op_id: '3', op: 'PUT', object_id: 'large2', checksum: 1795508474 },
      { op_id: '4', op: 'PUT', object_id: 'test3', checksum: 1359888332 }
    ]);
    expect(getBatchMeta(batch2)).toEqual({
      after: '2',
      has_more: false,
      next_after: '4'
    });

    const batch3 = await fromAsync(
      storage.getBucketDataBatch(checkpoint, new Map([['global[]', batch2[0].batch.next_after]]), options)
    );
    expect(getBatchData(batch3)).toEqual([]);
    expect(getBatchMeta(batch3)).toEqual(null);
  });

  test('large batch (2)', async () => {
    // Test syncing a batch of data that is small in count,
    // but large enough in size to be split over multiple returned chunks.
    // Similar to the above test, but splits over 1MB chunks.
    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    );
    const storage = (await factory()).getInstance(sync_rules);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      const largeDescription = '0123456789'.repeat(2_000_00);

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          description: 'test1'
        },
        afterReplicaId: rid('test1')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'large1',
          description: largeDescription
        },
        afterReplicaId: rid('large1')
      });

      // Large enough to split the returned batch
      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'large2',
          description: largeDescription
        },
        afterReplicaId: rid('large2')
      });

      await batch.save({
        sourceTable,
        tag: SaveOperationTag.INSERT,
        after: {
          id: 'test3',
          description: 'test3'
        },
        afterReplicaId: rid('test3')
      });
    });

    const checkpoint = result!.flushed_op;

    const options: BucketDataBatchOptions = {};

    const batch1 = await fromAsync(storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']]), options));
    expect(getBatchData(batch1)).toEqual([
      { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 },
      { op_id: '2', op: 'PUT', object_id: 'large1', checksum: 1178768505 }
    ]);
    expect(getBatchMeta(batch1)).toEqual({
      after: '0',
      has_more: true,
      next_after: '2'
    });

    const batch2 = await fromAsync(
      storage.getBucketDataBatch(checkpoint, new Map([['global[]', batch1[0].batch.next_after]]), options)
    );
    expect(getBatchData(batch2)).toEqual([{ op_id: '3', op: 'PUT', object_id: 'large2', checksum: 1607205872 }]);
    expect(getBatchMeta(batch2)).toEqual({
      after: '2',
      has_more: true,
      next_after: '3'
    });

    const batch3 = await fromAsync(
      storage.getBucketDataBatch(checkpoint, new Map([['global[]', batch2[0].batch.next_after]]), options)
    );
    expect(getBatchData(batch3)).toEqual([{ op_id: '4', op: 'PUT', object_id: 'test3', checksum: 1359888332 }]);
    expect(getBatchMeta(batch3)).toEqual({
      after: '3',
      has_more: false,
      next_after: '4'
    });
  });

  test('long batch', async () => {
    // Test syncing a batch of data that is limited by count.
    const sync_rules = testRules(
      `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`
    );
    const storage = (await factory()).getInstance(sync_rules);

    const result = await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      const sourceTable = TEST_TABLE;

      for (let i = 1; i <= 6; i++) {
        await batch.save({
          sourceTable,
          tag: SaveOperationTag.INSERT,
          after: {
            id: `test${i}`,
            description: `test${i}`
          },
          afterReplicaId: `test${i}`
        });
      }
    });

    const checkpoint = result!.flushed_op;

    const batch1 = await oneFromAsync(
      storage.getBucketDataBatch(checkpoint, new Map([['global[]', '0']]), { limit: 4 })
    );

    expect(getBatchData(batch1)).toEqual([
      { op_id: '1', op: 'PUT', object_id: 'test1', checksum: 2871785649 },
      { op_id: '2', op: 'PUT', object_id: 'test2', checksum: 730027011 },
      { op_id: '3', op: 'PUT', object_id: 'test3', checksum: 1359888332 },
      { op_id: '4', op: 'PUT', object_id: 'test4', checksum: 2049153252 }
    ]);

    expect(getBatchMeta(batch1)).toEqual({
      after: '0',
      has_more: true,
      next_after: '4'
    });

    const batch2 = await oneFromAsync(
      storage.getBucketDataBatch(checkpoint, new Map([['global[]', batch1.batch.next_after]]), {
        limit: 4
      })
    );
    expect(getBatchData(batch2)).toEqual([
      { op_id: '5', op: 'PUT', object_id: 'test5', checksum: 3686902721 },
      { op_id: '6', op: 'PUT', object_id: 'test6', checksum: 1974820016 }
    ]);

    expect(getBatchMeta(batch2)).toEqual({
      after: '4',
      has_more: false,
      next_after: '6'
    });

    const batch3 = await fromAsync(
      storage.getBucketDataBatch(checkpoint, new Map([['global[]', batch2.batch.next_after]]), {
        limit: 4
      })
    );
    expect(getBatchData(batch3)).toEqual([]);

    expect(getBatchMeta(batch3)).toEqual(null);
  });

  test('batch should be disposed automatically', async () => {
    const sync_rules = testRules(`
      bucket_definitions:
        global:
          data: [] 
          `);

    const storage = (await factory()).getInstance(sync_rules);

    let isDisposed = false;
    await storage.startBatch(BATCH_OPTIONS, async (batch) => {
      batch.registerListener({
        disposed: () => {
          isDisposed = true;
        }
      });
    });
    expect(isDisposed).true;

    isDisposed = false;
    let errorCaught = false;
    try {
      await storage.startBatch(BATCH_OPTIONS, async (batch) => {
        batch.registerListener({
          disposed: () => {
            isDisposed = true;
          }
        });
        throw new Error(`Testing exceptions`);
      });
    } catch (ex) {
      errorCaught = true;
      expect(ex.message.includes('Testing')).true;
    }
    expect(errorCaught).true;
    expect(isDisposed).true;
  });

  test('empty storage metrics', async () => {
    const f = await factory({ dropAll: true });

    const metrics = await f.getStorageMetrics();
    expect(metrics).toEqual({
      operations_size_bytes: 0,
      parameters_size_bytes: 0,
      replication_size_bytes: 0
    });

    const r = await f.configureSyncRules('bucket_definitions: {}');
    const storage = f.getInstance(r.persisted_sync_rules!);
    await storage.autoActivate();

    const metrics2 = await f.getStorageMetrics();
    expect(metrics2).toEqual({
      operations_size_bytes: 0,
      parameters_size_bytes: 0,
      replication_size_bytes: 0
    });
  });

  test('invalidate cached parsed sync rules', async () => {
    const sync_rules_content = testRules(
      `
bucket_definitions:
    by_workspace:
      parameters:
        - SELECT id as workspace_id FROM workspace WHERE
          workspace."userId" = token_parameters.user_id
      data: []
    `
    );

    const bucketStorageFactory = await factory();
    const syncBucketStorage = bucketStorageFactory.getInstance(sync_rules_content);

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
