import { randomUUID } from 'node:crypto';

import { METRICS_HELPER, removeOp } from '@powersync/service-core-tests';
import { JSONBig } from '@powersync/service-jsonbig';
import { ReplicationMetric } from '@powersync/service-types';
import { describe, expect, test, vi } from 'vitest';
import { env } from '../env.js';
import { ConvexStreamTestContext } from '../test-utils/ConvexStreamTestContext.js';
import { describeWithStorage, StorageVersionTestContext } from '../test-utils/util.js';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT uuid as id, name FROM "lists"
`;

describe.skipIf(!env.CONVEX_DEPLOY_KEY)('ConvexStream integration tests', function () {
  describeWithStorage({ timeout: 120_000 }, function ({ factory, storageVersion }) {
    defineConvexStreamTests(factory, storageVersion);
  });
});

function defineConvexStreamTests(
  factory: StorageVersionTestContext['factory'],
  storageVersion: StorageVersionTestContext['storageVersion']
) {
  test('Initial snapshot sync', async () => {
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const testData = await createList(context, { name: 'snapshot-list' });
    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;

    await context.replicateSnapshot();

    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([convexPutOp('lists', syncedId(testData), listBucketRow(testData))]);
    expect(endRowCount - startRowCount).toEqual(1);
  });

  test('Replicate basic values', async () => {
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    const testData = await createList(context, { name: 'streamed-list' });

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([convexPutOp('lists', syncedId(testData), listBucketRow(testData))]);

    await vi.waitFor(async () => {
      const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
      const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
      expect(endRowCount - startRowCount).toEqual(1);
      expect(endTxCount - startTxCount).toEqual(1);
    });
  });

  test('Counts Convex batch mutations as single replicated transactions', async () => {
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    const batchOne = await createLists(context, ['batch-one-a', 'batch-one-b']);
    const single = await createList(context, { name: 'single-mutation' });
    const batchTwo = await createLists(context, ['batch-two-a', 'batch-two-b']);

    const data = await context.getBucketData('global[]');
    expect(data).toHaveLength(5);
    expect(data.slice(0, 2)).toEqual(expectListOps(batchOne));
    expect(data[2]).toMatchObject(convexPutOp('lists', syncedId(single), listBucketRow(single)));
    expect(data.slice(3, 5)).toEqual(expectListOps(batchTwo));

    await vi.waitFor(async () => {
      const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
      const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
      expect(endRowCount - startRowCount).toEqual(5);
      expect(endTxCount - startTxCount).toEqual(3);
    });
  });

  test('Replicated rows in transactions are correctly ordered', async () => {
    /**
     * out-of-ordered operations on different rows are not an issue.
     * out-of-ordered operations on the same row could be large issues.
     */
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await context.replicateSnapshot();

    const backendResult = await context.backend.client.mutation(context.backend.api.lists.testUpdateMultipleTimes, {});

    const data = await context.getBucketData('global[]');
    // Convex seems to squash the deltas, so we don't get a delta for each update which happened in the backend
    // The deleted row is reported as _deleted:true, which causes us not to replicate it
    expect(data.length).eq(backendResult.listIds.length);

    // All the put ops should contain a value for name which ends with a-b-c
    expect(
      data.every((item) => {
        const parsed = JSON.parse(item.data!);
        return parsed.name.endsWith('a-b-c');
      })
    ).true;
  });

  test('Replicate row updates', async () => {
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const testData = await createList(context, { name: 'initial-list' });
    await context.replicateSnapshot();

    const updatedData = {
      ...testData,
      name: 'updated-list'
    };
    await context.backend.client.mutation(context.backend.api.lists.updateName, {
      uuid: testData.uuid,
      name: updatedData.name
    });

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      convexPutOp('lists', syncedId(testData), listBucketRow(testData)),
      convexPutOp('lists', syncedId(updatedData), listBucketRow(updatedData))
    ]);
  });

  test('Replicate row deletions', async () => {
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const testData = await createList(context, { name: 'deleted-list' });
    await context.replicateSnapshot();

    await context.backend.client.mutation(context.backend.api.lists.deleteItem, {
      uuid: testData.uuid
    });

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      convexPutOp('lists', syncedId(testData), listBucketRow(testData)),
      removeOp('lists', syncedId(testData))
    ]);
  });

  test('Replicate matched wildcard tables in sync rules', async () => {
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT uuid as id, * FROM "%"
`);

    const list = await createList(context, { name: 'parent-list' });
    const todo = await createTodo(context, {
      listUuid: list.uuid,
      description: 'snapshot-todo'
    });

    await context.replicateSnapshot();

    const streamedList = await createList(context, { name: 'streamed-list' });
    const streamedTodo = await createTodo(context, {
      listUuid: streamedList.uuid,
      description: 'streamed-todo'
    });

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      convexOp('PUT', 'lists', syncedId(list)),
      convexOp('PUT', 'todos', syncedId(todo)),
      convexOp('PUT', 'lists', syncedId(streamedList)),
      convexOp('PUT', 'todos', syncedId(streamedTodo))
    ]);
  });

  test('Replication for tables not in the sync rules are ignored', async () => {
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const list = await createList(context, { name: 'synced-parent' });
    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    // Basic sync streams here don't replicate todo rows
    await createTodo(context, {
      listUuid: list.uuid,
      description: 'ignored-todo'
    });

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([convexPutOp('lists', syncedId(list), listBucketRow(list))]);

    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(0);
    expect(endTxCount - startTxCount).toEqual(0);
  });

  test('Table matching is case sensitive', async () => {
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT uuid as id, name FROM "Lists"
`);

    await context.replicateSnapshot();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    await createList(context, { name: 'case-sensitive-list' });

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([]);

    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(0);
    expect(endTxCount - startTxCount).toEqual(0);
  });
}

async function createList(context: ConvexStreamTestContext, options: { name: string }) {
  const [list] = await createLists(context, [options.name]);
  return list;
}

async function createLists(context: ConvexStreamTestContext, names: string[]) {
  const lists = names.map((name) => ({
    uuid: randomUUID(),
    name
  }));

  const ids = await context.backend.client.mutation(context.backend.api.lists.createBatch, {
    lists
  });

  return lists.map((list, index) => ({
    id: ids[index]!,
    uuid: list.uuid,
    name: list.name
  }));
}

async function createTodo(
  context: ConvexStreamTestContext,
  options: {
    listUuid: string;
    description: string;
  }
) {
  const uuid = randomUUID();
  const [id] = await context.backend.client.mutation(context.backend.api.todos.createBatch, {
    todos: [
      {
        uuid,
        list_uuid: options.listUuid,
        description: options.description
      }
    ]
  });

  return {
    id,
    uuid,
    listUuid: options.listUuid,
    description: options.description
  };
}

function syncedId(document: { uuid: string }) {
  return document.uuid;
}

function listBucketRow(document: { uuid: string; name: string }) {
  return {
    id: document.uuid,
    name: document.name
  };
}

/**
 * The order of ops do not match that done in a single mutation, this is due to out-of-order (internal) updates.
 */
function expectListOps(lists: Array<{ uuid: string; name: string }>) {
  return expect.arrayContaining(
    lists.map((list) => expect.objectContaining(convexPutOp('lists', syncedId(list), listBucketRow(list))))
  );
}

function convexPutOp(table: string, id: string, data: Record<string, unknown>) {
  return {
    ...convexOp('PUT', table, id),
    data: JSONBig.stringify(data)
  };
}

function convexOp(op: 'PUT' | 'REMOVE', table: string, id: string) {
  return {
    op,
    object_type: table,
    object_id: id
  };
}
