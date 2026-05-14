import { randomUUID } from 'node:crypto';

import { METRICS_HELPER, removeOp } from '@powersync/service-core-tests';
import { JSONBig } from '@powersync/service-jsonbig';
import { ReplicationMetric } from '@powersync/service-types';

import { describe, expect, test, vi } from 'vitest';
import { env } from '../env.js';
import { ConvexStreamTestContext } from '../test-utils/ConvexStreamTestContext.js';
import { describeWithStorage, StorageVersionTestContext } from '../test-utils/util.js';

import schema from '@testing-convex/schema.js';

type ListsData = typeof schema.tables.lists.validator.type;
type TodosData = Omit<typeof schema.tables.todos.validator.type, 'list_id'>;

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

    const sampleTodo: TodosData = {
      uuid: randomUUID(),
      description: 'A test',
      list_uuid: list.uuid,
      title: 'Typed snapshot todo',
      notes: 'notes',
      category: 'testing',
      priority: 3,
      points: 9_007_199_254_740_991n,
      estimated_hours: 12.5,
      progress_percentage: 87.25,
      is_urgent: true,
      is_private: false,
      has_attachments: true,
      attachment_data: Uint8Array.from([0, 1, 255]).buffer,
      tags: ['convex', 'sqlite'],
      assigned_users: ['alice', 'bob'],
      details: { label: 'detail', count: 7, nested: { enabled: true } },
      metadata: { count: 3, enabled: true, nested: { label: 'metadata' } },
      custom_fields: { score: 10, ready: true, owner: 'tester' },
      status: 'in_progress',
      difficulty: 'hard',
      explicit_null: null,
      archived_at: '2026-05-14T00:00:00.000Z',
      deleted_by: 'debugger'
    };

    const todo = await createTodo(context, sampleTodo);

    await context.replicateSnapshot();

    const streamedList = await createList(context, { name: 'streamed-list' });
    const streamedTodo = await createTodo(context, {
      ...sampleTodo,
      list_uuid: streamedList.uuid,
      description: 'streamed-todo'
    });

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      convexOp('PUT', 'lists', syncedId(list)),
      convexOp('PUT', 'todos', syncedId(todo)),
      convexOp('PUT', 'lists', syncedId(streamedList)),
      convexOp('PUT', 'todos', syncedId(streamedTodo))
    ]);

    // Now verifying the to sqlite typings
    const firstTodoOp = data[1];
    const parsedData = JSONBig.parse(firstTodoOp.data!) as Record<string, unknown>;

    expect(typeof parsedData.uuid).eq('string'),
      expect(typeof parsedData.description).eq('string'),
      expect(typeof parsedData.list_uuid).eq('string'),
      expect(typeof parsedData.description).eq('string'),
      expect(typeof parsedData.list_id).eq('string'),
      expect(typeof parsedData.title).eq('string'),
      expect(typeof parsedData.notes).eq('string'),
      expect(typeof parsedData.category).eq('string'),
      expect(typeof parsedData.priority).eq('number'), // This is a regular v.number()
      expect(typeof parsedData.points).eq('bigint'),
      expect(typeof parsedData.estimated_hours).eq('number'),
      expect(typeof parsedData.progress_percentage).eq('number'),
      expect(typeof parsedData.is_urgent).eq('bigint'), // boolean
      expect(typeof parsedData.is_private).eq('bigint'), // boolean
      expect(typeof parsedData.has_attachments).eq('bigint'), // boolean
      // TODO, should this have been persisted? I believe this requires a sync config cast in order to work.
      // expect(typeof parsedData.attachment_data).eq('string'), // buffer
      expect(typeof parsedData.tags).eq('string'), // array
      expect(typeof parsedData.assigned_users).eq('string'), //array
      expect(typeof parsedData.details).eq('string'),
      expect(typeof parsedData.explicit_null).eq('object'), // null
      expect(typeof parsedData.archived_at).eq('string'); // '2026-05-14T00:00:00.000Z',
  });

  /**
   * It seems like the json-schema's route will not return JSON schema values for table
   * columns if not populated value exists for the row yet.
   * This test simulates this behaviour. We do an initial replication and start streaming
   * before adding any todo records. We create a todo record while streaming.
   */
  test('Replicate values when initial snapshot did not include data', async () => {
    await using context = await ConvexStreamTestContext.open(factory, { storageVersion });
    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT uuid as id, * FROM lists
      - SELECT uuid as id, * FROM todos

`);

    const list = await createList(context, { name: 'parent-list' });

    // This one does not include some optional column values.
    // Replicating this will cause the schema for todos to be queried and cached.
    const firstTodo = await createTodo(context, {
      description: 'the first one, to be recorded in initial snapshot',
      list_uuid: list.uuid,
      uuid: randomUUID()
    });

    // This starts the replication of the initial snapshot and also then streams
    await context.replicateSnapshot();

    await new Promise((r) => setTimeout(r, 1_000));

    // Create a new todo record
    const sampleTodo: TodosData = {
      uuid: randomUUID(),
      description: 'A test',
      list_uuid: list.uuid,
      title: 'Typed snapshot todo',
      notes: 'notes',
      category: 'testing',
      priority: 3,
      points: 9_007_199_254_740_991n,
      estimated_hours: 12.5,
      progress_percentage: 87.25,
      is_urgent: true,
      is_private: false,
      has_attachments: true,
      attachment_data: Uint8Array.from([0, 1, 255]).buffer,
      tags: ['convex', 'sqlite'],
      assigned_users: ['alice', 'bob'],
      details: { label: 'detail', count: 7, nested: { enabled: true } },
      metadata: { count: 3, enabled: true, nested: { label: 'metadata' } },
      custom_fields: { score: 10, ready: true, owner: 'tester' },
      status: 'in_progress',
      difficulty: 'hard',
      explicit_null: null,
      archived_at: '2026-05-14T00:00:00.000Z',
      deleted_by: 'debugger'
    };

    const streamedList = await createList(context, { name: 'streamed-list' });
    const streamedTodo = await createTodo(context, {
      ...sampleTodo,
      list_uuid: streamedList.uuid,
      description: 'streamed-todo'
    });

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      convexOp('PUT', 'lists', syncedId(list)),
      convexOp('PUT', 'todos', syncedId(firstTodo)),
      convexOp('PUT', 'lists', syncedId(streamedList)),
      convexOp('PUT', 'todos', syncedId(streamedTodo))
    ]);

    // Now verifying the to sqlite typings
    const firstTodoOp = data[3];
    const parsedData = JSONBig.parse(firstTodoOp.data!) as Record<string, unknown>;

    expect(typeof parsedData.uuid).eq('string'),
      expect(typeof parsedData.description).eq('string'),
      expect(typeof parsedData.list_uuid).eq('string'),
      expect(typeof parsedData.description).eq('string'),
      expect(typeof parsedData.list_id).eq('string'),
      expect(typeof parsedData.title).eq('string'),
      expect(typeof parsedData.notes).eq('string'),
      expect(typeof parsedData.category).eq('string'),
      expect(typeof parsedData.priority).eq('number'), // This is a regular v.number()
      /**
       * This is where the logic breaks down.
       * We fetched the schema for todos before any values for points (a int64 column)
       * were present. This means the json-schemas result did not include an entry for `points`.
       * We then receive a string value for the points value in the document-deltas endpoint.
       * We don't have a schema for this column, so we can only do runtime inspection for this value -
       * which results in us storing the value as is.
       * This results in the type being string instead of bigint.
       */
      // expect(typeof parsedData.points).eq('bigint'),
      expect(typeof parsedData.points).eq('string'),
      expect(typeof parsedData.estimated_hours).eq('number'),
      expect(typeof parsedData.progress_percentage).eq('number'),
      expect(typeof parsedData.is_urgent).eq('bigint'), // boolean
      expect(typeof parsedData.is_private).eq('bigint'), // boolean
      expect(typeof parsedData.has_attachments).eq('bigint'), // boolean
      // TODO, should this have been persisted? I believe this requires a sync config cast in order to work.
      // expect(typeof parsedData.attachment_data).eq('string'), // buffer
      expect(typeof parsedData.tags).eq('string'), // array
      expect(typeof parsedData.assigned_users).eq('string'), //array
      expect(typeof parsedData.details).eq('string'),
      expect(typeof parsedData.explicit_null).eq('object'), // null
      expect(typeof parsedData.archived_at).eq('string'); // '2026-05-14T00:00:00.000Z',
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
      uuid: randomUUID(),
      list_uuid: list.uuid,
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

async function createTodo(context: ConvexStreamTestContext, options: TodosData) {
  const [id] = await context.backend.client.mutation(context.backend.api.todos.createBatch, {
    todos: [options]
  });

  return {
    id,
    ...options
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
