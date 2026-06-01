import { randomUUID } from 'node:crypto';
import { describe, expect, test, vi } from 'vitest';
import { env } from '../env.js';
import { ConvexStreamTestContext } from '../test-utils/ConvexStreamTestContext.js';
import { describeWithStorage, StorageVersionTestContext } from '../test-utils/util.js';

describe.skipIf(!(env.CI || env.SLOW_TESTS))('batch replication', function () {
  describeWithStorage({ timeout: 240_000 }, function ({ factory, storageVersion }) {
    test('resuming initial replication', async () => {
      // The initial replication will be split into
      // 1 - The first 1001 list records
      // 2 - A batch of 1000 todo records
      // 3 - Another 1000 batch of todo records
      // We interupt the todos after batch 2 (in the middle of 3)
      await testResumingReplication(factory, storageVersion, 2500);
    });
  });
});

async function testResumingReplication(
  factory: StorageVersionTestContext['factory'],
  storageVersion: number,
  stopAfter: number
) {
  // This tests interrupting and then resuming initial replication.
  // We interrupt replication after lists has fully replicated, and
  // todos has partially replicated.
  // This test relies on interval behavior that is not 100% deterministic:
  // 1. We attempt to abort initial replication once a certain number of
  //    rows have been replicated, but this is not exact. Our only requirement
  //    is that we have not fully replicated todos yet.
  // 2. Order of replication is not deterministic, so which specific rows
  //    have been / have not been replicated at that point is not deterministic.
  //    We do allow for some variation in the test results to account for this.

  await using context = await ConvexStreamTestContext.open(factory, {
    storageVersion
  });

  await context.updateSyncRules(/* yaml */ `bucket_definitions:
      global:
        data:
          - SELECT uuid as id, * FROM lists
          - SELECT uuid as id, * FROM todos `);

  const { backend } = context;

  // Seed the database
  // Max number of mutations is batch size supported is 8192
  // Maximum number of reads is 4096 in a single mutation
  await backend.client.mutation(backend.api.lists.createBatch, {
    lists: Array.from({ length: 1_000 }).map((_, index) => ({
      uuid: randomUUID(),
      name: `list-${index}`
    }))
  });
  // Delay to avoid TooManyWrites error from Convex
  await new Promise((r) => setTimeout(r, 1_000));

  // create a single row to track deleted items
  const deletableListId = randomUUID();
  await backend.client.mutation(backend.api.lists.createBatch, {
    lists: [
      {
        uuid: deletableListId,
        name: 'parent'
      }
    ]
  });
  // create a single one with a tracked uuid for relationships
  const relationalListId = randomUUID();
  await backend.client.mutation(backend.api.lists.createBatch, {
    lists: [
      {
        uuid: relationalListId,
        name: 'parent'
      }
    ]
  });
  await backend.client.mutation(backend.api.todos.createBatch, {
    todos: Array.from({ length: 2_000 }).map((_, index) => ({
      uuid: randomUUID(),
      list_uuid: relationalListId,
      description: `todo-${index}`
    }))
  });
  // Delay to avoid TooManyWrites error from Convex
  await new Promise((r) => setTimeout(r, 1_000));
  // twice in order to get many todos (see limits above)
  await backend.client.mutation(backend.api.todos.createBatch, {
    todos: Array.from({ length: 2_000 }).map((_, index) => ({
      uuid: randomUUID(),
      list_uuid: relationalListId,
      description: `todo-${index}`
    }))
  });
  // Delay to avoid TooManyWrites error from Convex
  await new Promise((r) => setTimeout(r, 1_000));

  let stopped = new Promise<void>((resolve) => {
    context.storage!.registerListener({
      batchStarted: (batch) => {
        //register a pre-emptive spy in order to halt writes
        const original = batch.save;
        let savedCount = 0;
        vi.spyOn(batch, 'save').mockImplementation(async (param) => {
          if (savedCount >= stopAfter) {
            // This interrupts initial replication
            // don't await this since awaiting will cause a deadlock
            context.dispose();
            resolve();
            throw new Error('Stopping now');
          }
          savedCount++;
          return original.call(batch, param);
        });
      }
    });
  });

  const replicationError = context.replicateSnapshot().catch((error) => error);

  await stopped;
  await replicationError;

  // Add delete a row which has already been replicated
  await backend.client.mutation(backend.api.lists.deleteItem, {
    uuid: deletableListId
  });

  await using context2 = await ConvexStreamTestContext.open(factory, {
    doNotClear: true,
    storageVersion
  });

  // Spy on the list-snapshot endpoint
  const snapshotSpy = vi.spyOn(context2.connectionManager.client, 'listSnapshot');

  await context2.loadNextSyncRules();
  await context2.replicateSnapshot();

  // The second replication should have called list snapshot for the todos table, at a specific cursor value (resuming)
  expect(snapshotSpy).called;
  const firstCall = snapshotSpy.mock.calls[0];
  expect(firstCall).toBeDefined();
  expect(firstCall[0].tableName).eq('todos');
  expect(firstCall[0].cursor).toBeDefined(); //it resumed within the table

  // Check the final bucket data
  const data = await context2.getBucketData('global[]', undefined, {});
  expect(data.length).eq(5003); // Puts: 1000 + 1 + 1 + 2000 + 2000, REMOVE: 1
  expect(data.filter((item) => item.op == 'PUT').length).eq(5002);
  expect(data.filter((item) => item.op == 'REMOVE').length).eq(1);
}
