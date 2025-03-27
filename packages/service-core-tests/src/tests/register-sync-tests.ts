import {
  CheckpointLine,
  storage,
  StreamingSyncCheckpoint,
  StreamingSyncCheckpointDiff,
  sync,
  utils
} from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { RequestParameters } from '@powersync/service-sync-rules';
import path from 'path';
import * as timers from 'timers/promises';
import { fileURLToPath } from 'url';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TEST_TABLE = test_utils.makeTestTable('test', ['id']);

const BASIC_SYNC_RULES = `
bucket_definitions:
  mybucket:
    data:
      - SELECT * FROM test
    `;

export const SYNC_SNAPSHOT_PATH = path.resolve(__dirname, '../__snapshots/sync.test.js.snap');

/**
 * @example
 * ```TypeScript
 * describe('sync - mongodb', function () {
 * registerSyncTests(MONGO_STORAGE_FACTORY);
 * });
 * ```
 */
export function registerSyncTests(factory: storage.TestStorageFactory) {
  const tracker = new sync.RequestTracker();
  const syncContext = new sync.SyncContext({
    maxBuckets: 10,
    maxParameterQueryResults: 10,
    maxDataFetchConcurrency: 2
  });

  test('sync global data', async () => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          description: 'Test 1'
        },
        afterReplicaId: 't1'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2',
          description: 'Test 2'
        },
        afterReplicaId: 't2'
      });

      await batch.commit('0/1');
    });

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage: bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 10 } as any
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
  });

  test('sync buckets in order', async () => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: `
bucket_definitions:
  b0:
    priority: 2
    data:
      - SELECT * FROM test WHERE LENGTH(id) <= 2;
  b1:
    priority: 1
    data:
      - SELECT * FROM test WHERE LENGTH(id) > 2;
    `
    });

    const bucketStorage = f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          description: 'Test 1'
        },
        afterReplicaId: 't1'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'earlier',
          description: 'Test 2'
        },
        afterReplicaId: 'earlier'
      });

      await batch.commit('0/1');
    });

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 10 } as any
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
  });

  // Temporarily skipped - interruption disabled
  test.skip('sync interrupts low-priority buckets on new checkpoints', async () => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: `
bucket_definitions:
  b0:
    priority: 2
    data:
      - SELECT * FROM test WHERE LENGTH(id) <= 5;
  b1:
    priority: 1
    data:
      - SELECT * FROM test WHERE LENGTH(id) > 5;
    `
    });

    const bucketStorage = f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Initial data: Add one priority row and 10k low-priority rows.
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'highprio',
          description: 'High priority row'
        },
        afterReplicaId: 'highprio'
      });
      for (let i = 0; i < 10_000; i++) {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: `${i}`,
            description: 'low prio'
          },
          afterReplicaId: `${i}`
        });
      }

      await batch.commit('0/1');
    });

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 10 } as any
    });

    let sentCheckpoints = 0;
    let sentRows = 0;

    for await (let next of stream) {
      if (typeof next == 'string') {
        next = JSON.parse(next);
      }
      if (typeof next === 'object' && next !== null) {
        if ('partial_checkpoint_complete' in next) {
          if (sentCheckpoints == 1) {
            // Save new data to interrupt the low-priority sync.

            await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
              // Add another high-priority row. This should interrupt the long-running low-priority sync.
              await batch.save({
                sourceTable: TEST_TABLE,
                tag: storage.SaveOperationTag.INSERT,
                after: {
                  id: 'highprio2',
                  description: 'Another high-priority row'
                },
                afterReplicaId: 'highprio2'
              });

              await batch.commit('0/2');
            });
          } else {
            // Low-priority sync from the first checkpoint was interrupted. This should not happen before
            // 1000 low-priority items were synchronized.
            expect(sentCheckpoints).toBe(2);
            expect(sentRows).toBeGreaterThan(1000);
          }
        }
        if ('checkpoint' in next || 'checkpoint_diff' in next) {
          sentCheckpoints += 1;
        }

        if ('data' in next) {
          sentRows += next.data.data.length;
        }
        if ('checkpoint_complete' in next) {
          break;
        }
      }
    }

    expect(sentCheckpoints).toBe(2);
    expect(sentRows).toBe(10002);
  });

  test('sends checkpoint complete line for empty checkpoint', async () => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });
    const bucketStorage = f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          description: 'sync'
        },
        afterReplicaId: 't1'
      });
      await batch.commit('0/1');
    });

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 100000 } as any
    });

    const lines: any[] = [];
    let receivedCompletions = 0;

    for await (let next of stream) {
      if (typeof next == 'string') {
        next = JSON.parse(next);
      }
      lines.push(next);

      if (typeof next === 'object' && next !== null) {
        if ('checkpoint_complete' in next) {
          receivedCompletions++;
          if (receivedCompletions == 1) {
            // Trigger an empty bucket update.
            await bucketStorage.createManagedWriteCheckpoint({ user_id: '', heads: { '1': '1/0' } });
            await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
              await batch.commit('1/0');
            });
          } else {
            break;
          }
        }
      }
    }

    expect(lines).toMatchSnapshot();
  });

  test('sync legacy non-raw data', async () => {
    const f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    const result = await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          description: 'Test\n"string"',
          large_num: 12345678901234567890n
        },
        afterReplicaId: 't1'
      });

      await batch.commit('0/1');
    });

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: false
      },
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 10 } as any
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
    // Specifically check the number - this is the important part of the test
    expect(lines[1].data.data[0].data.large_num).toEqual(12345678901234567890n);
  });

  test('expired token', async () => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: 0 } as any
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
  });

  test('sync updates to global data', async (context) => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 10 } as any
    });
    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    expect(await getCheckpointLines(iter)).toMatchSnapshot();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          description: 'Test 1'
        },
        afterReplicaId: 't1'
      });

      await batch.commit('0/1');
    });

    expect(await getCheckpointLines(iter)).toMatchSnapshot();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2',
          description: 'Test 2'
        },
        afterReplicaId: 't2'
      });

      await batch.commit('0/2');
    });

    expect(await getCheckpointLines(iter)).toMatchSnapshot();
  });

  test('sync updates to parameter query only', async (context) => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: `bucket_definitions:
  by_user:
    parameters: select users.id as user_id from users where users.id = request.user_id()
    data:
      - select * from lists where user_id = bucket.user_id
`
    });

    const usersTable = test_utils.makeTestTable('users', ['id']);
    const listsTable = test_utils.makeTestTable('lists', ['id']);

    const bucketStorage = await f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: 'user1' }, {}),
      token: { exp: Date.now() / 1000 + 100 } as any
    });
    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    // Initial empty checkpoint
    const checkpoint1 = await getCheckpointLines(iter);
    expect((checkpoint1[0] as StreamingSyncCheckpoint).checkpoint?.buckets?.map((b) => b.bucket)).toEqual([]);
    expect(checkpoint1).toMatchSnapshot();

    // Add user
    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: usersTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'user1',
          name: 'User 1'
        },
        afterReplicaId: 'user1'
      });

      await batch.commit('0/1');
    });

    const checkpoint2 = await getCheckpointLines(iter);
    expect(
      (checkpoint2[0] as StreamingSyncCheckpointDiff).checkpoint_diff?.updated_buckets?.map((b) => b.bucket)
    ).toEqual(['by_user["user1"]']);
    expect(checkpoint2).toMatchSnapshot();
  });

  test('sync updates to data query only', async (context) => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: `bucket_definitions:
  by_user:
    parameters: select users.id as user_id from users where users.id = request.user_id()
    data:
      - select * from lists where user_id = bucket.user_id
`
    });

    const usersTable = test_utils.makeTestTable('users', ['id']);
    const listsTable = test_utils.makeTestTable('lists', ['id']);

    const bucketStorage = await f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: usersTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'user1',
          name: 'User 1'
        },
        afterReplicaId: 'user1'
      });

      await batch.commit('0/1');
    });

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: 'user1' }, {}),
      token: { exp: Date.now() / 1000 + 100 } as any
    });
    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    const checkpoint1 = await getCheckpointLines(iter);
    expect((checkpoint1[0] as StreamingSyncCheckpoint).checkpoint?.buckets?.map((b) => b.bucket)).toEqual([
      'by_user["user1"]'
    ]);
    expect(checkpoint1).toMatchSnapshot();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: listsTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'list1',
          user_id: 'user1',
          name: 'User 1'
        },
        afterReplicaId: 'list1'
      });

      await batch.commit('0/1');
    });

    const checkpoint2 = await getCheckpointLines(iter);
    expect(
      (checkpoint2[0] as StreamingSyncCheckpointDiff).checkpoint_diff?.updated_buckets?.map((b) => b.bucket)
    ).toEqual(['by_user["user1"]']);
    expect(checkpoint2).toMatchSnapshot();
  });

  test('sync updates to parameter query + data', async (context) => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: `bucket_definitions:
  by_user:
    parameters: select users.id as user_id from users where users.id = request.user_id()
    data:
      - select * from lists where user_id = bucket.user_id
`
    });

    const usersTable = test_utils.makeTestTable('users', ['id']);
    const listsTable = test_utils.makeTestTable('lists', ['id']);

    const bucketStorage = await f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: 'user1' }, {}),
      token: { exp: Date.now() / 1000 + 100 } as any
    });
    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    // Initial empty checkpoint
    expect(await getCheckpointLines(iter)).toMatchSnapshot();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: listsTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'list1',
          user_id: 'user1',
          name: 'User 1'
        },
        afterReplicaId: 'list1'
      });

      await batch.save({
        sourceTable: usersTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'user1',
          name: 'User 1'
        },
        afterReplicaId: 'user1'
      });

      await batch.commit('0/1');
    });

    const checkpoint2 = await getCheckpointLines(iter);
    expect(
      (checkpoint2[0] as StreamingSyncCheckpointDiff).checkpoint_diff?.updated_buckets?.map((b) => b.bucket)
    ).toEqual(['by_user["user1"]']);
    expect(checkpoint2).toMatchSnapshot();
  });

  test('expiring token', async (context) => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    const exp = Date.now() / 1000 + 0.1;

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: exp } as any
    });
    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    const checkpoint = await getCheckpointLines(iter);
    expect(checkpoint).toMatchSnapshot();

    const expLines = await getCheckpointLines(iter);
    expect(expLines).toMatchSnapshot();
  });

  test('compacting data - invalidate checkpoint', async (context) => {
    // This tests a case of a compact operation invalidating a checkpoint in the
    // middle of syncing data.
    // This is expected to be rare in practice, but it is important to handle
    // this case correctly to maintain consistency on the client.

    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          description: 'Test 1'
        },
        afterReplicaId: 't1'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2',
          description: 'Test 2'
        },
        afterReplicaId: 't2'
      });

      await batch.commit('0/1');
    });

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 10 } as any
    });

    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    // Only consume the first "checkpoint" message, and pause before receiving data.
    const lines = await consumeIterator(iter, { consume: false, isDone: (line) => (line as any)?.checkpoint != null });
    expect(lines).toMatchSnapshot();
    expect(lines[0]).toEqual({
      checkpoint: expect.objectContaining({
        last_op_id: '2'
      })
    });

    // Now we save additional data AND compact before continuing.
    // This invalidates the checkpoint we've received above.

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 't1',
          description: 'Test 1b'
        },
        afterReplicaId: 't1'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 't2',
          description: 'Test 2b'
        },
        afterReplicaId: 't2'
      });

      await batch.commit('0/2');
    });

    await bucketStorage.compact();

    const lines2 = await getCheckpointLines(iter, { consume: true });

    // Snapshot test checks for changes in general.
    // The tests after that documents the specific things we're looking for
    // in this test.
    expect(lines2).toMatchSnapshot();

    expect(lines2[0]).toEqual({
      data: expect.objectContaining({
        has_more: false,
        data: [
          // The first two ops have been replaced by a single CLEAR op
          expect.objectContaining({
            op: 'CLEAR'
          })
        ]
      })
    });

    // Note: No checkpoint_complete here, since the checkpoint has been
    // invalidated by the CLEAR op.

    expect(lines2[1]).toEqual({
      checkpoint_diff: expect.objectContaining({
        last_op_id: '4'
      })
    });

    expect(lines2[2]).toEqual({
      data: expect.objectContaining({
        has_more: false,
        data: [
          expect.objectContaining({
            op: 'PUT'
          }),
          expect.objectContaining({
            op: 'PUT'
          })
        ]
      })
    });

    // Now we get a checkpoint_complete
    expect(lines2[3]).toEqual({
      checkpoint_complete: expect.objectContaining({
        last_op_id: '4'
      })
    });
  });

  test('write checkpoint', async () => {
    await using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // <= the managed write checkpoint LSN below
      await batch.commit('0/1');
    });

    const checkpoint = await bucketStorage.createManagedWriteCheckpoint({
      user_id: 'test',
      heads: { '1': '1/0' }
    });

    const params: sync.SyncStreamParameters = {
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      tracker,
      syncParams: new RequestParameters({ sub: 'test' }, {}),
      token: { sub: 'test', exp: Date.now() / 1000 + 10 } as any
    };
    const stream1 = sync.streamResponse(params);
    const lines1 = await consumeCheckpointLines(stream1);

    // If write checkpoints are not correctly filtered, this may already
    // contain the write checkpoint.
    expect(lines1[0]).toMatchObject({
      checkpoint: expect.objectContaining({
        last_op_id: '0',
        write_checkpoint: undefined
      })
    });

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // must be >= the managed write checkpoint LSN
      await batch.commit('1/0');
    });

    // At this point the LSN has advanced, so the write checkpoint should be
    // included in the next checkpoint message.
    const stream2 = sync.streamResponse(params);
    const lines2 = await consumeCheckpointLines(stream2);
    expect(lines2[0]).toMatchObject({
      checkpoint: expect.objectContaining({
        last_op_id: '0',
        write_checkpoint: `${checkpoint}`
      })
    });
  });
}

/**
 * Get lines on an iterator until isDone(line) == true.
 *
 * Does not stop the iterator unless options.consume is true.
 */
async function consumeIterator<T>(
  iter: AsyncIterator<T>,
  options: { isDone: (line: T) => boolean; consume?: boolean }
) {
  let lines: T[] = [];
  try {
    const controller = new AbortController();
    const timeout = timers.setTimeout(1500, { value: null, done: 'timeout' }, { signal: controller.signal });
    while (true) {
      let { value, done } = await Promise.race([timeout, iter.next()]);
      if (done == 'timeout') {
        throw new Error('Timeout');
      }
      if (typeof value == 'string') {
        value = JSONBig.parse(value);
      }
      if (value) {
        lines.push(value);
      }
      if (done || options.isDone(value)) {
        break;
      }
    }
    controller.abort();

    if (options?.consume) {
      iter.return?.();
    }
    return lines;
  } catch (e) {
    if (options?.consume) {
      iter.throw?.(e);
    }
    throw e;
  }
}

/**
 * Get lines on an iterator until the next checkpoint_complete.
 *
 * Does not stop the iterator unless options.consume is true.
 */
async function getCheckpointLines(
  iter: AsyncIterator<utils.StreamingSyncLine | string | null>,
  options?: { consume?: boolean }
) {
  return consumeIterator(iter, {
    consume: options?.consume,
    isDone: (line) => (line as any)?.checkpoint_complete
  });
}

/**
 * Get lines on an iterator until the next checkpoint_complete.
 *
 * Stops the iterator afterwards.
 */
async function consumeCheckpointLines(
  iterable: AsyncIterable<utils.StreamingSyncLine | string | null>
): Promise<any[]> {
  return getCheckpointLines(iterable[Symbol.asyncIterator](), { consume: true });
}
