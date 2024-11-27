import { storage, sync, utils } from '@powersync/service-core';
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
export function registerSyncTests(factory: test_utils.StorageFactory) {
  const tracker = new sync.RequestTracker();

  test('sync global data', async () => {
    using f = await factory();

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
      storage: f,
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      parseOptions: test_utils.PARSE_OPTIONS,
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 10 } as any
    });

    const lines = await consumeCheckpointLines(stream);
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
      storage: f,
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: false
      },
      parseOptions: test_utils.PARSE_OPTIONS,
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
    const f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const storage = await f.getInstance(syncRules);
    await storage.autoActivate();

    const stream = sync.streamResponse({
      storage: f,
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      parseOptions: test_utils.PARSE_OPTIONS,
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: 0 } as any
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
  });

  test('sync updates to global data', async () => {
    using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await bucketStorage.autoActivate();

    const stream = sync.streamResponse({
      storage: f,
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      parseOptions: test_utils.PARSE_OPTIONS,
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 10 } as any
    });
    const iter = stream[Symbol.asyncIterator]();

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

    iter.return?.();
  });

  test('expiring token', async () => {
    using f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const storage = await f.getInstance(syncRules);
    await storage.autoActivate();

    const exp = Date.now() / 1000 + 0.1;

    const stream = sync.streamResponse({
      storage: f,
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      parseOptions: test_utils.PARSE_OPTIONS,
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: exp } as any
    });
    const iter = stream[Symbol.asyncIterator]();

    const checkpoint = await getCheckpointLines(iter);
    expect(checkpoint).toMatchSnapshot();

    const expLines = await getCheckpointLines(iter);
    expect(expLines).toMatchSnapshot();
  });

  test('compacting data - invalidate checkpoint', async () => {
    // This tests a case of a compact operation invalidating a checkpoint in the
    // middle of syncing data.
    // This is expected to be rare in practice, but it is important to handle
    // this case correctly to maintain consistency on the client.

    using f = await factory();

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
      storage: f,
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      parseOptions: test_utils.PARSE_OPTIONS,
      tracker,
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: Date.now() / 1000 + 10 } as any
    });

    const iter = stream[Symbol.asyncIterator]();

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
    using f = await factory();

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
      storage: f,
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true
      },
      parseOptions: test_utils.PARSE_OPTIONS,
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
async function consumeCheckpointLines(iterable: AsyncIterable<utils.StreamingSyncLine | string | null>): Promise<any[]> {
  return getCheckpointLines(iterable[Symbol.asyncIterator](), { consume: true });
}
