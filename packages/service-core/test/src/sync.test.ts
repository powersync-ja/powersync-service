import { RequestTracker } from '@/sync/RequestTracker.js';
import { StreamingSyncLine } from '@/util/protocol-types.js';
import { lsnMakeComparable } from '@powersync/service-jpgwire';
import { JSONBig } from '@powersync/service-jsonbig';
import { RequestParameters } from '@powersync/service-sync-rules';
import * as timers from 'timers/promises';
import { describe, expect, test } from 'vitest';
import { ZERO_LSN } from '../../src/replication/WalStream.js';
import { streamResponse } from '../../src/sync/sync.js';
import { makeTestTable, MONGO_STORAGE_FACTORY, StorageFactory } from './util.js';

describe('sync - mongodb', function () {
  defineTests(MONGO_STORAGE_FACTORY);
});

const TEST_TABLE = makeTestTable('test', ['id']);

const BASIC_SYNC_RULES = `
bucket_definitions:
  mybucket:
    data:
      - SELECT * FROM test
    `;

function defineTests(factory: StorageFactory) {
  const tracker = new RequestTracker();

  test('sync global data', async () => {
    const f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const storage = await f.getInstance(syncRules.parsed());
    await storage.setSnapshotDone(ZERO_LSN);
    await storage.autoActivate();

    const result = await storage.startBatch({}, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: 'insert',
        after: {
          id: 't1',
          description: 'Test 1'
        }
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: 'insert',
        after: {
          id: 't2',
          description: 'Test 2'
        }
      });

      await batch.commit(lsnMakeComparable('0/1'));
    });

    const stream = streamResponse({
      storage: f,
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

  test('sync legacy non-raw data', async () => {
    const f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const storage = await f.getInstance(syncRules.parsed());
    await storage.setSnapshotDone(ZERO_LSN);
    await storage.autoActivate();

    const result = await storage.startBatch({}, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: 'insert',
        after: {
          id: 't1',
          description: 'Test\n"string"',
          large_num: 12345678901234567890n
        }
      });

      await batch.commit(lsnMakeComparable('0/1'));
    });

    const stream = streamResponse({
      storage: f,
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
    const f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const storage = await f.getInstance(syncRules.parsed());
    await storage.setSnapshotDone(ZERO_LSN);
    await storage.autoActivate();

    const stream = streamResponse({
      storage: f,
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

  test('sync updates to global data', async () => {
    const f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const storage = await f.getInstance(syncRules.parsed());
    await storage.setSnapshotDone(ZERO_LSN);
    await storage.autoActivate();

    const stream = streamResponse({
      storage: f,
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

    expect(await getCheckpointLines(iter)).toMatchSnapshot();

    await storage.startBatch({}, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: 'insert',
        after: {
          id: 't1',
          description: 'Test 1'
        }
      });

      await batch.commit(lsnMakeComparable('0/1'));
    });

    expect(await getCheckpointLines(iter)).toMatchSnapshot();

    await storage.startBatch({}, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: 'insert',
        after: {
          id: 't2',
          description: 'Test 2'
        }
      });

      await batch.commit(lsnMakeComparable('0/2'));
    });

    expect(await getCheckpointLines(iter)).toMatchSnapshot();

    iter.return?.();
  });

  test('expiring token', async () => {
    const f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const storage = await f.getInstance(syncRules.parsed());
    await storage.setSnapshotDone(ZERO_LSN);
    await storage.autoActivate();

    const exp = Date.now() / 1000 + 0.1;

    const stream = streamResponse({
      storage: f,
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

    const f = await factory();

    const syncRules = await f.updateSyncRules({
      content: BASIC_SYNC_RULES
    });

    const storage = await f.getInstance(syncRules.parsed());
    await storage.setSnapshotDone(ZERO_LSN);
    await storage.autoActivate();

    await storage.startBatch({}, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: 'insert',
        after: {
          id: 't1',
          description: 'Test 1'
        }
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: 'insert',
        after: {
          id: 't2',
          description: 'Test 2'
        }
      });

      await batch.commit(lsnMakeComparable('0/1'));
    });

    const stream = streamResponse({
      storage: f,
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

    await storage.startBatch({}, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: 'update',
        after: {
          id: 't1',
          description: 'Test 1b'
        }
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: 'update',
        after: {
          id: 't2',
          description: 'Test 2b'
        }
      });

      await batch.commit(lsnMakeComparable('0/2'));
    });

    await storage.compact();

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
  iter: AsyncIterator<StreamingSyncLine | string | null>,
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
async function consumeCheckpointLines(iterable: AsyncIterable<StreamingSyncLine | string | null>): Promise<any[]> {
  return getCheckpointLines(iterable[Symbol.asyncIterator](), { consume: true });
}
