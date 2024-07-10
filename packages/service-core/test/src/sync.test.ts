import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { ZERO_LSN } from '../../src/replication/WalStream.js';
import { SourceTable } from '../../src/storage/SourceTable.js';
import { hashData } from '../../src/util/utils.js';
import { MONGO_STORAGE_FACTORY, StorageFactory } from './util.js';
import { JSONBig } from '@powersync/service-jsonbig';
import { streamResponse } from '../../src/sync/sync.js';
import * as timers from 'timers/promises';
import { lsnMakeComparable } from '@powersync/service-jpgwire';
import { RequestParameters } from '@powersync/service-sync-rules';

describe('sync - mongodb', function () {
  defineTests(MONGO_STORAGE_FACTORY);
});

function makeTestTable(name: string, columns?: string[] | undefined) {
  const relId = hashData('table', name, (columns ?? ['id']).join(','));
  const id = new bson.ObjectId('6544e3899293153fa7b38331');
  return new SourceTable(
    id,
    SourceTable.DEFAULT_TAG,
    relId,
    SourceTable.DEFAULT_SCHEMA,
    name,
    (columns ?? ['id']).map((column) => ({ name: column, typeOid: 25 })),
    true
  );
}

const TEST_TABLE = makeTestTable('test', ['id']);

const BASIC_SYNC_RULES = `
bucket_definitions:
  mybucket:
    data:
      - SELECT * FROM test
    `;

function defineTests(factory: StorageFactory) {
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
      syncParams: new RequestParameters({ sub: '' }, {}),
      token: { exp: exp } as any
    });
    const iter = stream[Symbol.asyncIterator]();

    const checkpoint = await getCheckpointLines(iter);
    expect(checkpoint).toMatchSnapshot();

    const expLines = await getCheckpointLines(iter);
    expect(expLines).toMatchSnapshot();
  });
}

/**
 * Get lines on an iterator until the next checkpoint_complete.
 *
 * Does not stop the iterator.
 */
async function getCheckpointLines(iter: AsyncIterator<any>, options?: { consume?: boolean }): Promise<any[]> {
  let lines: any[] = [];
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
      if (done || value.checkpoint_complete) {
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
 * Stops the iterator afterwards.
 */
async function consumeCheckpointLines(iterable: AsyncIterable<any>): Promise<any[]> {
  return getCheckpointLines(iterable[Symbol.asyncIterator](), { consume: true });
}
