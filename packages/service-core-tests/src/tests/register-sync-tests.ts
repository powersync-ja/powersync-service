import {
  createCoreAPIMetrics,
  JwtPayload,
  storage,
  StreamingSyncCheckpoint,
  StreamingSyncCheckpointDiff,
  sync,
  updateSyncRulesFromYaml,
  utils
} from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import path from 'path';
import * as timers from 'timers/promises';
import { fileURLToPath } from 'url';
import { expect, test, vi } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';
import { bucketRequest, METRICS_HELPER } from '../test-utils/test-utils-index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

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
export function registerSyncTests(
  configOrFactory: storage.TestStorageConfig | storage.TestStorageFactory,
  options: { storageVersion?: number; tableIdStrings?: boolean } = {}
) {
  const config: storage.TestStorageConfig =
    typeof configOrFactory == 'function'
      ? { factory: configOrFactory, tableIdStrings: options.tableIdStrings ?? true }
      : configOrFactory;
  const factory = config.factory;
  createCoreAPIMetrics(METRICS_HELPER.metricsEngine);
  const tracker = new sync.RequestTracker(METRICS_HELPER.metricsEngine);
  const syncContext = new sync.SyncContext({
    maxBuckets: 10,
    maxParameterQueryResults: 10,
    maxDataFetchConcurrency: 2
  });

  const updateSyncRules = (bucketStorageFactory: storage.BucketStorageFactory, updateOptions: { content: string }) => {
    return bucketStorageFactory.updateSyncRules(
      updateSyncRulesFromYaml(updateOptions.content, {
        validate: true,
        storageVersion: options.storageVersion
      })
    );
  };

  test('sync global data', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('0/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        description: 'Test 1'
      },
      afterReplicaId: 't1'
    });

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2',
        description: 'Test 2'
      },
      afterReplicaId: 't2'
    });

    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
  });

  test('sync buckets in order', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
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
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        description: 'Test 1'
      },
      afterReplicaId: 't1'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'earlier',
        description: 'Test 2'
      },
      afterReplicaId: 'earlier'
    });

    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
  });

  test('can override priority when subscribing to stream', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: `
config:
  edition: 3

streams:
  todos:
    query: SELECT * FROM test WHERE id IN subscription.parameter('test_ids')
`
    });

    const bucketStorage = f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'a',
        description: 'Test 1'
      },
      afterReplicaId: 't1'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'b',
        description: 'Test 2'
      },
      afterReplicaId: 'earlier'
    });

    await writer.commit('0/1');

    const stream = sync.streamResponse({
      syncContext,
      bucketStorage,
      syncRules: bucketStorage.getParsedSyncRules(test_utils.PARSE_OPTIONS),
      params: {
        buckets: [],
        include_checksum: true,
        raw_data: true,
        streams: {
          include_defaults: true,
          subscriptions: [
            {
              stream: 'todos',
              parameters: { test_ids: ['a'] },
              override_priority: 0
            },
            {
              stream: 'todos',
              parameters: { test_ids: ['a', 'b'] },
              override_priority: null
            }
          ]
        }
      },
      tracker,
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
  });

  test('carries pending low-priority buckets into a new checkpoint', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: `
bucket_definitions:
  b0a:
    priority: 2
    data:
      - SELECT * FROM test WHERE substring(id, 1, 6) = 'first-';
  b0b:
    priority: 3
    data:
      - SELECT * FROM test WHERE substring(id, 1, 4) = 'low-';
  b1:
    priority: 1
    data:
      - SELECT * FROM test WHERE substring(id, 1, 8) = 'highprio';
    `
    });

    const bucketStorage = f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    const syncRulesContent = syncRules.syncConfigContent[0];
    const b0aBucket = test_utils.bucketRequest(syncRulesContent, 'b0a[]').bucket;
    const b0bBucket = test_utils.bucketRequest(syncRulesContent, 'b0b[]').bucket;
    const b1Bucket = test_utils.bucketRequest(syncRulesContent, 'b1[]').bucket;

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'highprio',
        description: 'High priority row'
      },
      afterReplicaId: 'highprio'
    });
    for (let i = 0; i < 999; i++) {
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: `first-${i}`,
          description: 'first low-priority bucket'
        },
        afterReplicaId: `first-${i}`
      });
    }
    for (let i = 0; i < 9_001; i++) {
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: `low-${i}`,
          description: 'last low-priority bucket'
        },
        afterReplicaId: `low-${i}`
      });
    }

    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
    });

    const dataLines: Array<{ bucket: string; objectIds: string[]; afterCheckpointDiff: boolean }> = [];
    const partialCheckpoints: Array<{ priority: number; afterCheckpointDiff: boolean }> = [];
    const checkpoints: any[] = [];
    const checkpointCompletes: any[] = [];
    let afterCheckpointDiff = false;
    let interruptionTriggered = false;

    for await (let next of stream) {
      if (typeof next == 'string') {
        next = JSON.parse(next);
      }
      if (typeof next === 'object' && next !== null) {
        if ('partial_checkpoint_complete' in next) {
          partialCheckpoints.push({
            priority: next.partial_checkpoint_complete.priority,
            afterCheckpointDiff
          });

          if (next.partial_checkpoint_complete.priority == 2 && !interruptionTriggered) {
            interruptionTriggered = true;
            await writer.save({
              sourceTable: testTable,
              tag: storage.SaveOperationTag.INSERT,
              after: {
                id: 'highprio2',
                description: 'Another high-priority row'
              },
              afterReplicaId: 'highprio2'
            });

            await writer.commit('0/2');
            // Let the checkpoint watcher observe the update before requesting the next priority.
            await timers.setTimeout(50);
          }
        }
        if ('checkpoint' in next || 'checkpoint_diff' in next) {
          checkpoints.push(next);
          afterCheckpointDiff = 'checkpoint_diff' in next;
        }

        if ('data' in next) {
          dataLines.push({
            bucket: next.data.bucket,
            objectIds: next.data.data.flatMap((entry: any) => (entry.object_id == null ? [] : [entry.object_id])),
            afterCheckpointDiff
          });
        }
        if ('checkpoint_complete' in next) {
          checkpointCompletes.push(next);
          break;
        }
      }
    }

    // Expected flow (data may be split into any number of chunks):
    //
    // checkpoint
    // data: b1 contains highprio
    // partial_checkpoint_complete: priority 1
    // data: b0a contains all 999 first-* rows (1,000 operations including highprio)
    // partial_checkpoint_complete: priority 2
    // ## add highprio2, interrupting before b0b starts
    // checkpoint_diff
    // data: b1 contains highprio2
    // partial_checkpoint_complete: priority 1
    // data: b0b contains all 9,001 low-* rows carried over from the interrupted checkpoint
    // checkpoint_complete: only for the new checkpoint
    expect(interruptionTriggered).toBe(true);
    expect(checkpoints).toHaveLength(2);
    expect(checkpoints[0]).toHaveProperty('checkpoint');
    expect(checkpoints[1]).toHaveProperty('checkpoint_diff');
    expect(partialCheckpoints).toEqual([
      { priority: 1, afterCheckpointDiff: false },
      { priority: 2, afterCheckpointDiff: false },
      { priority: 1, afterCheckpointDiff: true }
    ]);
    expect(checkpointCompletes).toEqual([
      {
        checkpoint_complete: {
          last_op_id: checkpoints[1].checkpoint_diff.last_op_id
        }
      }
    ]);

    const objectIdsFor = (bucket: string, afterDiff: boolean) =>
      dataLines
        .filter((line) => line.bucket == bucket && line.afterCheckpointDiff == afterDiff)
        .flatMap((line) => line.objectIds);

    expect(objectIdsFor(b1Bucket, false)).toEqual(['highprio']);
    const b0aObjectIds = objectIdsFor(b0aBucket, false);
    expect(new Set(b0aObjectIds)).toEqual(new Set(Array.from({ length: 999 }, (_, i) => `first-${i}`)));
    expect(b0aObjectIds).toHaveLength(999);
    expect(objectIdsFor(b0bBucket, false)).toEqual([]);

    expect(objectIdsFor(b1Bucket, true)).toEqual(['highprio2']);
    expect(objectIdsFor(b0aBucket, true)).toEqual([]);

    const b0bObjectIds = objectIdsFor(b0bBucket, true);
    expect(new Set(b0bObjectIds)).toEqual(new Set(Array.from({ length: 9_001 }, (_, i) => `low-${i}`)));
    expect(b0bObjectIds).toHaveLength(9_001);
  });

  test('sync interruptions with unrelated data', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: `
bucket_definitions:
  b0:
    priority: 2
    data:
      - SELECT * FROM test WHERE LENGTH(id) <= 5;
  b1:
    priority: 1
    parameters: SELECT request.user_id() as user_id
    data:
      - SELECT * FROM test WHERE LENGTH(id) > 5 AND description = bucket.user_id;
    `
    });

    const bucketStorage = f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('0/1');
    // Initial data: Add one priority row and 10k low-priority rows.
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'highprio',
        description: 'user_one'
      },
      afterReplicaId: 'highprio'
    });
    for (let i = 0; i < 10_000; i++) {
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: `${i}`,
          description: 'low prio'
        },
        afterReplicaId: `${i}`
      });
    }

    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: 'user_one', exp: Date.now() / 1000 + 100000 }),
      isEncodingAsBson: false
    });

    let sentCheckpoints = 0;
    let completedCheckpoints = 0;
    let sentRows = 0;

    // Expected flow:
    //  1. Stream starts, we receive a checkpoint followed by the one high-prio row and a partial completion.
    //  2. We insert a new row that is not part of a bucket relevant to this stream.
    //  3. This means that no interruption happens and we receive all the low-priority data, followed by a checkpoint.
    //  4. After the checkpoint, add a new row that _is_ relevant for this sync, which should trigger a new iteration.

    for await (let next of stream) {
      if (typeof next == 'string') {
        next = JSON.parse(next);
      }
      if (typeof next === 'object' && next !== null) {
        if ('partial_checkpoint_complete' in next) {
          if (sentCheckpoints == 1) {
            // Add a high-priority row that doesn't affect this sync stream.
            await writer.save({
              sourceTable: testTable,
              tag: storage.SaveOperationTag.INSERT,
              after: {
                id: 'highprio2',
                description: 'user_two'
              },
              afterReplicaId: 'highprio2'
            });

            await writer.commit('0/2');
          } else {
            expect(sentCheckpoints).toBe(2);
            expect(sentRows).toBe(10002);
          }
        }
        if ('checkpoint' in next || 'checkpoint_diff' in next) {
          sentCheckpoints += 1;
        }

        if ('data' in next) {
          sentRows += next.data.data.length;
        }
        if ('checkpoint_complete' in next) {
          completedCheckpoints++;
          if (completedCheckpoints == 2) {
            break;
          }
          if (completedCheckpoints == 1) {
            expect(sentRows).toBe(10001);

            // Add a high-priority row that affects this sync stream.
            await writer.save({
              sourceTable: testTable,
              tag: storage.SaveOperationTag.INSERT,
              after: {
                id: 'highprio3',
                description: 'user_one'
              },
              afterReplicaId: 'highprio3'
            });

            await writer.commit('0/3');
          }
        }
      }
    }

    expect(sentCheckpoints).toBe(2);
    expect(sentRows).toBe(10002);
  });

  test('restarts updated low-priority buckets at a new checkpoint', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: `
bucket_definitions:
  b0a:
    priority: 2
    data:
      - SELECT * FROM test WHERE substring(id, 1, 6) = 'first-';
  b0b:
    priority: 3
    data:
      - SELECT * FROM test WHERE substring(id, 1, 4) = 'low-';
  b1:
    priority: 1
    data:
      - SELECT * FROM test WHERE substring(id, 1, 8) = 'highprio';
    `
    });

    const bucketStorage = f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    const syncRulesContent = syncRules.syncConfigContent[0];
    const b0aBucket = test_utils.bucketRequest(syncRulesContent, 'b0a[]').bucket;
    const b0bBucket = test_utils.bucketRequest(syncRulesContent, 'b0b[]').bucket;
    const b1Bucket = test_utils.bucketRequest(syncRulesContent, 'b1[]').bucket;

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'highprio',
        description: 'High priority row'
      },
      afterReplicaId: 'highprio'
    });
    for (let i = 0; i < 999; i++) {
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: `first-${i}`,
          description: 'first low-priority bucket'
        },
        afterReplicaId: `first-${i}`
      });
    }
    for (let i = 0; i < 2_000; i++) {
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: `low-${i}`,
          description: 'low prio'
        },
        afterReplicaId: `low-${i}`
      });
    }

    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
    });

    const dataLines: Array<{ bucket: string; objectIds: string[]; afterCheckpointDiff: boolean }> = [];
    const partialCheckpoints: Array<{ priority: number; afterCheckpointDiff: boolean }> = [];
    const checkpoints: any[] = [];
    const checkpointCompletes: any[] = [];
    let afterCheckpointDiff = false;
    let interruptionTriggered = false;

    for await (let next of stream) {
      if (typeof next == 'string') {
        next = JSON.parse(next);
      }
      if (typeof next === 'object' && next !== null) {
        if ('partial_checkpoint_complete' in next) {
          partialCheckpoints.push({
            priority: next.partial_checkpoint_complete.priority,
            afterCheckpointDiff
          });

          if (next.partial_checkpoint_complete.priority == 2 && !interruptionTriggered) {
            interruptionTriggered = true;
            await writer.save({
              sourceTable: testTable,
              tag: storage.SaveOperationTag.INSERT,
              after: {
                id: 'highprio2',
                description: 'Another high-priority row'
              },
              afterReplicaId: 'highprio2'
            });

            await writer.save({
              sourceTable: testTable,
              tag: storage.SaveOperationTag.INSERT,
              after: {
                id: 'low-2000',
                description: 'Another low-priority row'
              },
              afterReplicaId: 'low-2000'
            });

            await writer.commit('0/2');
            // Let the checkpoint watcher observe the update before requesting the next priority.
            await timers.setTimeout(50);
          }
        }
        if ('checkpoint' in next || 'checkpoint_diff' in next) {
          checkpoints.push(next);
          afterCheckpointDiff = 'checkpoint_diff' in next;
        }

        if ('data' in next) {
          dataLines.push({
            bucket: next.data.bucket,
            objectIds: next.data.data.flatMap((entry: any) => (entry.object_id == null ? [] : [entry.object_id])),
            afterCheckpointDiff
          });
        }
        if ('checkpoint_complete' in next) {
          checkpointCompletes.push(next);
          break;
        }
      }
    }

    // Expected flow (data may be split into any number of chunks):
    //
    // checkpoint
    // data: b1 contains highprio
    // partial_checkpoint_complete: priority 1
    // data: b0a contains all 999 first-* rows (1,000 operations including highprio)
    // partial_checkpoint_complete: priority 2
    // ## add highprio2 and low-2000, interrupting before b0b starts
    // checkpoint_diff
    // data: b1 contains highprio2
    // partial_checkpoint_complete: priority 1
    // data: b0b contains all 2,001 low-* rows
    // checkpoint_complete: only for the new checkpoint
    expect(interruptionTriggered).toBe(true);
    expect(checkpoints).toHaveLength(2);
    expect(checkpoints[0]).toHaveProperty('checkpoint');
    expect(checkpoints[1]).toHaveProperty('checkpoint_diff');
    expect(partialCheckpoints).toEqual([
      { priority: 1, afterCheckpointDiff: false },
      { priority: 2, afterCheckpointDiff: false },
      { priority: 1, afterCheckpointDiff: true }
    ]);
    expect(checkpointCompletes).toEqual([
      {
        checkpoint_complete: {
          last_op_id: checkpoints[1].checkpoint_diff.last_op_id
        }
      }
    ]);

    const objectIdsFor = (bucket: string, afterDiff: boolean) =>
      dataLines
        .filter((line) => line.bucket == bucket && line.afterCheckpointDiff == afterDiff)
        .flatMap((line) => line.objectIds);

    expect(objectIdsFor(b1Bucket, false)).toEqual(['highprio']);
    const b0aObjectIds = objectIdsFor(b0aBucket, false);
    expect(new Set(b0aObjectIds)).toEqual(new Set(Array.from({ length: 999 }, (_, i) => `first-${i}`)));
    expect(b0aObjectIds).toHaveLength(999);
    expect(objectIdsFor(b0bBucket, false)).toEqual([]);

    expect(objectIdsFor(b1Bucket, true)).toEqual(['highprio2']);
    expect(objectIdsFor(b0aBucket, true)).toEqual([]);

    const b0bObjectIds = objectIdsFor(b0bBucket, true);
    expect(new Set(b0bObjectIds)).toEqual(new Set(Array.from({ length: 2_001 }, (_, i) => `low-${i}`)));
    expect(b0bObjectIds).toHaveLength(2_001);
  });

  test('sends checkpoint complete line for empty checkpoint', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: BASIC_SYNC_RULES
    });
    const bucketStorage = f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        description: 'sync'
      },
      afterReplicaId: 't1'
    });
    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 100000 }),
      isEncodingAsBson: false
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
            await bucketStorage.createManagedWriteCheckpoints([{ user_id: '', heads: { '1': '1/0' } }]);
            await writer.commit('1/0');
          } else {
            break;
          }
        }
      }
    }

    expect(lines).toMatchSnapshot();
  });

  test('sync legacy non-raw data', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        description: 'Test\n"string"',
        large_num: 12345678901234567890n
      },
      afterReplicaId: 't1'
    });

    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
    // Specifically check the number - this is the important part of the test
    expect(lines[1].data.data[0].data.large_num).toEqual(12345678901234567890n);
  });

  test('expired token', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);

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
      token: new JwtPayload({ sub: '', exp: 0 }),
      isEncodingAsBson: false
    });

    const lines = await consumeCheckpointLines(stream);
    expect(lines).toMatchSnapshot();
  });

  test('sync updates to global data', async (context) => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    // Activate
    await writer.markAllSnapshotDone('0/0');
    await writer.keepalive('0/0');

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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
    });
    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    expect(await getCheckpointLines(iter)).toMatchSnapshot();

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        description: 'Test 1'
      },
      afterReplicaId: 't1'
    });

    await writer.commit('0/1');

    expect(await getCheckpointLines(iter)).toMatchSnapshot();

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2',
        description: 'Test 2'
      },
      afterReplicaId: 't2'
    });

    await writer.commit('0/2');

    expect(await getCheckpointLines(iter)).toMatchSnapshot();
  });

  test('sync updates to parameter query only', async (context) => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: `bucket_definitions:
  by_user:
    parameters: select users.id as user_id from users where users.id = request.user_id()
    data:
      - select * from lists where user_id = bucket.user_id
`
    });

    const bucketStorage = await f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const usersTable = await test_utils.resolveTestTable(writer, 'users', ['id'], config, 1);

    // Activate
    await writer.markAllSnapshotDone('0/0');
    await writer.keepalive('0/0');

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
      token: new JwtPayload({ sub: 'user1', exp: Date.now() / 1000 + 100 }),
      isEncodingAsBson: false
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
    await writer.save({
      sourceTable: usersTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'user1',
        name: 'User 1'
      },
      afterReplicaId: 'user1'
    });

    await writer.commit('0/1');

    const checkpoint2 = await getCheckpointLines(iter);

    const syncRulesContent = syncRules.syncConfigContent[0];
    const { bucket } = test_utils.bucketRequest(syncRulesContent, 'by_user["user1"]');
    expect(
      (checkpoint2[0] as StreamingSyncCheckpointDiff).checkpoint_diff?.updated_buckets?.map((b) => b.bucket)
    ).toEqual([bucket]);
    expect(checkpoint2).toMatchSnapshot();
  });

  test('sync updates to data query only', async (context) => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: `bucket_definitions:
  by_user:
    parameters: select users.id as user_id from users where users.id = request.user_id()
    data:
      - select * from lists where user_id = bucket.user_id
`
    });

    const bucketStorage = await f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const usersTable = await test_utils.resolveTestTable(writer, 'users', ['id'], config, 1);
    const listsTable = await test_utils.resolveTestTable(writer, 'lists', ['id'], config, 2);

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: usersTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'user1',
        name: 'User 1'
      },
      afterReplicaId: 'user1'
    });

    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: 'user1', exp: Date.now() / 1000 + 100 }),
      isEncodingAsBson: false
    });
    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    const syncRulesContent = syncRules.syncConfigContent[0];
    const { bucket } = bucketRequest(syncRulesContent, 'by_user["user1"]');
    const checkpoint1 = await getCheckpointLines(iter);

    expect((checkpoint1[0] as StreamingSyncCheckpoint).checkpoint?.buckets?.map((b) => b.bucket)).toEqual([bucket]);
    expect(checkpoint1).toMatchSnapshot();

    await writer.save({
      sourceTable: listsTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'list1',
        user_id: 'user1',
        name: 'User 1'
      },
      afterReplicaId: 'list1'
    });

    await writer.commit('0/1');

    const checkpoint2 = await getCheckpointLines(iter);
    expect(
      (checkpoint2[0] as StreamingSyncCheckpointDiff).checkpoint_diff?.updated_buckets?.map((b) => b.bucket)
    ).toEqual([bucket]);
    expect(checkpoint2).toMatchSnapshot();
  });

  test('sync updates to parameter query + data', async (context) => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: `bucket_definitions:
  by_user:
    parameters: select users.id as user_id from users where users.id = request.user_id()
    data:
      - select * from lists where user_id = bucket.user_id
`
    });

    const bucketStorage = await f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const usersTable = await test_utils.resolveTestTable(writer, 'users', ['id'], config, 1);
    const listsTable = await test_utils.resolveTestTable(writer, 'lists', ['id'], config, 2);
    // Activate
    await writer.markAllSnapshotDone('0/0');
    await writer.keepalive('0/0');

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
      token: new JwtPayload({ sub: 'user1', exp: Date.now() / 1000 + 100 }),
      isEncodingAsBson: false
    });
    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    // Initial empty checkpoint
    expect(await getCheckpointLines(iter)).toMatchSnapshot();

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: listsTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'list1',
        user_id: 'user1',
        name: 'User 1'
      },
      afterReplicaId: 'list1'
    });

    await writer.save({
      sourceTable: usersTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'user1',
        name: 'User 1'
      },
      afterReplicaId: 'user1'
    });

    await writer.commit('0/1');

    const syncRulesContent = syncRules.syncConfigContent[0];
    const { bucket } = test_utils.bucketRequest(syncRulesContent, 'by_user["user1"]');

    const checkpoint2 = await getCheckpointLines(iter);
    expect(
      (checkpoint2[0] as StreamingSyncCheckpointDiff).checkpoint_diff?.updated_buckets?.map((b) => b.bucket)
    ).toEqual([bucket]);
    expect(checkpoint2).toMatchSnapshot();
  });

  test('expiring token', async (context) => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    // Activate
    await writer.markAllSnapshotDone('0/0');
    await writer.keepalive('0/0');

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
      token: new JwtPayload({ sub: '', exp: exp }),
      isEncodingAsBson: false
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

  test('checksum invalidation skips the candidate checkpoint', async (context) => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        description: 'Test 1'
      },
      afterReplicaId: 't1'
    });
    await writer.commit('0/1');

    let resolveInvalidatedChecksum!: () => void;
    const invalidatedChecksum = new Promise<void>((resolve) => {
      resolveInvalidatedChecksum = resolve;
    });
    const checksumSpy = vi.spyOn(bucketStorage, 'getChecksums').mockImplementationOnce(async (checkpoint, buckets) => {
      resolveInvalidatedChecksum();
      throw new storage.CheckpointChecksumInvalidatedError(checkpoint.checkpoint, buckets[0].bucket);
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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
    });

    const iter = stream[Symbol.asyncIterator]();
    context.onTestFinished(() => {
      iter.return?.();
    });

    const linesPromise = getCheckpointLines(iter, { consume: true });
    await invalidatedChecksum;

    // The first candidate was discarded before CheckpointLine.advance(). A later
    // checkpoint must therefore be calculated from the original connection state.
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2',
        description: 'Test 2'
      },
      afterReplicaId: 't2'
    });
    await writer.commit('0/2');

    const lines = await linesPromise;
    expect(checksumSpy).toHaveBeenCalledTimes(2);
    expect(lines[0]).toEqual({
      checkpoint: expect.objectContaining({
        last_op_id: '2'
      })
    });
    expect(lines).not.toContainEqual({
      checkpoint: expect.objectContaining({
        last_op_id: '1'
      })
    });
    expect(lines.at(-1)).toEqual({
      checkpoint_complete: expect.objectContaining({
        last_op_id: '2'
      })
    });
  });

  test('compacting data - invalidate checkpoint', async (context) => {
    // This tests a case of a compact operation invalidating a checkpoint in the
    // middle of syncing data.
    // This is expected to be rare in practice, but it is important to handle
    // this case correctly to maintain consistency on the client.

    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = await f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    const bucket = bucketRequest(syncRules.syncConfigContent[0], 'mybucket[]').bucket;

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1',
        description: 'Test 1'
      },
      afterReplicaId: 't1'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2',
        description: 'Test 2'
      },
      afterReplicaId: 't2'
    });

    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
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

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: {
        id: 't1',
        description: 'Test 1b'
      },
      afterReplicaId: 't1'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: {
        id: 't2',
        description: 'Test 2b'
      },
      afterReplicaId: 't2'
    });

    await writer.commit('0/2');

    await bucketStorage.compact({ compactBuckets: [bucket] });

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

  test('compacting high-priority data invalidates checkpoint before lower priorities', async (context) => {
    // Scenario:
    // 1. Bucket priorities are used.
    // 2. Client syncs a high-priority bucket.
    // 3. Due to a concurrent compaction, the checkpoint is invalidated (via target_op).
    // 4. The service picks up that invalidation, and the batch stops, without emitting a "partial_checkpoint_complete". The intention is continuing with the next checkpoint.
    // 5. However, the loop handling priorities does not see that invalidation, it only sees "bucket data for this priority is done".
    // 6. The priority loop incorrectly continues with the next priority.
    // 7. The service emits the data, as well as a final checkpoint_complete.
    // 8. The client now received a checkpoint_complete without the full data for that checkpoint. It would pick up the missing data in the checksum check, requiring a full re-download of the affected buckets.
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: `
bucket_definitions:
  high_priority:
    priority: 1
    data:
      - SELECT * FROM test WHERE substring(id, 1, 4) = 'high';
  low_priority:
    priority: 2
    data:
      - SELECT * FROM test WHERE id = 'low';
    `
    });

    const bucketStorage = await f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    const highPriorityBucket = bucketRequest(syncRules.syncConfigContent[0], 'high_priority[]').bucket;

    await writer.markAllSnapshotDone('0/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'high1',
        description: 'High priority 1'
      },
      afterReplicaId: 'high1'
    });
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'high2',
        description: 'High priority 2'
      },
      afterReplicaId: 'high2'
    });
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 'low',
        description: 'Low priority'
      },
      afterReplicaId: 'low'
    });
    await writer.commit('0/1');

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
      token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
    });

    const iter = stream[Symbol.asyncIterator]();
    await using _ = {
      async [Symbol.asyncDispose]() {
        await iter.return?.();
      }
    };

    const checkpoint = await consumeIterator(iter, {
      consume: false,
      isDone: (line) => (line as any)?.checkpoint != null
    });
    expect(checkpoint[0]).toEqual({
      checkpoint: expect.objectContaining({
        last_op_id: '3'
      })
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: {
        id: 'high1',
        description: 'Updated high priority 1'
      },
      afterReplicaId: 'high1'
    });
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.UPDATE,
      after: {
        id: 'high2',
        description: 'Updated high priority 2'
      },
      afterReplicaId: 'high2'
    });
    await writer.commit('0/2');

    await bucketStorage.compact({
      // Explicitly compact the high-priority bucket: V3 schedules background
      // compaction, while this test needs compaction at this exact point.
      compactBuckets: [highPriorityBucket]
    });

    const lines = await getCheckpointLines(iter, { consume: true });
    const nextCheckpointIndex = lines.findIndex((line) => (line as any)?.checkpoint_diff != null);
    expect(nextCheckpointIndex).toBeGreaterThan(0);

    const invalidatedCheckpointLines = lines.slice(0, nextCheckpointIndex);
    expect(invalidatedCheckpointLines).not.toContainEqual(
      expect.objectContaining({ checkpoint_complete: expect.anything() })
    );
    expect(invalidatedCheckpointLines).not.toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          bucket: expect.stringContaining('low_priority')
        })
      })
    );

    expect(lines[nextCheckpointIndex]).toEqual({
      checkpoint_diff: expect.objectContaining({
        last_op_id: '5'
      })
    });
    expect(lines.at(-1)).toEqual({
      checkpoint_complete: expect.objectContaining({
        last_op_id: '5'
      })
    });
  });

  test('write checkpoint', async () => {
    await using f = await factory();

    const syncRules = await updateSyncRules(f, {
      content: BASIC_SYNC_RULES
    });

    const bucketStorage = f.getInstance(syncRules);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

    await writer.markAllSnapshotDone('0/1');
    // <= the managed write checkpoint LSN below
    await writer.commit('0/1');

    const checkpoint = (
      await bucketStorage.createManagedWriteCheckpoints([
        {
          user_id: 'test',
          heads: { '1': '1/0' }
        }
      ])
    ).writeCheckpoints.get('test')!;

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
      token: new JwtPayload({ sub: 'test', exp: Date.now() / 1000 + 10 }),
      isEncodingAsBson: false
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

    await writer.markAllSnapshotDone('0/1');
    // must be >= the managed write checkpoint LSN
    await writer.commit('1/0');

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

  test('encodes sync rules id in buckets for streams', async () => {
    await using f = await factory();
    // This test relies making an actual update to sync rules to test the different bucket names.
    // The actual naming scheme may change, as long as the two buckets have different names.
    const rules = [
      `
streams:
  test:
    auto_subscribe: true
    query: SELECT * FROM test;

config:
  edition: 2
`,
      `
streams:
  test2:
    auto_subscribe: true
    query: SELECT * FROM test WHERE 1;

config:
  edition: 2
`
    ];

    for (let i = 0; i < 2; i++) {
      const syncRules = await updateSyncRules(f, {
        content: rules[i]
      });
      const bucketStorage = f.getInstance(syncRules);
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

      const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config, i + 1);

      await writer.markAllSnapshotDone('0/1');
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          description: 'Test 1'
        },
        afterReplicaId: 't1'
      });
      await writer.commit('0/1');

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
        token: new JwtPayload({ sub: '', exp: Date.now() / 1000 + 10 }),
        isEncodingAsBson: false
      });

      const lines = await consumeCheckpointLines(stream);
      expect(lines).toMatchSnapshot();
    }
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
      // iter.throw here would result in an uncaught error
      iter.return?.(e);
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
