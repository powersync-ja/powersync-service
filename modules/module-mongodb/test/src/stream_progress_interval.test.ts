import { MongoLSN } from '@module/common/MongoLSN.js';
import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { createMongoReplicationStreamBatch } from '@module/replication/MongoReplicationStream.js';
import type { MongoReplicationStreamBatch } from '@module/replication/MongoReplicationStreamTypes.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import type { TestStorageFactory } from '@powersync/service-core';
import { performance } from 'node:perf_hooks';
import { describe, expect, test, vi } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { env } from './env.js';
import { describeWithStorage, INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

const CONFIG = `
config:
  edition: 3
streams:
  global:
    query: SELECT _id AS id FROM documents
`;

// Queue real source changes after completing a snapshot, so each test can inspect streaming alone.
async function prepare(factory: TestStorageFactory = INITIALIZED_MONGO_STORAGE_FACTORY.factory, storageVersion = 4) {
  await using context = await openChangeStreamTestContext(factory, { storageVersion });
  await context.updateSyncRules(CONFIG);
  await context.db.createCollection('documents');
  await context.replicateSnapshot();
  await context.getCheckpoint();
  await context.stop();
  await context.db.collection<{ _id: number }>('documents').insertMany([1, 2, 3, 4].map((_id) => ({ _id })));
}

describe.skipIf(!env.TEST_MONGO_STORAGE || DATABASE_TYPE == DatabaseType.DOCUMENTDB)(
  'streaming progress interval',
  () => {
    test.each([
      { interval: undefined, times: [0, 9999, 10000, 10001], pages: [3, 4] },
      { interval: 50, times: [0, 49, 50, 51], pages: [3, 4] },
      { interval: 0, times: [0, 0, 0, 0], pages: [1, 2, 3, 4] },
      { interval: 60000, times: [0, 1, 2, 3], pages: [4] }
    ])('persists retained and filtered pages at interval $interval', async ({ interval, times, pages }) => {
      await prepare();
      let now = 0;
      using _clock = vi.spyOn(performance, 'now').mockImplementation(() => now);
      const tokens: string[] = [];
      const saved: string[] = [];
      const order: string[] = [];
      const provider = {
        ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
        openChangeStream: ({ open }: Parameters<typeof DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER.openChangeStream>[0]) =>
          (async function* (): AsyncGenerator<MongoReplicationStreamBatch> {
            for await (const source of open({})) {
              for (const change of source.events) {
                if (!('ns' in change.event) || change.event.ns.coll != 'documents') {
                  continue;
                }
                tokens.push(MongoLSN.fromResumeToken(change.event._id).comparable);
                now = times[tokens.length - 1];
                // Alternate retained and excluded events. Keep each real event's exact recovery token
                // while controlling page boundaries independently of MongoDB's adaptive transport size.
                const filtered = tokens.length % 2 == 0;
                yield createMongoReplicationStreamBatch({
                  events: filtered ? [] : [change],
                  resolveResumeToken: () => change.event._id,
                  resolveMetadata: () => ({ filteredCount: filtered ? 1 : 0 })
                });
                if (tokens.length == 4) {
                  context.abort();
                  return;
                }
              }
            }
          })()
      };
      await using context = await openChangeStreamTestContext(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
        storageVersion: 4,
        doNotClear: true,
        streamOptions: { streamingProgressIntervalMs: interval, createReplicationQueryProvider: () => provider }
      });
      await context.loadActiveSyncRules();
      const checkpointBefore = await context.storage!.getCheckpoint();
      context.storage!.registerListener({
        batchStarted: (batch) => {
          const flush = batch.flush.bind(batch);
          vi.spyOn(batch, 'flush').mockImplementation(async (...args) => {
            const result = await flush(...args);
            order.push('flush');
            return result;
          });
          const setResumeLsn = batch.setResumeLsn.bind(batch);
          vi.spyOn(batch, 'setResumeLsn').mockImplementation(async (lsn) => {
            expect(order.at(-1)).toBe('flush');
            await setResumeLsn(lsn);
            order.push('resume');
            saved.push(lsn);
          });
        }
      });
      await expect(context.streamer.streamChangesInternal()).rejects.toBeInstanceOf(ReplicationAbortedError);
      expect(saved).toEqual(pages.map((page) => tokens[page - 1]));
      expect((await context.storage!.getStatus()).resumeLsn).toBe(tokens[3]);
      // Restart-only updates must leave client checkpoints behind the unread startup barrier.
      expect((await context.storage!.getCheckpoint()).lsn).toBe(checkpointBefore.lsn);
    });

    test.each(['partial page', 'unsafe token', 'adapter failure', 'flush failure'] as const)(
      'shutdown and failure preserve safe recovery after %s',
      async (scenario) => {
        await prepare();
        const tokens: string[] = [];
        const saved: string[] = [];
        let failFlush = false;
        const abortCause = new Error('cancel test reader');
        const provider = {
          ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
          openChangeStream: ({
            open
          }: Parameters<typeof DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER.openChangeStream>[0]) =>
            (async function* (): AsyncGenerator<MongoReplicationStreamBatch> {
              for await (const source of open({})) {
                for (const change of source.events) {
                  if (!('ns' in change.event) || change.event.ns.coll != 'documents') {
                    continue;
                  }
                  tokens.push(MongoLSN.fromResumeToken(change.event._id).comparable);
                  yield createMongoReplicationStreamBatch({
                    events: (function* () {
                      yield change;
                      if (tokens.length == 2 && scenario == 'partial page') {
                        context.abort(abortCause);
                      }
                    })(),
                    resolveResumeToken: () => {
                      if (tokens.length == 2 && scenario == 'adapter failure') {
                        throw new Error('injected adapter failure');
                      }
                      return tokens.length == 2 && scenario == 'unsafe token' ? undefined : change.event._id;
                    }
                  });
                  if (tokens.length == 2) {
                    failFlush = scenario == 'flush failure';
                    context.abort(abortCause);
                    // Model the reader throwing its cancellation reason while fetching another page.
                    throw abortCause;
                  }
                }
              }
            })()
        };
        await using context = await openChangeStreamTestContext(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
          storageVersion: 4,
          doNotClear: true,
          streamOptions: { streamingProgressIntervalMs: 60000, createReplicationQueryProvider: () => provider }
        });
        await context.loadActiveSyncRules();
        const originalLsn = (await context.storage!.getStatus()).resumeLsn;
        context.storage!.registerListener({
          batchStarted: (batch) => {
            const flush = batch.flush.bind(batch);
            vi.spyOn(batch, 'flush').mockImplementation(async (...args) => {
              if (failFlush) {
                throw new Error('injected flush failure');
              }
              return flush(...args);
            });
            const setResumeLsn = batch.setResumeLsn.bind(batch);
            vi.spyOn(batch, 'setResumeLsn').mockImplementation(async (lsn) => {
              await setResumeLsn(lsn);
              saved.push(lsn);
            });
          }
        });
        await expect(context.streamer.streamChangesInternal()).rejects.toThrow(
          scenario.includes('failure') ? `injected ${scenario}` : 'Replication stream aborted'
        );
        if (scenario.includes('failure')) {
          expect(saved).toEqual([]);
          expect((await context.storage!.getStatus()).resumeLsn).toBe(originalLsn);
        } else {
          expect(saved).toEqual([tokens[0]]);
          expect((await context.storage!.getStatus()).resumeLsn).toBe(tokens[0]);
        }
      }
    );

    test.each([0, 60000])('checkpoint commits stay immediate at interval %i', async (interval) => {
      await prepare();
      const saved: string[] = [];
      const committed: string[] = [];
      let expectedCommit: string | undefined;
      const provider = {
        ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
        openChangeStream: ({ open }: Parameters<typeof DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER.openChangeStream>[0]) =>
          (async function* (): AsyncGenerator<MongoReplicationStreamBatch> {
            for await (const source of open({})) {
              for (const change of source.events) {
                yield createMongoReplicationStreamBatch({
                  events: [change],
                  resolveResumeToken: () => change.event._id
                });
                if (committed.length > 0) {
                  expectedCommit = MongoLSN.fromResumeToken(change.event._id).comparable;
                  context.abort();
                  return;
                }
              }
            }
          })()
      };
      await using context = await openChangeStreamTestContext(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
        storageVersion: 4,
        doNotClear: true,
        streamOptions: { streamingProgressIntervalMs: interval, createReplicationQueryProvider: () => provider }
      });
      await context.loadActiveSyncRules();
      context.storage!.registerListener({
        batchStarted: (batch) => {
          const commit = batch.commit.bind(batch);
          vi.spyOn(batch, 'commit').mockImplementation(async (lsn, ...args) => {
            const result = await commit(lsn, ...args);
            committed.push(lsn);
            return result;
          });
          const setResumeLsn = batch.setResumeLsn.bind(batch);
          vi.spyOn(batch, 'setResumeLsn').mockImplementation(async (lsn) => {
            await setResumeLsn(lsn);
            saved.push(lsn);
          });
        }
      });
      await expect(context.streamer.streamChangesInternal()).rejects.toBeInstanceOf(ReplicationAbortedError);
      expect(committed).toEqual([expectedCommit]);
      expect(saved).not.toContain(expectedCommit);
      if (interval > 0) {
        // The commit supersedes pending pages, so shutdown needs no restart-only update either.
        expect(saved).toEqual([]);
      }
      expect((await context.storage!.getStatus()).resumeLsn).toBe(expectedCommit);
    });

    describeWithStorage({ timeout: 30_000 }, ({ factory, storageVersion }) => {
      test('blocked checkpoints retain a restart-only update for shutdown', async () => {
        await prepare(factory, storageVersion);
        let blockedLsn: string | undefined;
        const saved: string[] = [];
        const provider = {
          ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
          openChangeStream: ({
            open
          }: Parameters<typeof DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER.openChangeStream>[0]) =>
            (async function* (): AsyncGenerator<MongoReplicationStreamBatch> {
              for await (const source of open({})) {
                for (const change of source.events) {
                  yield createMongoReplicationStreamBatch({
                    events: [change],
                    resolveResumeToken: () => change.event._id
                  });
                  if (blockedLsn != null) {
                    context.abort();
                    return;
                  }
                }
              }
            })()
        };
        await using context = await openChangeStreamTestContext(factory, {
          storageVersion,
          doNotClear: true,
          streamOptions: { streamingProgressIntervalMs: 60000, createReplicationQueryProvider: () => provider }
        });
        await context.loadActiveSyncRules();
        // Keep checkpoint publication blocked beyond every event in this test, using real storage metadata.
        {
          await using writer = await context.storage!.createWriter({
            zeroLSN: MongoLSN.ZERO.comparable,
            defaultSchema: context.db.databaseName,
            storeCurrentData: false
          });
          const future = new MongoLSN({
            timestamp: new mongo.Timestamp({ t: Math.floor(Date.now() / 1000) + 3600, i: 0 })
          }).comparable;
          await writer.markSnapshotDone(future);
        }
        const checkpointBefore = await context.storage!.getCheckpoint();
        context.storage!.registerListener({
          batchStarted: (batch) => {
            const commit = batch.commit.bind(batch);
            vi.spyOn(batch, 'commit').mockImplementation(async (lsn, ...args) => {
              const result = await commit(lsn, ...args);
              expect(result.checkpointBlocked).toBe(true);
              expect(result.checkpointCreated).toBe(false);
              blockedLsn = lsn;
              return result;
            });
            const setResumeLsn = batch.setResumeLsn.bind(batch);
            vi.spyOn(batch, 'setResumeLsn').mockImplementation(async (lsn) => {
              await setResumeLsn(lsn);
              saved.push(lsn);
            });
          }
        });
        await expect(context.streamer.streamChangesInternal()).rejects.toBeInstanceOf(ReplicationAbortedError);
        expect(blockedLsn).toBeDefined();
        expect(saved).toEqual([blockedLsn]);
        expect((await context.storage!.getStatus()).resumeLsn).toBe(blockedLsn);
        expect((await context.storage!.getCheckpoint()).lsn).toBe(checkpointBefore.lsn);
      });
    });
  }
);
