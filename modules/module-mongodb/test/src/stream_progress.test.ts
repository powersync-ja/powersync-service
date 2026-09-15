import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { MongoReplicationStreamItem } from '@module/replication/MongoReplicationStream.js';
import { describe, expect, test, vi } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { describeWithStorage } from './util.js';

describe.skipIf(DATABASE_TYPE == DatabaseType.DOCUMENTDB)('MongoDB durable adapter progress', () => {
  describeWithStorage({ timeout: 30_000 }, ({ factory, storageVersion }) => {
    test('does not acknowledge retained rows when flushing a later filtered boundary fails', async () => {
      let originalLsn: string;
      {
        await using context = await openChangeStreamTestContext(factory, { storageVersion });
        await context.updateSyncRules(/* yaml */ ` bucket_definitions:
            global:
              data:
                - SELECT _id AS id FROM documents WHERE _id = 1 `);
        await context.db.createCollection('documents');
        await context.replicateSnapshot();
        await context.getCheckpoint();
        await context.stop();
        originalLsn = (await context.storage!.getStatus()).resumeLsn!;
        await context.db
          .collection<{ _id: number }>('documents')
          .insertMany(Array.from({ length: 20 }, (_, i) => ({ _id: i + 1 })));
      }

      let failFlush = false;
      const provider = {
        ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
        openChangeStream: ({ open }: Parameters<typeof DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER.openChangeStream>[0]) =>
          (async function* (): AsyncGenerator<MongoReplicationStreamItem> {
            let retained = false;
            let filtered = 0;
            for await (const item of open({})) {
              if (item.type == 'change' && 'documentKey' in item.event && item.event.ns.coll == 'documents') {
                if (String(item.event.documentKey._id) != '1') {
                  filtered++;
                  continue;
                }
                retained = true;
              } else if (item.type == 'progress') {
                // Carry the first retained row into the next boundary, after an excluded suffix.
                // This exercises flush-before-resume with actual pending writes, not an empty writer.
                if (retained && filtered == 0) continue;
                yield { ...item, filteredCount: filtered };
                retained = false;
                filtered = 0;
                continue;
              }
              yield item;
            }
          })()
      };
      {
        const save = vi.fn();
        const setResumeLsn = vi.fn();
        await using context = await openChangeStreamTestContext(factory, {
          storageVersion,
          doNotClear: true,
          streamOptions: {
            snapshotChunkLength: 5,
            createReplicationQueryProvider: () => provider,
            storageHooks: {
              beforeBatchFlush: async () => {
                if (failFlush) throw new Error('injected retained-row flush failure');
              }
            }
          }
        });
        await context.loadActiveSyncRules();
        context.storage!.registerListener({
          batchStarted: (batch) => {
            const originalSave = batch.save.bind(batch);
            vi.spyOn(batch, 'save').mockImplementation((...args) => {
              save();
              failFlush = true;
              return originalSave(...args);
            });
            const originalSetResumeLsn = batch.setResumeLsn.bind(batch);
            vi.spyOn(batch, 'setResumeLsn').mockImplementation((...args) => {
              setResumeLsn();
              return originalSetResumeLsn(...args);
            });
          }
        });
        const result = await context.startStreaming();
        expect(result).toMatchObject({ status: 'rejected', reason: expect.any(Error) });
        if (result.status == 'rejected') expect(String(result.reason)).toContain('retained-row flush failure');
        expect(save).toHaveBeenCalledOnce();
        expect(setResumeLsn).not.toHaveBeenCalled();
        expect((await context.storage!.getStatus()).resumeLsn).toBe(originalLsn);
      }
      // Reopen from the last durable token with the same selection. The retained row must be replayed.
      await using context = await openChangeStreamTestContext(factory, {
        storageVersion,
        doNotClear: true,
        streamOptions: { createReplicationQueryProvider: () => provider }
      });
      await context.loadActiveSyncRules();
      context.startStreaming();
      expect(
        (await context.getBucketData('global[]')).filter((op) => op.op == 'PUT').map((op) => op.object_id)
      ).toEqual(['1']);
    });

    test('resumes ordinary changes from a token inside a transaction', async () => {
      {
        await using context = await openChangeStreamTestContext(factory, { storageVersion });
        await context.updateSyncRules(
          'bucket_definitions:\n  global:\n    data:\n      - SELECT _id AS id FROM documents'
        );
        await context.db.createCollection('documents');
        await context.replicateSnapshot();
        await context.getCheckpoint();
        await context.stop();
        const session = context.client.startSession();
        await using sessionDisposer = { [Symbol.asyncDispose]: () => session.endSession() };
        await session.withTransaction(async () => {
          await context.db.collection<{ _id: string }>('documents').insertMany(
            Array.from({ length: 50 }, (_, index) => ({ _id: String(index) })),
            { session }
          );
        });
      }
      {
        let progressCount = 0;
        await using context = await openChangeStreamTestContext(factory, {
          storageVersion,
          doNotClear: true,
          streamOptions: { snapshotChunkLength: 5 }
        });
        await context.loadActiveSyncRules();
        context.storage!.registerListener({
          batchStarted: (batch) => {
            const setResumeLsn = batch.setResumeLsn.bind(batch);
            vi.spyOn(batch, 'setResumeLsn').mockImplementation(async (lsn) => {
              await setResumeLsn(lsn);
              if (++progressCount == 3) context.abort(new Error('stop inside transaction'));
            });
          }
        });
        await context.startStreaming();
        expect(progressCount).toBe(3);
      }
      await using context = await openChangeStreamTestContext(factory, { storageVersion, doNotClear: true });
      await context.loadActiveSyncRules();
      context.startStreaming();
      await context.getCheckpoint();
      const operations = await context.getBucketData('global[]');
      // Every insert in this transaction shares a clusterTime, but each has its own resume token.
      // A timestamp dedupe guard here would lose the unprocessed suffix after the third batch.
      expect(
        operations
          .filter((op) => op.op == 'PUT')
          .map((op) => op.object_id)
          .sort()
      ).toEqual(Array.from({ length: 50 }, (_, index) => String(index)).sort());
    });

    test.each(['progress', 'flush failure', 'adapter failure'] as const)(
      '%s before the pending checkpoint arrives',
      async (scenario) => {
        let originalLsn: string;
        {
          await using context = await openChangeStreamTestContext(factory, { storageVersion });
          await context.updateSyncRules(
            'bucket_definitions:\n  global:\n    data:\n      - SELECT _id AS id FROM documents'
          );
          await context.db.createCollection('documents');
          await context.replicateSnapshot();
          await context.getCheckpoint();
          await context.stop();
          originalLsn = (await context.storage!.getStatus()).resumeLsn!;
          // All changes precede the barrier which the next replication attempt creates on startup.
          await context.db
            .collection<{ _id: number }>('documents')
            .insertMany(Array.from({ length: 100 }, (_, _id) => ({ _id })));
        }

        const savedPositions: string[] = [];
        let filtered = 0;
        let beforeFailure: string | undefined;
        let failFlush = false;
        const save = vi.fn();
        const resolve = vi.fn();
        const commit = vi.fn();
        await using context = await openChangeStreamTestContext(factory, {
          storageVersion,
          doNotClear: true,
          streamOptions: {
            snapshotChunkLength: 5,
            keepaliveIntervalMs: 60_000,
            storageHooks: {
              beforeBatchFlush: async () => {
                if (failFlush) throw new Error('injected flush failure');
              }
            },
            createReplicationQueryProvider: () => ({
              ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
              openChangeStream({ open }) {
                return (async function* (): AsyncGenerator<MongoReplicationStreamItem> {
                  let count = 0;
                  for await (const item of open({})) {
                    if (item.type == 'change' && 'ns' in item.event && item.event.ns.coll == 'documents') {
                      count++;
                      continue;
                    }
                    if (item.type == 'progress' && count > 0) {
                      filtered += count;
                      beforeFailure = (await context.storage!.getStatus()).resumeLsn!;
                      if (scenario == 'adapter failure') throw new Error('injected adapter failure');
                      failFlush = scenario == 'flush failure';
                      yield { ...item, filteredCount: count };
                      count = 0;
                      // next() is requested only after this module has handled the progress item. Inspect actual
                      // persisted state here, without sleeps or waiting for a later checkpoint event.
                      savedPositions.push((await context.storage!.getStatus()).resumeLsn!);
                      if (savedPositions.length == 3) throw new Error('stopped after three safe boundaries');
                    } else {
                      yield item;
                    }
                  }
                })();
              }
            })
          }
        });
        await context.loadActiveSyncRules();
        const checkpointBefore = await context.storage!.getCheckpoint();
        context.storage!.registerListener({
          batchStarted: (batch) => {
            const originalSave = batch.save.bind(batch);
            vi.spyOn(batch, 'save').mockImplementation((...args) => {
              save();
              return originalSave(...args);
            });
            // Observe calls while retaining the real relation resolver and commit behavior.
            const originalResolve = batch.resolveTables.bind(batch);
            vi.spyOn(batch, 'resolveTables').mockImplementation((...args) => {
              resolve();
              return originalResolve(...args);
            });
            const originalCommit = batch.commit.bind(batch);
            vi.spyOn(batch, 'commit').mockImplementation((...args) => {
              commit();
              return originalCommit(...args);
            });
          }
        });
        const result = await context.startStreaming();
        expect(result.status).toBe('rejected');
        if (result.status != 'rejected') throw new Error('Expected the controlled stop');
        expect(String(result.reason)).toContain(
          scenario == 'progress' ? 'three safe boundaries' : `injected ${scenario}`
        );
        expect(filtered).toBeGreaterThan(0);
        expect(save).not.toHaveBeenCalled();
        expect(resolve).not.toHaveBeenCalled();
        expect(commit).not.toHaveBeenCalled();
        const status = await context.storage!.getStatus();
        if (scenario == 'progress') {
          expect(new Set(savedPositions).size).toBe(3);
          expect(savedPositions.every((lsn) => lsn > originalLsn)).toBe(true);
          expect(status.resumeLsn).toBe(savedPositions.at(-1));
        } else {
          expect(status.resumeLsn).toBe(beforeFailure);
        }
        // Persisting source progress is not permission to expose a checkpoint past the pending barrier.
        expect((await context.storage!.getCheckpoint()).lsn).toBe(checkpointBefore.lsn);
      }
    );
  });
});
