import { MongoLSN } from '@module/common/MongoLSN.js';
import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { createMongoReplicationStreamBatch } from '@module/replication/MongoReplicationStream.js';
import type { MongoReplicationStreamBatch } from '@module/replication/MongoReplicationStreamTypes.js';
import { MongoSnapshotter } from '@module/replication/MongoSnapshotter.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import {
  MongoBucketStorageOptions,
  MongoSyncBucketStorage,
  OperationBatch,
  test_utils
} from '@powersync/service-module-mongodb-storage';
import { BSON } from 'bson';
import { performance } from 'node:perf_hooks';
import { describe, expect, test, vi } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { env } from './env.js';

type ObjectStorage = NonNullable<MongoBucketStorageOptions['objectStorage']>;
type StoredObject = Awaited<ReturnType<ObjectStorage['get']>>;

// Keep the real storage packing, upload-marker, publication and read paths. Only remote object I/O
// is in memory, so tests can block or fail it deterministically and retain it across restarts.
class MemoryObjects implements ObjectStorage {
  readonly data = new Map<string, StoredObject>();

  async put(...[path, data, metadata, options]: Parameters<ObjectStorage['put']>) {
    options?.signal?.throwIfAborted();
    this.data.set(path, { data, metadata });
  }

  async get(...[path, options]: Parameters<ObjectStorage['get']>) {
    options?.signal?.throwIfAborted();
    const object = this.data.get(path);
    if (object == null) {
      throw new Error(`Object not found: ${path}`);
    }
    return object;
  }

  async *list(...[prefix, options]: Parameters<ObjectStorage['list']>) {
    for (const path of this.data.keys()) {
      options?.signal?.throwIfAborted();
      if (path.startsWith(prefix)) {
        yield path;
      }
    }
  }

  async delete(...[paths, options]: Parameters<ObjectStorage['delete']>) {
    options?.signal?.throwIfAborted();
    for (const path of paths) {
      this.data.delete(path);
    }
  }

  async deletePrefix(...[prefix, options]: Parameters<ObjectStorage['deletePrefix']>) {
    const paths = [];
    for await (const path of this.list(prefix, options)) {
      paths.push(path);
    }
    await this.delete(paths, options);
    return { objectCount: paths.length };
  }
}

// Deadlines diagnose a missing synchronization event; ordering assertions use the gates themselves.
async function waitFor<T>(promise: Promise<T>, description: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timed out waiting for ${description}`)), 10000);
    timer.unref();
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer!);
  }
}

const CONFIG = `
config:
  edition: 3
streams:
  global:
    query: SELECT _id AS id, description FROM documents
`;

function fixture() {
  const objects = new MemoryObjects();
  const { factory } = test_utils.mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    supportsMultipleSyncConfigs: true,
    inlineThresholdBytes: 0,
    objectStorage: objects
  });
  return { objects, factory };
}

function lease(context: Awaited<ReturnType<typeof openChangeStreamTestContext>>) {
  return (context.storage! as MongoSyncBucketStorage).replicationStream.current_lock!;
}

async function insertRows(context: Awaited<ReturnType<typeof openChangeStreamTestContext>>) {
  await context.db
    .collection<{ _id: number; description: string }>('documents')
    .insertMany([1, 2, 3, 4].map((_id) => ({ _id, description: `row ${_id}` })));
}

async function expectRecoveredRows(context: Awaited<ReturnType<typeof openChangeStreamTestContext>>) {
  context.startStreaming();
  const ops = await context.getBucketData('global|0[]');
  const latest = new Map<string, string>();
  for (const op of ops) {
    if (op.op == 'PUT') {
      latest.set(op.object_id!, JSON.parse(op.data!).description);
    } else if (op.op == 'REMOVE') {
      latest.delete(op.object_id!);
    }
  }
  expect([...latest.entries()].sort()).toEqual([1, 2, 3, 4].map((id) => [String(id), `row ${id}`]));
}

describe.skipIf(!env.TEST_MONGO_STORAGE || DATABASE_TYPE == DatabaseType.DOCUMENTDB)(
  'v4 offloaded replication progress',
  { timeout: 30_000 },
  () => {
    test.each(['shutdown', 'upload failure', 'publication failure'] as const)(
      'streaming %s drains or replays the pending prefix',
      async (scenario) => {
        const { objects, factory } = fixture();
        {
          await using context = await openChangeStreamTestContext(factory, { storageVersion: 4 });
          await context.updateSyncRules(CONFIG);
          await context.db.createCollection('documents');
          await context.replicateSnapshot();
          await context.getCheckpoint();
          await context.stop();
          await insertRows(context);
        }
        {
          const entered = Promise.withResolvers<void>();
          const release = Promise.withResolvers<void>();
          const handled = Promise.withResolvers<void>();
          const failure = new Error(`injected ${scenario}`);
          let page = 0;
          let finalLsn: string | undefined;
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
                    yield createMongoReplicationStreamBatch({
                      events: [change],
                      resolveResumeToken: () => change.event._id
                    });
                    if (++page == 2) {
                      finalLsn = MongoLSN.fromResumeToken(change.event._id).comparable;
                      handled.resolve();
                      context.abort();
                      return;
                    }
                  }
                }
              })()
          };
          await using context = await openChangeStreamTestContext(factory, {
            storageVersion: 4,
            doNotClear: true,
            streamOptions: { streamingProgressIntervalMs: 60000, createReplicationQueryProvider: () => provider }
          });
          await context.loadActiveSyncRules();
          const before = await context.storage!.getStatus();
          const checkpointBefore = await context.storage!.getCheckpoint();
          // Exercise the automatic pipeline with small real source pages.
          using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
          using _clock = vi.spyOn(performance, 'now').mockReturnValue(0);
          const put = objects.put.bind(objects);
          using uploads = vi.spyOn(objects, 'put').mockImplementation(async (...args) => {
            entered.resolve();
            await release.promise;
            if (scenario == 'upload failure') {
              throw failure;
            }
            return put(...args);
          });
          const saved: string[] = [];
          context.storage!.registerListener({
            batchStarted: (writer) => {
              const setResumeLsn = writer.setResumeLsn.bind(writer);
              vi.spyOn(writer, 'setResumeLsn').mockImplementation(async (lsn) => {
                await setResumeLsn(lsn);
                saved.push(lsn);
              });
            }
          });
          const running = context.streamer.streamChangesInternal().catch((error: unknown) => error);
          let settled = false;
          void running.then(() => {
            settled = true;
          });
          try {
            await waitFor(entered.promise, 'entered');
            await waitFor(handled.promise, 'handled');
            const pipeline = lease(context).replicationPipeline!;
            expect(pipeline.pendingCount).toBe(2);
            expect(saved).toEqual([]);
            expect((await context.storage!.getStatus()).resumeLsn).toBe(before.resumeLsn);
            expect(settled).toBe(false);
            if (scenario == 'publication failure') {
              const session = pipeline.publicationSession;
              const transaction = session.withTransaction.bind(session);
              vi.spyOn(session, 'withTransaction').mockImplementationOnce(async (callback, options) => {
                return transaction(async (activeSession) => {
                  await callback(activeSession);
                  // Fail after real writes, so the test also proves the publication is rolled back.
                  throw failure;
                }, options);
              });
            }
          } finally {
            release.resolve();
            context.abort();
            await running;
          }
          const error = await running;
          expect(uploads).toHaveBeenCalled();
          const pipeline = lease(context).replicationPipeline!;
          expect(pipeline.pendingCount).toBe(0);
          expect(pipeline.accountedBytes).toBe(0);
          expect(pipeline.overlayEntryCount).toBe(0);
          expect((await context.storage!.getCheckpoint()).lsn).toBe(checkpointBefore.lsn);
          if (scenario == 'shutdown') {
            expect(error).toBeInstanceOf(ReplicationAbortedError);
            expect(saved).toEqual([finalLsn]);
            expect((await context.storage!.getStatus()).resumeLsn).toBe(finalLsn);
            expect(objects.data.size).toBe(2);
          } else {
            expect(error).toBeInstanceOf(ReplicationAbortedError);
            expect(saved).toEqual([]);
            expect((await context.storage!.getStatus()).resumeLsn).toBe(before.resumeLsn);
            expect(lease(context).signal.aborted).toBe(true);
            expect(lease(context).signal.reason).toMatchObject({ cause: failure });
          }
        }
        // Reopen both source and storage. Reads hydrate the actual offloaded payloads, including
        // rows flushed before a shutdown and rows replayed after a failed publication.
        await using resumed = await openChangeStreamTestContext(factory, { storageVersion: 4, doNotClear: true });
        await resumed.loadActiveSyncRules();
        using reads = vi.spyOn(objects, 'get');
        await expectRecoveredRows(resumed);
        expect(reads).toHaveBeenCalled();
      }
    );

    test.each(['shutdown', 'upload failure', 'publication failure'] as const)(
      'snapshot %s preserves a durable cursor and recovers all offloaded rows',
      async (scenario) => {
        const { objects, factory } = fixture();
        let durableCount = 0;
        {
          await using context = await openChangeStreamTestContext(factory, { storageVersion: 4 });
          await context.updateSyncRules(CONFIG);
          await insertRows(context);
          const abort = new AbortController();
          const snapshotter = new MongoSnapshotter({
            storage: context.storage!,
            connections: context.connectionManager,
            metrics: METRICS_HELPER.metricsEngine,
            abortSignal: abort.signal,
            checkpointStreamId: new mongo.ObjectId(),
            snapshotChunkLength: 1,
            snapshotProgressIntervalMs: 60000
          });
          let now = 0;
          using _clock = vi.spyOn(performance, 'now').mockImplementation(() => now);
          await snapshotter.setupCheckpointsCollection();
          await snapshotter.queueSnapshotTables(null);
          let rows = 0;
          let failPublication = false;
          let progressTable: storage.SourceTable | undefined;
          const failure = new Error(`injected ${scenario}`);
          const create = context.storage!.createWriter.bind(context.storage!);
          using _create = vi.spyOn(context.storage!, 'createWriter').mockImplementation(async (...args) => {
            const writer = await create(...args);
            const save = writer.save.bind(writer);
            vi.spyOn(writer, 'save').mockImplementation(async (record) => {
              const result = await save(record);
              rows++;
              if (scenario == 'shutdown') {
                if (rows == 2) {
                  abort.abort();
                }
              } else {
                // Persist one page, then fail the next durable boundary using a controlled clock.
                now += 60000;
              }
              return result;
            });
            const update = writer.updateTableProgress.bind(writer);
            vi.spyOn(writer, 'updateTableProgress').mockImplementation(async (...args) => {
              const result = await update(...args);
              if (args[1].replicatedCount != null) {
                durableCount = args[1].replicatedCount;
                progressTable = result;
              }
              return result;
            });
            return writer;
          });
          const put = objects.put.bind(objects);
          let uploads = 0;
          using _uploads = vi.spyOn(objects, 'put').mockImplementation(async (...args) => {
            if (++uploads == 2) {
              if (scenario == 'upload failure') {
                throw failure;
              }
              failPublication = scenario == 'publication failure';
            }
            return put(...args);
          });
          const session = lease(context).session;
          const transaction = session.withTransaction.bind(session);
          using _transaction = vi.spyOn(session, 'withTransaction').mockImplementation(async (callback, options) => {
            return transaction(async (activeSession) => {
              const result = await callback(activeSession);
              if (failPublication) {
                throw failure;
              }
              return result;
            }, options);
          });
          await expect(snapshotter.replicationLoop()).rejects.toThrow(
            scenario == 'shutdown' ? 'Aborted initial replication' : `injected ${scenario}`
          );
          expect(rows).toBe(2);
          expect(durableCount).toBe(scenario == 'shutdown' ? 2 : 1);
          expect(BSON.deserialize(progressTable!.snapshotStatus!.lastKey!)._id).toBe(durableCount);
          expect(progressTable!.snapshotComplete).toBe(false);
          expect((await context.storage!.getStatus()).snapshotDone).toBe(false);
          expect((await context.storage!.getCheckpoint()).lsn).toBeNull();
          expect(uploads).toBeGreaterThan(0);
        }
        await using resumed = await openChangeStreamTestContext(factory, { storageVersion: 4, doNotClear: true });
        await resumed.loadNextSyncRules();
        const snapshotRows: string[] = [];
        resumed.storage!.registerListener({
          batchStarted: (writer) => {
            if (!writer.skipExistingRows) {
              return;
            }
            const save = writer.save.bind(writer);
            vi.spyOn(writer, 'save').mockImplementation(async (record) => {
              snapshotRows.push(String(record.after!._id));
              return save(record);
            });
          }
        });
        await resumed.replicateSnapshot();
        await expectRecoveredRows(resumed);
        expect(snapshotRows).toEqual([1, 2, 3, 4].slice(durableCount).map(String));
      }
    );

    test('concurrent snapshot and streaming writers share backpressure and publish a consistent checkpoint', async () => {
      const { objects, factory } = fixture();
      const sourceReady = Promise.withResolvers<void>();
      const continueStreaming = Promise.withResolvers<void>();
      const snapshotQueued = Promise.withResolvers<void>();
      const startSnapshot = Promise.withResolvers<void>();
      const snapshotFirstSaved = Promise.withResolvers<void>();
      const continueSnapshot = Promise.withResolvers<void>();
      const snapshotSecondAttempt = Promise.withResolvers<void>();
      const streamStarted = Promise.withResolvers<void>();
      const streamWarm = Promise.withResolvers<void>();
      const streamSecondSaved = Promise.withResolvers<void>();
      const streamThirdAttempt = Promise.withResolvers<void>();
      const uploadEntered = Promise.withResolvers<void>();
      const releaseUpload = Promise.withResolvers<void>();
      let pausedSnapshot = false;
      let holdUpload = false;
      let heldUpload = false;
      let snapshotSaves = 0;
      let streamSaves = 0;
      let secondSnapshotSaved = false;
      let thirdStreamSaved = false;
      await using context = await openChangeStreamTestContext(factory, {
        storageVersion: 4,
        streamOptions: {
          snapshotChunkLength: 1,
          snapshotProgressIntervalMs: 60000,
          streamingProgressIntervalMs: 60000,
          createReplicationQueryProvider: () => ({
            ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
            openChangeStream: ({ open }) =>
              (async function* (): AsyncGenerator<MongoReplicationStreamBatch> {
                let documents = 0;
                for await (const source of open({})) {
                  for (const change of source.events) {
                    const document = 'ns' in change.event && change.event.ns.coll == 'documents';
                    if (document && documents == 0) {
                      // Queue all three writes before processing the first, so the source barrier
                      // requested by that first write follows the updates under test.
                      await sourceReady.promise;
                    }
                    yield createMongoReplicationStreamBatch({
                      events: [change],
                      resolveResumeToken: () => change.event._id
                    });
                    if (document && ++documents == 1) {
                      await continueStreaming.promise;
                    }
                  }
                }
              })()
          }),
          snapshotHooks: {
            beforeSnapshotStarted: async () => {
              if (!pausedSnapshot) {
                pausedSnapshot = true;
                snapshotQueued.resolve();
                await startSnapshot.promise;
              }
            }
          },
          storageHooks: {
            afterBatchFlush: async (writer) => {
              if (!writer.skipExistingRows && streamSaves > 0) {
                streamWarm.resolve();
              }
            }
          }
        }
      });
      await context.updateSyncRules(CONFIG);
      await insertRows(context);
      using _limit = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockReturnValue(true);
      context.storage!.registerListener({
        batchStarted: (writer) => {
          if (!writer.skipExistingRows) {
            const commit = writer.commit.bind(writer);
            vi.spyOn(writer, 'commit').mockImplementation(async (...args) => {
              const result = await commit(...args);
              if (streamSaves == 0) {
                streamStarted.resolve();
              }
              return result;
            });
          }
          const save = writer.save.bind(writer);
          vi.spyOn(writer, 'save').mockImplementation(async (record) => {
            if (writer.skipExistingRows) {
              if (++snapshotSaves == 2) {
                snapshotSecondAttempt.resolve();
              }
              const result = await save(record);
              if (snapshotSaves == 1) {
                snapshotFirstSaved.resolve();
                await continueSnapshot.promise;
              } else if (snapshotSaves == 2) {
                secondSnapshotSaved = true;
              }
              return result;
            }
            if (++streamSaves == 3) {
              streamThirdAttempt.resolve();
            }
            const result = await save(record);
            if (streamSaves == 2) {
              streamSecondSaved.resolve();
            } else if (streamSaves == 3) {
              thirdStreamSaved = true;
            }
            return result;
          });
        }
      });
      const put = objects.put.bind(objects);
      using _uploads = vi.spyOn(objects, 'put').mockImplementation(async (...args) => {
        if (holdUpload && !heldUpload) {
          heldUpload = true;
          uploadEntered.resolve();
          await releaseUpload.promise;
        }
        return put(...args);
      });
      const snapshot = context.replicateSnapshot().then(
        () => null,
        (error: unknown) => error
      );
      try {
        await waitFor(snapshotQueued.promise, 'snapshotQueued');
        await waitFor(streamStarted.promise, 'streamStarted');
        const collection = context.db.collection<{ _id: string; description: string }>('documents');
        await collection.insertOne({ _id: 'S', description: 'warm' });
        await collection.updateOne({ _id: 'S' }, { $set: { description: 'latest' } });
        await collection.insertOne({ _id: 'T', description: 'tail' });
        sourceReady.resolve();
        await waitFor(streamWarm.promise, 'streamWarm');
        holdUpload = true;
        startSnapshot.resolve();
        await waitFor(uploadEntered.promise, 'uploadEntered');
        await waitFor(snapshotFirstSaved.promise, 'snapshotFirstSaved');
        continueStreaming.resolve();
        await waitFor(streamSecondSaved.promise, 'streamSecondSaved');
        continueSnapshot.resolve();
        await waitFor(snapshotSecondAttempt.promise, 'snapshotSecondAttempt');
        await waitFor(streamThirdAttempt.promise, 'streamThirdAttempt');
        const pipeline = lease(context).replicationPipeline!;
        expect(pipeline.pendingCount).toBe(2);
        expect(pipeline.accountedBytes).toBeGreaterThan(0);
        expect(pipeline.accountedBytes).toBeLessThan(64 * 1024 * 1024);
        expect(secondSnapshotSaved).toBe(false);
        expect(thirdStreamSaved).toBe(false);
        expect((await context.storage!.getStatus()).snapshotDone).toBe(false);
        expect((await context.storage!.getCheckpoint()).lsn).toBeNull();
      } finally {
        releaseUpload.resolve();
        sourceReady.resolve();
        continueStreaming.resolve();
        startSnapshot.resolve();
        continueSnapshot.resolve();
      }
      const snapshotError = await snapshot;
      if (snapshotError != null) {
        throw snapshotError;
      }
      const ops = await context.getBucketData('global|0[]');
      const latest = new Map<string, string>();
      for (const op of ops) {
        if (op.op == 'PUT') {
          latest.set(op.object_id!, JSON.parse(op.data!).description);
        }
      }
      expect([...latest.entries()].sort()).toEqual([
        ...[1, 2, 3, 4].map((id) => [String(id), `row ${id}`]),
        ['S', 'latest'],
        ['T', 'tail']
      ]);
      await context.stop();
      const pipeline = lease(context).replicationPipeline!;
      expect(pipeline.pendingCount).toBe(0);
      expect(pipeline.accountedBytes).toBe(0);
      expect(pipeline.overlayEntryCount).toBe(0);
    });
  }
);
