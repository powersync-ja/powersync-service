import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { createMongoReplicationStreamBatch } from '@module/replication/MongoReplicationStream.js';
import type { MongoReplicationStreamChange } from '@module/replication/MongoReplicationStreamTypes.js';
import { MongoSnapshotter } from '@module/replication/MongoSnapshotter.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import { SnapshotGate, storage } from '@powersync/service-core';
import { METRICS_HELPER, test_utils } from '@powersync/service-core-tests';
import { expect, test, vi } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

const CONFIG = /* yaml */ `
  # Sync config fixture.
  config:
    edition: 3
  streams:
    items:
      query: SELECT _id AS id, description FROM documents
`;

test.skipIf(!env.TEST_MONGO_STORAGE).each(['resume', 'abort'])(
  'snapshot preserves its completed page and releases storage locks while paused (%s)',
  async (action) => {
    const gate = new SnapshotGate();
    using pressure = gate.trackStreaming();
    await using context = await openChangeStreamTestContext(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
      storageVersion: 4
    });
    await context.updateSyncRules(CONFIG);
    await context.db
      .collection<{ _id: number; description: string }>('documents')
      .insertMany([1, 2, 3].map((_id) => ({ _id, description: String(_id) })));
    const abort = new AbortController();
    const snapshotter = new MongoSnapshotter({
      storage: context.storage!,
      connections: context.connectionManager,
      metrics: METRICS_HELPER.metricsEngine,
      abortSignal: abort.signal,
      checkpointStreamId: new mongo.ObjectId(),
      snapshotChunkLength: 1,
      snapshotProgressIntervalMs: 60_000,
      snapshotGate: gate
    });
    let rows = 0;
    let progress: storage.SourceTable | undefined;
    const entered = Promise.withResolvers<void>();
    const wait = gate.wait.bind(gate);
    using _wait = vi.spyOn(gate, 'wait').mockImplementation((signal) => {
      entered.resolve();
      return wait(signal);
    });
    const create = context.storage!.createWriter.bind(context.storage!);
    using _create = vi.spyOn(context.storage!, 'createWriter').mockImplementation(async (options) => {
      const writer = await create(options);
      const save = writer.save.bind(writer);
      vi.spyOn(writer, 'save').mockImplementation(async (record) => {
        const result = await save(record);
        if (++rows === 1) {
          pressure.setBacklogged(true);
        }
        return result;
      });
      const update = writer.updateTableProgress.bind(writer);
      vi.spyOn(writer, 'updateTableProgress').mockImplementation(async (...args) => {
        progress = await update(...args);
        return progress;
      });
      return writer;
    });
    await snapshotter.setupCheckpointsCollection();
    await snapshotter.queueSnapshotTables(null);
    const running = snapshotter.replicationLoop().catch((error: unknown) => error);
    try {
      await entered.promise;
      expect(rows).toBe(1);
      expect(progress?.snapshotStatus?.replicatedCount).toBe(1);
      // A metadata barrier from another writer must remain free to drain storage.
      await using other = await context.storage!.createWriter(test_utils.BATCH_OPTIONS);
      expect((await other.getSourceTableStatus(progress!))?.snapshotStatus?.replicatedCount).toBe(1);
      if (action === 'resume') {
        pressure.setBacklogged(false);
        await snapshotter.waitForInitialSnapshot();
        expect(rows).toBe(3);
      } else {
        abort.abort();
        await running;
        expect(rows).toBe(1);
        expect(progress?.snapshotStatus?.replicatedCount).toBe(1);
      }
    } finally {
      abort.abort();
      await running;
    }
  }
);

async function prepareStreaming() {
  await using context = await openChangeStreamTestContext(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
    storageVersion: 4
  });
  await context.updateSyncRules(CONFIG);
  await context.db.createCollection('documents');
  await context.replicateSnapshot();
  await context.getCheckpoint();
  await context.stop();
  await context.db.collection<{ _id: number }>('documents').insertMany([1, 2, 3].map((_id) => ({ _id })));
}

test.skipIf(!env.TEST_MONGO_STORAGE).each(['idle', 'failure'])(
  'streaming pauses snapshots across full/filtered batches, then releases on %s',
  async (ending) => {
    await prepareStreaming();
    const gate = new SnapshotGate();
    const failure = new Error('reader failed while backlogged');
    let forcedFlush = false;
    let observedBacklog = false;
    let observedIdle = false;
    const provider = {
      ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
      openChangeStream: ({ open }: Parameters<typeof DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER.openChangeStream>[0]) =>
        (async function* () {
          const changes: MongoReplicationStreamChange[] = [];
          let sent = false;
          for await (const source of open({})) {
            if (sent) {
              yield source;
              if (!gate.isPaused) {
                observedIdle = true;
                context.abort();
                return;
              }
              continue;
            }
            for (const change of source.events) {
              if ('ns' in change.event && change.event.ns.coll === 'documents') {
                changes.push(change);
              }
            }
            source.getResumeToken();
            if (changes.length < 3) {
              continue;
            }
            const token = changes.at(-1)!.event._id;
            yield createMongoReplicationStreamBatch({
              events: changes.map((change, index) => ({ ...change, hasBufferedChanges: index < changes.length - 1 })),
              resolveResumeToken: () => token
            });
            expect(gate.isPaused).toBe(true);
            observedBacklog = true;
            // A filtered-only batch is not evidence that the source is idle.
            yield createMongoReplicationStreamBatch({
              events: [],
              resolveResumeToken: () => token,
              resolveMetadata: () => ({ filteredCount: 1 })
            });
            expect(gate.isPaused).toBe(true);
            if (ending === 'failure') {
              throw failure;
            }
            sent = true;
          }
        })()
    };
    await using context = await openChangeStreamTestContext(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
      storageVersion: 4,
      doNotClear: true,
      streamOptions: { snapshotGate: gate, createReplicationQueryProvider: () => provider }
    });
    await context.loadActiveSyncRules();
    context.storage!.registerListener({
      batchStarted: (writer) => {
        const save = writer.save.bind(writer);
        vi.spyOn(writer, 'save').mockImplementation(async (record) => {
          const result = await save(record);
          if (!forcedFlush) {
            forcedFlush = true;
            await writer.flush();
            expect(gate.isPaused).toBe(true);
          }
          return result;
        });
      }
    });
    if (ending === 'failure') {
      await expect(context.streamer.streamChangesInternal()).rejects.toBe(failure);
    } else {
      await expect(context.streamer.streamChangesInternal()).rejects.toBeInstanceOf(ReplicationAbortedError);
      expect(observedIdle).toBe(true);
    }
    expect(forcedFlush).toBe(true);
    expect(observedBacklog).toBe(true);
    expect(gate.isPaused).toBe(false);
  }
);
