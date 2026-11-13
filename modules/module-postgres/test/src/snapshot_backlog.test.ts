import { SnapshotGate, storage } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { expect, test, vi } from 'vitest';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

const CONFIG = /* yaml */ `
  # Sync config fixture.
  config:
    edition: 3
  streams:
    items:
      query: SELECT * FROM test_data
`;

test.skipIf(!env.TEST_MONGO_STORAGE).each(['key scan', 'cursor'])(
  'pauses a Postgres snapshot after persisting its page (%s)',
  async (mode) => {
    const gate = new SnapshotGate();
    using pressure = gate.trackStreaming();
    await using context = await WalStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
      storageVersion: 4,
      walStreamOptions: { snapshotGate: gate, snapshotChunkLength: 1, snapshotProgressIntervalMs: 60_000 }
    });
    await context.updateSyncRules(CONFIG);
    await context.pool.query(
      mode === 'key scan'
        ? 'CREATE TABLE test_data(id integer primary key, other integer)'
        : 'CREATE TABLE test_data(id integer, other integer, PRIMARY KEY(id, other))'
    );
    await context.pool.query('INSERT INTO test_data VALUES (1, 1), (2, 2), (3, 3)');
    let rows = 0;
    let progress: storage.SourceTable | undefined;
    const entered = Promise.withResolvers<void>();
    const wait = gate.wait.bind(gate);
    using _wait = vi.spyOn(gate, 'wait').mockImplementation((signal) => {
      if (gate.isPaused) {
        entered.resolve();
      }
      return wait(signal);
    });
    context.storage!.registerListener({
      batchStarted: (writer) => {
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
      }
    });
    const running = context.replicateSnapshot();
    try {
      await entered.promise;
      expect(rows).toBe(1);
      expect(progress?.snapshotStatus?.replicatedCount).toBe(1);
      await using other = await context.storage!.createWriter(test_utils.BATCH_OPTIONS);
      expect((await other.getSourceTableStatus(progress!))?.snapshotStatus?.replicatedCount).toBe(1);
    } finally {
      pressure.setBacklogged(false);
      await running;
    }
    expect(rows).toBe(3);
  }
);

test.skipIf(!env.TEST_MONGO_STORAGE)(
  'Postgres streaming gates snapshots while a flushed transaction is unfinished',
  async () => {
    const gate = new SnapshotGate();
    await using context = await WalStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
      storageVersion: 4,
      walStreamOptions: { snapshotGate: gate }
    });
    await context.updateSyncRules(CONFIG);
    await context.pool.query('CREATE TABLE test_data(id integer primary key)');
    const flushed = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let forced = false;
    context.storage!.registerListener({
      batchStarted: (writer) => {
        const save = writer.save.bind(writer);
        vi.spyOn(writer, 'save').mockImplementation(async (record) => {
          const result = await save(record);
          if (!forced) {
            forced = true;
            await writer.flush();
            flushed.resolve();
            await release.promise;
          }
          return result;
        });
      }
    });
    await context.initializeReplication();
    await context.pool.query('INSERT INTO test_data VALUES (1), (2), (3)');
    try {
      await flushed.promise;
      expect(gate.isPaused).toBe(true);
    } finally {
      release.resolve();
    }
    await gate.wait(new AbortController().signal);
    expect(gate.isPaused).toBe(false);
    await context.getCheckpoint();
  }
);
