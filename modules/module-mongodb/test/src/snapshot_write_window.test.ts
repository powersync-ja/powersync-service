import { MongoSnapshotWriteWindow } from '@module/replication/MongoSnapshotWriteWindow.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { BSON } from 'bson';
import { expect, test, vi } from 'vitest';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

test.skipIf(!env.TEST_MONGO_STORAGE).each(['rows', 'pages', 'bytes', 'tail', 'flush-failure', 'progress-failure'])(
  'snapshot write window persists progress after durable rows (%s)',
  async (mode) => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const stream = await factory.updateSyncRules(
      updateSyncRulesFromYaml('bucket_definitions:\n  global:\n    data:\n      - SELECT id FROM items', {
        storageVersion: 4
      })
    );
    const bucketStorage = factory.getInstance(stream);
    await using writer = await bucketStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      skipExistingRows: true,
      storeCurrentData: false
    });
    const table = await test_utils.resolveTestTable(writer, 'items', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);
    // Exercise the compatibility path for storage implementations without receipts.
    writer.queueTableProgress = undefined;
    const count = mode === 'rows' ? 24001 : mode === 'pages' ? 5 : 1;
    const window = new MongoSnapshotWriteWindow(writer, table, count);
    const flush = vi.spyOn(writer, 'flush');
    const progress = vi.spyOn(writer, 'updateTableProgress');
    const key = (i: number) => BSON.serialize({ _id: String(i) });
    try {
      for (let i = 1; i <= count; i++) {
        await writer.save({
          sourceTable: table,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: String(i) },
          afterReplicaId: test_utils.rid(String(i))
        });
        if (mode === 'rows' && i % 6000 === 0) {
          expect(await window.addPage(6000, 6000 * 20, key(i))).toBe(i === 24000 ? 24000 : 0);
          expect(progress).toHaveBeenCalledTimes(i === 24000 ? 1 : 0);
        }
        if (mode === 'pages' && i <= 4) {
          expect(await window.addPage(1, 20, key(i))).toBe(i === 4 ? 4 : 0);
          expect(progress).toHaveBeenCalledTimes(i === 4 ? 1 : 0);
        }
      }
      if (mode === 'rows') {
        expect(window.table.snapshotStatus?.replicatedCount).toBe(24000);
        expect(await window.addPage(1, 20, key(count))).toBe(0);
      } else {
        // Byte accounting is supplied by the source cursor, independently of row count.
        expect(await window.addPage(1, mode === 'bytes' ? 16 * 1024 * 1024 : 20, key(count))).toBe(
          mode === 'bytes' ? 1 : 0
        );
      }
      if (mode === 'flush-failure' || mode === 'progress-failure') {
        const failing = mode === 'flush-failure' ? flush : progress;
        failing.mockRejectedValueOnce(new Error('injected failure'));
        await expect(window.flush()).rejects.toThrow('injected failure');
        expect(window.table.snapshotStatus?.replicatedCount ?? 0).toBe(0);
        expect((await writer.getSourceTableStatus(table))?.snapshotStatus?.replicatedCount ?? 0).toBe(0);
        if (mode === 'flush-failure') expect(progress).not.toHaveBeenCalled();
      }
      expect(await window.flush()).toBe(mode === 'bytes' ? 0 : 1);
      const calls = progress.mock.calls.length;
      expect(await window.flush()).toBe(0);
      expect(progress).toHaveBeenCalledTimes(calls);
      const status = (await writer.getSourceTableStatus(table))!.snapshotStatus!;
      expect(status.replicatedCount).toBe(count);
      expect(status.totalEstimatedCount).toBe(count);
      expect(BSON.deserialize(status.lastKey!)._id).toBe(String(count));
      await writer.markTableSnapshotDone([window.table], '1/1');
      await writer.markAllSnapshotDone('1/1');
      await writer.commit('1/2');
      expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(BigInt(count));
    } finally {
      flush.mockRestore();
      progress.mockRestore();
    }
  }
);
