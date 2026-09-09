import { MONGO_PREPARATION_WORKER } from '@module/replication/writeMongoChange.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import { BSON } from 'bson';
import { expect, test, vi } from 'vitest';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

test.skipIf(!env.TEST_MONGO_STORAGE).each(['none', 'application', 'preparation'])(
  'bounds four workers and applies out-of-order preparation in source order (failure=%s)',
  async (failure) => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const stream = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  global:
    data:
      - SELECT _id AS id, description FROM items
`,
        { storageVersion: 4 }
      )
    );
    const bucketStorage = factory.getInstance(stream);
    const release = Promise.withResolvers<void>();
    const applying = Promise.withResolvers<void>();
    const fourthPrepared = Promise.withResolvers<void>();
    const releaseSecond = Promise.withResolvers<void>();
    const workers = new Set<storage.RowPreparationWorker>();
    let applications = 0;
    let preparations = 0;
    await using writer = await bucketStorage.createWriter({
      ...test_utils.BATCH_OPTIONS,
      storeCurrentData: false,
      hooks: {
        beforeBatchFlush: async () => {
          if (++applications === 1) {
            applying.resolve();
            await release.promise;
            if (failure === 'application') throw new Error('application failed');
          }
        }
      }
    });
    const table = await test_utils.resolveTestTable(writer, 'items', ['_id'], INITIALIZED_MONGO_STORAGE_FACTORY);
    await writer.markAllSnapshotDone('1/1');
    const prepare = storage.RowPreparationWorker.prototype.prepare;
    const spy = vi.spyOn(storage.RowPreparationWorker.prototype, 'prepare').mockImplementation(async function (
      this: storage.RowPreparationWorker,
      rows
    ) {
      workers.add(this);
      const result = await prepare.call(this, rows);
      preparations++;
      if (preparations === 4) {
        fourthPrepared.resolve();
        releaseSecond.resolve();
        if (failure === 'preparation') throw new Error('preparation failed');
      }
      if (BSON.deserialize(rows[0].raw).description === 'second') await releaseSecond.promise;
      return result;
    });
    const saveBlock = async (description: string) => {
      for (let i = 0; i < 2000; i++)
        await writer.saveRaw!({
          tag: storage.SaveOperationTag.UPDATE,
          sourceTable: table,
          raw: BSON.serialize({ _id: `${i}`, description }),
          worker: MONGO_PREPARATION_WORKER,
          convert: () => {
            throw new Error('Expected worker preparation');
          }
        });
    };
    try {
      await saveBlock('first');
      await applying.promise;
      await saveBlock('second');
      await saveBlock('third');
      await saveBlock('fourth');
      await fourthPrepared.promise;
      expect(applications).toBe(1);
      expect(workers.size).toBe(4);
      let fifthAdmitted = false;
      const fifth = saveBlock('fifth').then(() => {
        fifthAdmitted = true;
      });
      void fifth.catch(() => {});
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(fifthAdmitted).toBe(false);
      expect(preparations).toBe(4);
      expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
      release.resolve();
      if (failure !== 'none') {
        await expect(fifth).rejects.toThrow(`${failure} failed`);
        await expect(writer.commit('1/2')).rejects.toThrow();
        expect(applications).toBe(1);
        expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
      } else {
        await fifth;
        await writer.commit('1/2');
        expect(preparations).toBe(5);
        expect(applications).toBe(5);
        const checkpoint = await bucketStorage.getCheckpoint();
        expect(checkpoint.checkpoint).toBe(10000n);
        const chunks = await test_utils.fromAsync(
          bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(stream.syncConfigContent[0], 'global[]', 0n)], {
            limit: 12000,
            chunkLimitBytes: 16 * 1024 * 1024
          })
        );
        const operations = chunks.flatMap((chunk) => ('chunkData' in chunk ? chunk.chunkData.data : []));
        expect(operations).toHaveLength(10000);
        for (const [block, description] of ['first', 'second', 'third', 'fourth', 'fifth'].entries()) {
          expect(
            operations
              .slice(block * 2000, (block + 1) * 2000)
              .every((op) => JSON.parse(op.data!).description === description)
          ).toBe(true);
        }
      }
    } finally {
      release.resolve();
      releaseSecond.resolve();
      spy.mockRestore();
    }
  }
);

test.skipIf(!env.TEST_MONGO_STORAGE)('source admission overlaps preparation, but progress waits for it', async () => {
  await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
  const stream = await factory.updateSyncRules(
    updateSyncRulesFromYaml(
      `
bucket_definitions:
  global:
    data:
      - SELECT _id AS id, description FROM items
`,
      { storageVersion: 4 }
    )
  );
  const bucketStorage = factory.getInstance(stream);
  await using writer = await bucketStorage.createWriter({ ...test_utils.BATCH_OPTIONS, storeCurrentData: false });
  const table = await test_utils.resolveTestTable(writer, 'items', ['_id'], INITIALIZED_MONGO_STORAGE_FACTORY);
  await writer.markAllSnapshotDone('1/1');
  const release = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  const prepare = storage.RowPreparationWorker.prototype.prepare;
  const spy = vi.spyOn(storage.RowPreparationWorker.prototype, 'prepare').mockImplementationOnce(async function (
    this: storage.RowPreparationWorker,
    rows
  ) {
    started.resolve();
    await release.promise;
    return prepare.call(this, rows);
  });
  const save = (id: string, description: string, tag = storage.SaveOperationTag.INSERT) =>
    writer.saveRaw!({
      tag: tag as storage.SaveOperationTag.INSERT | storage.SaveOperationTag.UPDATE,
      sourceTable: table,
      raw: BSON.serialize({ _id: id, description }),
      worker: MONGO_PREPARATION_WORKER,
      convert: () => {
        throw new Error('Expected worker preparation');
      }
    });
  try {
    // The first full block is admitted without waiting for the deliberately blocked worker.
    for (let i = 0; i < 2000; i++) await save(`${i}`, 'first');
    await started.promise;
    await save('0', 'second', storage.SaveOperationTag.UPDATE);
    let progressed = false;
    const boundary = writer.queueResumeLsn!('1/2').then(async (receipt) => {
      await receipt.persisted;
      progressed = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(progressed).toBe(false);
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(0n);
    release.resolve();
    await boundary;
    await writer.commit('1/2');
    expect((await bucketStorage.getCheckpoint()).checkpoint).toBe(2001n);
    expect(spy).toHaveBeenCalledTimes(2);
  } finally {
    release.resolve();
    spy.mockRestore();
  }
});
