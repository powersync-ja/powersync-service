import { MONGO_PREPARATION_WORKER } from '@module/replication/writeMongoChange.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { BSON } from 'bson';
import { expect, test, vi } from 'vitest';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

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
