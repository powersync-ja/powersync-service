import { storage } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { mongoTestStorageFactoryGenerator } from '../../../dist/utils/test-utils.js';

// A separate process exercises the database protocol without sharing any mutex,
// allocator, MongoClient, or lease object with the test runner.
async function main() {
  const factoryGen = mongoTestStorageFactoryGenerator({ url: process.argv[2], isCI: true });
  await using factory = await factoryGen.factory({ doNotClear: true });
  const streamId = Number(process.argv[3]);
  const stream = (await factory.getReplicatingReplicationStreams()).find((s) => s.replicationStreamId === streamId);
  const lock = await stream.lock();
  await using lockLifetime = { [Symbol.asyncDispose]: () => lock.release() };
  const bucketStorage = factory.getInstance(stream, { replicationLock: lock });
  await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  const table = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, streamId);
  await writer.save({
    sourceTable: table,
    tag: storage.SaveOperationTag.INSERT,
    after: { id: 'child', description: 'child' },
    afterReplicaId: test_utils.rid('child')
  });
  await writer.commit('1/2');
  console.log(`checkpoint=${(await bucketStorage.getCheckpoint()).checkpoint}`);
}

await main();
// Workspace test utilities start background timers. Exit after all resources are disposed.
process.exit(0);
