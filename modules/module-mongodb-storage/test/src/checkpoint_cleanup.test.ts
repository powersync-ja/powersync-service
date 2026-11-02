import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import { setTimeout } from 'node:timers/promises';
import { describe, expect, test } from 'vitest';
import { env } from './env.js';
import { TEST_STORAGE_VERSIONS } from './util.js';

describe.each(TEST_STORAGE_VERSIONS)('checkpoint cleanup v%s', (storageVersion) => {
  test('cancels an idle cursor without waiting for another checkpoint', async (context) => {
    const monitored = mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      monitorCommands: true
    });
    await using factory = await monitored.factory();
    const stream = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
config:
  edition: 3
streams:
  test:
    query: SELECT * FROM test
`,
        { storageVersion }
      )
    );
    const bucketStorage = await getTestStorage(factory, stream);
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    await writer.markAllSnapshotDone('1/0');
    await writer.keepalive('1/0');

    const reading = Promise.withResolvers<void>();
    const killed = Promise.withResolvers<void>();
    const monitor = (event: mongo.CommandStartedEvent) => {
      if (event.commandName === 'getMore' && event.command.collection === 'checkpoint_events') {
        reading.resolve();
      }
    };
    const finished = (event: mongo.CommandSucceededEvent) => {
      if (event.commandName === 'killCursors') killed.resolve();
    };
    factory.db.client.on('commandStarted', monitor);
    context.onTestFinished(() => {
      factory.db.client.off('commandStarted', monitor);
    });
    factory.db.client.on('commandSucceeded', finished);
    context.onTestFinished(() => {
      factory.db.client.off('commandSucceeded', finished);
    });
    const controller = new AbortController();
    context.onTestFinished(() => controller.abort());
    const iterator = bucketStorage
      .watchCheckpointChanges({ user_id: 'test', signal: controller.signal })
      [Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toMatchObject({ done: false, value: { base: { lsn: '1/0' } } });
    const next = iterator.next().catch(() => {});
    await reading.promise;

    controller.abort();
    const deadline = new AbortController();
    try {
      await Promise.race([
        Promise.all([next, iterator.return?.(), killed.promise]),
        setTimeout(2_000, undefined, { signal: deadline.signal }).then(() => {
          throw new Error('Checkpoint cleanup waited for an idle getMore or a new checkpoint');
        })
      ]);
    } finally {
      deadline.abort();
    }
  });
});
