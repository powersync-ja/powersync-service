import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as pgwire from '@powersync/service-jpgwire';
import { expect, test, TestContext, vi } from 'vitest';
import * as checkpointUtils from '../../src/utils/checkpoints.js';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

/**
 * Reproduces checkpoints being committed while the Postgres notification connection is unavailable:
 *
 * 1. Start the checkpoint stream at 1/0 and configure a short idle session timeout on the connection
 *    that executed LISTEN.
 * 2. Wait for Postgres to destroy that connection and verify its `whenDestroyed` callback fires.
 * 3. Pause the replacement connection immediately before it executes LISTEN, then commit 2/0 and 3/0.
 *    Those notifications cannot be received because no notification channel is registered at that point.
 * 4. Allow LISTEN to complete. The `channels-registered` event writes `null` to the watcher, which
 *    must re-query storage and recover the latest missed checkpoint, 3/0.
 * 5. Commit 4/0 after the channel is restored and verify it is delivered as a normal live notification.
 *
 * The stream must therefore emit 1/0, 3/0, and 4/0 in order, proving that reconnect catches up to the
 * latest persisted state without emitting a stale missed checkpoint or losing later live notifications.
 */
test('checkpoint stream catches up in order after the notification connection is recreated', async (context) => {
  await using factory = await POSTGRES_STORAGE_FACTORY.factory();
  const reconnect = controlNotificationReconnect(factory, context);

  const syncRules = await factory.configureSyncRules(
    updateSyncRulesFromYaml(
      `
bucket_definitions:
  global:
    data: []
`,
      { validate: false }
    )
  );
  const bucketStorage = factory.getInstance(syncRules.persisted_sync_rules!);

  await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  await writer.markAllSnapshotDone('1/0');
  await writer.keepalive('1/0');

  const abortController = new AbortController();
  context.onTestFinished(() => abortController.abort());
  const iterator = bucketStorage
    .watchCheckpointChanges({ user_id: 'user', signal: abortController.signal })
    [Symbol.asyncIterator]();

  await expect(
    resolvesWithin({ promise: iterator.next(), description: 'Initial checkpoint should be returned' })
  ).resolves.toMatchObject({
    done: false,
    value: { base: { checkpoint: 0n, lsn: '1/0' } }
  });

  const notificationConnection = await resolvesWithin({
    promise: reconnect.initialConnection,
    description: 'Notification connection should register LISTEN'
  });
  await notificationConnection.query({ statement: `SET idle_session_timeout = '250ms'` });
  await resolvesWithin({
    promise: notificationConnection.whenDestroyed,
    description: 'Notification connection should be destroyed after the idle timeout'
  });
  expect(reconnect.connectionDestroyed).toHaveBeenCalledOnce();

  try {
    // Hold the replacement connection immediately before LISTEN so these updates
    // cannot produce notifications for this process.
    await resolvesWithin({ promise: reconnect.listenStarted, description: 'Connection pool should try to listen' });
    const recoveredWriteCheckpoint = await createManagedWriteCheckpoint(bucketStorage, '3/0');
    await writer.keepalive('2/0');
    await writer.keepalive('3/0');
    await expect(bucketStorage.getCheckpoint()).resolves.toMatchObject({ checkpoint: 0n, lsn: '3/0' });

    const recoveredCheckpoint = iterator.next();
    reconnect.allowListen();
    await resolvesWithin({ promise: reconnect.listenCompleted, description: 'Replacement LISTEN should complete' });

    await expect(
      resolvesWithin({ promise: recoveredCheckpoint, description: 'The checkpoint should be emitted after recovery' })
    ).resolves.toMatchObject({
      done: false,
      value: {
        base: { checkpoint: 0n, lsn: '3/0' },
        writeCheckpoint: recoveredWriteCheckpoint
      }
    });

    const liveWriteCheckpoint = await createManagedWriteCheckpoint(bucketStorage, '4/0');
    await writer.keepalive('4/0');
    await expect(
      resolvesWithin({
        promise: iterator.next(),
        description: 'Live checkpoint should be emitted after LISTEN is restored'
      })
    ).resolves.toMatchObject({
      done: false,
      value: {
        base: { checkpoint: 0n, lsn: '4/0' },
        writeCheckpoint: liveWriteCheckpoint
      }
    });
  } finally {
    reconnect.allowListen();
  }
}, 15_000);

/**
 * Reproduces the checkpoint advancing while the reconnect-triggered storage query is already in flight:
 *
 * 1. Recreate the notification connection as above, commit 2/0 while LISTEN is unavailable, then allow
 *    LISTEN to complete so its `null` event starts the recovery query.
 * 2. Let that query read 2/0 from Postgres, but pause it before returning the result to the watcher.
 * 3. While the query is paused and LISTEN is active, commit 3/0. Its notification is buffered behind the
 *    in-flight 2/0 query result.
 * 4. Release the stale 2/0 query result. The last-value-buffered public stream must emit the newer 3/0,
 *    rather than exposing 3/0 followed later by the stale 2/0.
 * 5. Commit 4/0 and verify it is emitted next, proving no delayed result can regress the stream afterward.
 *
 * This covers the boundary between reconnect recovery and normal notifications: concurrent progress may
 * supersede an in-flight query result, but observable checkpoints must remain monotonic and converge on the
 * latest persisted value. The immediate supersession of 2/0 by 3/0 is primarily provided by the
 * last-value-buffered sink; the monotonic watcher comparison is the additional guard that prevents an older
 * result from being emitted after a newer checkpoint.
 */
test('checkpoint stream emits the latest value when the checkpoint advances during the reconnect query', async (context) => {
  await using factory = await POSTGRES_STORAGE_FACTORY.factory();
  const reconnect = controlNotificationReconnect(factory, context);

  const syncRules = await factory.configureSyncRules(
    updateSyncRulesFromYaml(
      `
bucket_definitions:
  global:
    data: []
`,
      { validate: false }
    )
  );
  const bucketStorage = factory.getInstance(syncRules.persisted_sync_rules!);

  await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  await writer.markAllSnapshotDone('1/0');
  await writer.keepalive('1/0');

  const abortController = new AbortController();
  context.onTestFinished(() => abortController.abort());
  const iterator = bucketStorage
    .watchCheckpointChanges({ user_id: 'user', signal: abortController.signal })
    [Symbol.asyncIterator]();

  await expect(
    resolvesWithin({ promise: iterator.next(), description: 'Initial checkpoint should be returned' })
  ).resolves.toMatchObject({
    done: false,
    value: { base: { checkpoint: 0n, lsn: '1/0' } }
  });

  const notificationConnection = await resolvesWithin({
    promise: reconnect.initialConnection,
    description: 'Notification connection should register LISTEN'
  });
  await notificationConnection.query({ statement: `SET idle_session_timeout = '250ms'` });
  await resolvesWithin({
    promise: notificationConnection.whenDestroyed,
    description: 'Notification connection should be destroyed after the idle timeout'
  });
  expect(reconnect.connectionDestroyed).toHaveBeenCalledOnce();

  const queryResultCaptured = Promise.withResolvers<void>();
  const allowQueryResult = Promise.withResolvers<void>();
  const originalQuery = checkpointUtils.getActiveCheckpointDocument;
  let delayActiveCheckpointQuery = true;
  const activeCheckpointQuerySpy = vi
    .spyOn(checkpointUtils, 'getActiveCheckpointDocument')
    .mockImplementation(async (options) => {
      const result = await originalQuery(options);
      if (delayActiveCheckpointQuery) {
        delayActiveCheckpointQuery = false;
        queryResultCaptured.resolve();
        await allowQueryResult.promise;
      }
      return result;
    });

  try {
    await resolvesWithin({
      promise: reconnect.listenStarted,
      description: 'Replacement connection should attempt LISTEN'
    });

    await createManagedWriteCheckpoint(bucketStorage, '2/0');
    await writer.keepalive('2/0');

    const firstRecoveredCheckpoint = iterator.next();
    reconnect.allowListen();
    await resolvesWithin({ promise: reconnect.listenCompleted, description: 'Replacement LISTEN should complete' });
    await resolvesWithin({
      promise: queryResultCaptured.promise,
      description: 'Recovery query should read the active checkpoint'
    });
    await expect(bucketStorage.getCheckpoint()).resolves.toMatchObject({ checkpoint: 0n, lsn: '2/0' });

    // The query has read 2/0 but has not returned it to the watcher yet. Advance
    // to 3/0 so its notification is buffered behind the in-flight query result.
    const secondWriteCheckpoint = await createManagedWriteCheckpoint(bucketStorage, '3/0');
    await writer.keepalive('3/0');
    allowQueryResult.resolve();

    // The public stream is last-value buffered, so 3/0 supersedes the stale 2/0
    // query result before it is emitted to this consumer.
    await expect(
      resolvesWithin({
        promise: firstRecoveredCheckpoint,
        description: 'Latest checkpoint should supersede the delayed query result'
      })
    ).resolves.toMatchObject({
      done: false,
      value: {
        base: { checkpoint: 0n, lsn: '3/0' },
        writeCheckpoint: secondWriteCheckpoint
      }
    });

    const liveWriteCheckpoint = await createManagedWriteCheckpoint(bucketStorage, '4/0');
    await writer.keepalive('4/0');
    await expect(
      resolvesWithin({ promise: iterator.next(), description: 'Subsequent live checkpoint should be emitted' })
    ).resolves.toMatchObject({
      done: false,
      value: {
        base: { checkpoint: 0n, lsn: '4/0' },
        writeCheckpoint: liveWriteCheckpoint
      }
    });
  } finally {
    reconnect.allowListen();
    allowQueryResult.resolve();
    activeCheckpointQuerySpy.mockRestore();
  }
}, 15_000);

/**
 * Reproduces reconnecting when no checkpoint was committed during the notification outage:
 *
 * 1. Start the active-checkpoint watcher at 1/0, destroy its LISTEN connection, and pause the replacement
 *    connection before it restores LISTEN.
 * 2. Restore LISTEN without committing anything. The registration callback writes `null`, causing the
 *    watcher to query storage and read the same 1/0 checkpoint it emitted before the disconnect.
 * 3. Verify the next iterator read remains pending after that query completes. Re-registering the channel
 *    must not turn an unchanged persisted checkpoint into a duplicate stream event.
 * 4. Commit 2/0 and verify the already-pending read resolves with that actual advancement.
 *
 * This test observes the internal active-checkpoint stream directly so the public write-checkpoint layer's
 * separate deduplication cannot hide a duplicate produced by the reconnect watcher itself.
 */
test('active checkpoint stream does not duplicate an unchanged checkpoint after reconnect', async (context) => {
  await using factory = await POSTGRES_STORAGE_FACTORY.factory();
  const reconnect = controlNotificationReconnect(factory, context);

  const syncRules = await factory.configureSyncRules(
    updateSyncRulesFromYaml(
      `
bucket_definitions:
  global:
    data: []
`,
      { validate: false }
    )
  );
  const bucketStorage = factory.getInstance(syncRules.persisted_sync_rules!);

  await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
  await writer.markAllSnapshotDone('1/0');
  await writer.keepalive('1/0');

  const abortController = new AbortController();
  context.onTestFinished(() => abortController.abort());

  // Observe the protected watcher directly for this assertion. watchCheckpointChanges()
  // independently suppresses unchanged operation and write checkpoints, which would hide
  // a duplicate emitted by watchActiveCheckpoint() and let this regression test pass incorrectly.
  const iterator = (
    bucketStorage as unknown as {
      watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<storage.ReplicationCheckpoint>;
    }
  )
    .watchActiveCheckpoint(abortController.signal)
    [Symbol.asyncIterator]();

  await expect(
    resolvesWithin({ promise: iterator.next(), description: 'Initial active checkpoint should be returned' })
  ).resolves.toMatchObject({
    done: false,
    value: { checkpoint: 0n, lsn: '1/0' }
  });

  const notificationConnection = await resolvesWithin({
    promise: reconnect.initialConnection,
    description: 'Notification connection should register LISTEN'
  });
  // Simulate a small idle timeout, which will cause the notification conection to close
  await notificationConnection.query({ statement: `SET idle_session_timeout = '250ms'` });
  await resolvesWithin({
    promise: notificationConnection.whenDestroyed,
    description: 'Notification connection should be destroyed after the idle timeout'
  });
  expect(reconnect.connectionDestroyed).toHaveBeenCalledOnce();

  const unchangedCheckpointQueried = Promise.withResolvers<void>();
  const originalQuery = checkpointUtils.getActiveCheckpointDocument;
  let observeActiveCheckpointQuery = true;
  const activeCheckpointQuerySpy = vi
    .spyOn(checkpointUtils, 'getActiveCheckpointDocument')
    .mockImplementation(async (options) => {
      const result = await originalQuery(options);
      if (observeActiveCheckpointQuery) {
        observeActiveCheckpointQuery = false;
        unchangedCheckpointQueried.resolve();
      }
      return result;
    });

  try {
    await resolvesWithin({
      promise: reconnect.listenStarted,
      description: 'Replacement connection should attempt LISTEN'
    });
    const nextCheckpoint = iterator.next();
    reconnect.allowListen();
    await resolvesWithin({ promise: reconnect.listenCompleted, description: 'Replacement LISTEN should complete' });
    await resolvesWithin({
      promise: unchangedCheckpointQueried.promise,
      description: 'Reconnect query should return the unchanged active checkpoint'
    });

    const duplicateEmitted = await Promise.race([
      nextCheckpoint.then(() => true),
      new Promise<false>((resolve) => setTimeout(() => resolve(false), 100))
    ]);
    expect(duplicateEmitted).toBe(false);

    await writer.keepalive('2/0');
    await expect(
      resolvesWithin({
        promise: nextCheckpoint,
        description: 'Pending iterator should resolve after the checkpoint advances'
      })
    ).resolves.toMatchObject({
      done: false,
      value: { checkpoint: 0n, lsn: '2/0' }
    });
  } finally {
    reconnect.allowListen();
    activeCheckpointQuerySpy.mockRestore();
  }
}, 15_000);

test('active checkpoint stream closes when a new replication stream is activated', async (context) => {
  await using factory = await POSTGRES_STORAGE_FACTORY.factory();

  const initialSyncRules = await factory.configureSyncRules(
    updateSyncRulesFromYaml(
      `
bucket_definitions:
  initial:
    data: []
`,
      { validate: false }
    )
  );
  const initialStorage = factory.getInstance(initialSyncRules.persisted_sync_rules!);

  await using initialWriter = await initialStorage.createWriter(test_utils.BATCH_OPTIONS);
  await initialWriter.markAllSnapshotDone('1/0');
  await initialWriter.keepalive('1/0');

  const abortController = new AbortController();
  context.onTestFinished(() => abortController.abort());
  const iterator = (
    initialStorage as unknown as {
      watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<storage.ReplicationCheckpoint>;
    }
  )
    .watchActiveCheckpoint(abortController.signal)
    [Symbol.asyncIterator]();

  await expect(
    resolvesWithin({ promise: iterator.next(), description: 'Initial active checkpoint should be returned' })
  ).resolves.toMatchObject({
    done: false,
    value: { checkpoint: 0n, lsn: '1/0' }
  });

  const streamClosed = iterator.next();
  const nextSyncRules = await factory.configureSyncRules(
    updateSyncRulesFromYaml(
      `
bucket_definitions:
  replacement:
    data: []
`,
      { validate: false }
    )
  );
  const nextStorage = factory.getInstance(nextSyncRules.persisted_sync_rules!);

  await using nextWriter = await nextStorage.createWriter(test_utils.BATCH_OPTIONS);
  await nextWriter.markAllSnapshotDone('2/0');
  await nextWriter.keepalive('2/0');

  await expect(
    resolvesWithin({ promise: streamClosed, description: 'Old active checkpoint stream should close on activation' })
  ).resolves.toEqual({ done: true, value: undefined });
}, 15_000);

type PostgresTestFactory = Awaited<ReturnType<typeof POSTGRES_STORAGE_FACTORY.factory>>;

function controlNotificationReconnect(factory: PostgresTestFactory, context: TestContext) {
  const firstListen = Promise.withResolvers<pgwire.PgConnection>();
  const reconnectListenStarted = Promise.withResolvers<void>();
  const allowReconnectListen = Promise.withResolvers<void>();
  const reconnectListenCompleted = Promise.withResolvers<void>();
  const notificationConnectionDestroyed = vi.fn();
  let listenCount = 0;
  let notificationRegistrationCount = 0;

  context.onTestFinished(() => allowReconnectListen.resolve());
  const disposeConnectionListener = factory.db.registerListener({
    notificationEvent: (event) => {
      if (event.type != 'channels-registered') {
        return;
      }
      notificationRegistrationCount++;
      if (notificationRegistrationCount === 2) {
        reconnectListenCompleted.resolve();
      }
    },
    connectionCreated: async (connection) => {
      const originalQuery = connection.query.bind(connection) as (...args: any[]) => Promise<any>;

      vi.spyOn(connection, 'query').mockImplementation(async (...args: any[]) => {
        const statement = args[0]?.statement;
        if (typeof statement === 'string' && statement.startsWith('LISTEN ')) {
          listenCount++;

          if (listenCount === 1) {
            const result = await originalQuery(...args);
            connection.whenDestroyed.then(notificationConnectionDestroyed);
            firstListen.resolve(connection);
            return result;
          }

          if (listenCount === 2) {
            reconnectListenStarted.resolve();
            await allowReconnectListen.promise;
          }
        }

        return originalQuery(...args);
      });
    }
  });
  context.onTestFinished(disposeConnectionListener);

  return {
    initialConnection: firstListen.promise,
    listenStarted: reconnectListenStarted.promise,
    listenCompleted: reconnectListenCompleted.promise,
    allowListen: () => allowReconnectListen.resolve(),
    connectionDestroyed: notificationConnectionDestroyed
  };
}

async function createManagedWriteCheckpoint(
  bucketStorage: storage.SyncRulesBucketStorage,
  lsn: string
): Promise<bigint> {
  const checkpoints = await bucketStorage.createManagedWriteCheckpoints([
    {
      heads: { '1': lsn },
      user_id: 'user'
    }
  ]);
  const checkpoint = checkpoints.get('user');
  expect(checkpoint).toBeDefined();
  return checkpoint!;
}

async function resolvesWithin<T>({
  promise,
  description,
  timeoutMs = 5_000
}: {
  promise: Promise<T>;
  description: string;
  timeoutMs?: number;
}): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeout = setTimeout(() => reject(new Error(`Timed out waiting for ${description}`)), timeoutMs);
      })
    ]);
  } finally {
    clearTimeout(timeout);
  }
}
