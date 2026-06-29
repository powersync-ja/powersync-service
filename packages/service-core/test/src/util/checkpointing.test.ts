import { createWriteCheckpoint, storage, WriteCheckpointBatcher } from '@/index.js';
import { describe, expect, test, vi } from 'vitest';

function deferred<T = void>() {
  return Promise.withResolvers<T>();
}

async function waitForAsyncWork() {
  await new Promise((resolve) => setTimeout(resolve, 0));
  await new Promise((resolve) => setImmediate(resolve));
}

function createStorage(options?: { shouldAdvance?: (checkpoint: { user_id: string }) => boolean }) {
  let nextCheckpoint = 1n;
  const bucketStorage = {
    createManagedWriteCheckpoints: vi.fn(async (checkpoints: { user_id: string }[]) => {
      return {
        writeCheckpoints: new Map(checkpoints.map((checkpoint) => [checkpoint.user_id, nextCheckpoint++])),
        shouldAdvance: checkpoints.some((checkpoint) => options?.shouldAdvance?.(checkpoint) ?? true)
      };
    })
  };

  const storage = {
    getActiveSyncConfig: vi.fn(async () => ({ storage: bucketStorage }))
  };

  return { bucketStorage, storage };
}

/**
 * Mock RouteAPI.createReplicationHead. The callback persists the write-checkpoint
 * mapping and returns { response, shouldAdvance }; the source marker (tracked by
 * `advanceMarker`) is only forced when shouldAdvance is true. An optional gate per
 * batch keeps the call in flight so concurrency can be exercised.
 */
function createApi(options?: { gates?: PromiseWithResolvers<void>[] }) {
  const advanceMarker = vi.fn(async (_head: string) => undefined);
  const createReplicationHead = vi.fn(
    async (callback: (head: string) => Promise<{ response: unknown; shouldAdvance: boolean }>) => {
      const batch = createReplicationHead.mock.calls.length;
      const head = `head-${batch}`;
      const { response, shouldAdvance } = await callback(head);
      if (shouldAdvance) {
        await advanceMarker(head);
      }
      if (options?.gates) {
        await options.gates[batch - 1].promise;
      }
      return response;
    }
  );
  return { createReplicationHead, advanceMarker };
}

function createBatcher(api: any, storage: any) {
  return new WriteCheckpointBatcher(
    () => (callback) => api.createReplicationHead(callback),
    () => storage
  );
}

describe('write checkpoint batching', () => {
  test('deduplicates managed checkpoints by greatest supplied request id per user', () => {
    expect(
      storage.uniqueManagedWriteCheckpoints([
        { user_id: 'user-a', heads: { '1': 'generated' } },
        { user_id: 'user-a', heads: { '1': 'stale' }, checkpoint_request_id: 41n },
        { user_id: 'user-a', heads: { '1': 'winner' }, checkpoint_request_id: 43n },
        { user_id: 'user-a', heads: { '1': 'lower' }, checkpoint_request_id: 42n },
        { user_id: 'user-b', heads: { '1': 'generated-b' } }
      ])
    ).toEqual([
      { user_id: 'user-a', heads: { '1': 'winner' }, checkpoint_request_id: 43n },
      { user_id: 'user-b', heads: { '1': 'generated-b' } }
    ]);
  });

  test('coalesces same-turn requests and dispatches queued requests as capacity becomes available', async () => {
    const gates = [deferred(), deferred()];
    const { bucketStorage, storage } = createStorage();
    const api = createApi({ gates });
    const batcher = createBatcher(api, storage);

    const firstBatch = [
      createWriteCheckpoint({
        userId: 'user-a',
        clientId: 'client-1',
        batcher
      }),
      createWriteCheckpoint({
        userId: 'user-b',
        clientId: undefined,
        batcher
      })
    ];
    await waitForAsyncWork();

    expect(api.createReplicationHead).toHaveBeenCalledTimes(1);
    expect(bucketStorage.createManagedWriteCheckpoints).toHaveBeenNthCalledWith(1, [
      { user_id: 'user-a/client-1', heads: { '1': 'head-1' } },
      { user_id: 'user-b', heads: { '1': 'head-1' } }
    ]);

    const secondBatch = [
      createWriteCheckpoint({ userId: 'user-c', clientId: 'client-3', batcher }),
      createWriteCheckpoint({ userId: 'user-d', clientId: undefined, batcher }),
      createWriteCheckpoint({ userId: 'user-e', clientId: undefined, batcher })
    ];

    await waitForAsyncWork();

    expect(api.createReplicationHead).toHaveBeenCalledTimes(2);
    expect(bucketStorage.createManagedWriteCheckpoints).toHaveBeenNthCalledWith(2, [
      { user_id: 'user-c/client-3', heads: { '1': 'head-2' } },
      { user_id: 'user-d', heads: { '1': 'head-2' } },
      { user_id: 'user-e', heads: { '1': 'head-2' } }
    ]);

    gates[0].resolve();
    gates[1].resolve();
    await expect(Promise.all([...firstBatch, ...secondBatch])).resolves.toEqual([
      { writeCheckpoint: '1', replicationHead: 'head-1' },
      { writeCheckpoint: '2', replicationHead: 'head-1' },
      { writeCheckpoint: '3', replicationHead: 'head-2' },
      { writeCheckpoint: '4', replicationHead: 'head-2' },
      { writeCheckpoint: '5', replicationHead: 'head-2' }
    ]);
  });

  test('passes supplied checkpoint request ids into the storage batch', async () => {
    const { bucketStorage, storage } = createStorage();
    const api = createApi();
    const batcher = createBatcher(api, storage);

    await expect(
      createWriteCheckpoint({
        userId: 'user-a',
        clientId: 'client-1',
        checkpointRequestId: 42n,
        batcher
      })
    ).resolves.toEqual({ writeCheckpoint: '1', replicationHead: 'head-1' });

    expect(bucketStorage.createManagedWriteCheckpoints).toHaveBeenCalledWith([
      {
        user_id: 'user-a/client-1',
        heads: { '1': 'head-1' },
        checkpoint_request_id: 42n
      }
    ]);
  });

  test('does not advance the source marker when storage reports no checkpoint updates', async () => {
    const { storage } = createStorage({ shouldAdvance: () => false });
    const api = createApi();
    const batcher = createBatcher(api, storage);

    await expect(createWriteCheckpoint({ userId: 'user-a', clientId: undefined, batcher })).resolves.toEqual({
      writeCheckpoint: '1',
      replicationHead: 'head-1'
    });

    expect(api.createReplicationHead).toHaveBeenCalledTimes(1);
    expect(api.advanceMarker).not.toHaveBeenCalled();
  });

  test('advances the source marker when storage reports a checkpoint update', async () => {
    const { storage } = createStorage();
    const api = createApi();
    const batcher = createBatcher(api, storage);

    await createWriteCheckpoint({ userId: 'user-a', clientId: undefined, batcher });

    expect(api.advanceMarker).toHaveBeenCalledTimes(1);
    expect(api.advanceMarker).toHaveBeenCalledWith('head-1');
  });

  test('allows three executing batches and queues later requests until one completes', async () => {
    const gates = [deferred(), deferred(), deferred(), deferred()];
    const { storage } = createStorage();
    const api = createApi({ gates });
    const batcher = createBatcher(api, storage);

    const first = createWriteCheckpoint({
      userId: 'user-a',
      clientId: undefined,
      batcher
    });
    await waitForAsyncWork();

    const second = createWriteCheckpoint({
      userId: 'user-b',
      clientId: undefined,
      batcher
    });
    await waitForAsyncWork();

    const third = createWriteCheckpoint({
      userId: 'user-c',
      clientId: undefined,
      batcher
    });
    await waitForAsyncWork();

    expect(api.createReplicationHead).toHaveBeenCalledTimes(3);

    const fourth = createWriteCheckpoint({
      userId: 'user-d',
      clientId: undefined,
      batcher
    });
    await waitForAsyncWork();

    expect(api.createReplicationHead).toHaveBeenCalledTimes(3);

    gates[0].resolve();
    await first;
    await waitForAsyncWork();

    expect(api.createReplicationHead).toHaveBeenCalledTimes(4);

    gates[1].resolve();
    gates[2].resolve();
    gates[3].resolve();

    await expect(Promise.all([second, third, fourth])).resolves.toEqual([
      { writeCheckpoint: '2', replicationHead: 'head-2' },
      { writeCheckpoint: '3', replicationHead: 'head-3' },
      { writeCheckpoint: '4', replicationHead: 'head-4' }
    ]);
  });

  test('passes batch errors through without retrying', async () => {
    const error = new Error('source unavailable');
    const { storage } = createStorage();
    const api = {
      createReplicationHead: vi.fn(async () => {
        throw error;
      }),
      advanceMarker: vi.fn(async () => undefined)
    };
    const batcher = createBatcher(api, storage);

    await expect(createWriteCheckpoint({ userId: 'user-a', clientId: undefined, batcher })).rejects.toBe(error);
    expect(api.createReplicationHead).toHaveBeenCalledTimes(1);
    expect(api.advanceMarker).not.toHaveBeenCalled();
  });
});
