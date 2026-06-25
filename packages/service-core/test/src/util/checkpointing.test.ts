import { createWriteCheckpoint, WriteCheckpointBatcher } from '@/index.js';
import { describe, expect, test, vi } from 'vitest';

function deferred<T = void>() {
  return Promise.withResolvers<T>();
}

async function waitForAsyncWork() {
  await new Promise((resolve) => setImmediate(resolve));
}

function createStorage() {
  let nextCheckpoint = 1n;
  const bucketStorage = {
    createManagedWriteCheckpoints: vi.fn(async (checkpoints: { user_id: string }[]) => {
      return new Map(checkpoints.map((checkpoint) => [checkpoint.user_id, nextCheckpoint++]));
    })
  };

  const storage = {
    getActiveSyncConfig: vi.fn(async () => ({ storage: bucketStorage }))
  };

  return { bucketStorage, storage };
}

function createBatcher(api: any, storage: any) {
  return new WriteCheckpointBatcher(
    () => api,
    () => storage
  );
}

describe('write checkpoint batching', () => {
  test('dispatches immediately up to capacity and batches queued requests once saturated', async () => {
    const gates = [deferred(), deferred(), deferred(), deferred()];
    const { bucketStorage, storage } = createStorage();
    const api = {
      createReplicationHead: vi.fn(async (callback: (head: string) => Promise<unknown>) => {
        const batch = api.createReplicationHead.mock.calls.length;
        const result = await callback(`head-${batch}`);
        await gates[batch - 1].promise;
        return result;
      })
    };
    const batcher = createBatcher(api, storage);

    const first = createWriteCheckpoint({
      userId: 'user-a',
      clientId: 'client-1',
      batcher
    });
    const second = createWriteCheckpoint({
      userId: 'user-b',
      clientId: undefined,
      batcher
    });
    await waitForAsyncWork();

    expect(api.createReplicationHead).toHaveBeenCalledTimes(2);

    const queued = [
      createWriteCheckpoint({ userId: 'user-c', clientId: 'client-3', batcher }),
      createWriteCheckpoint({ userId: 'user-d', clientId: undefined, batcher }),
      createWriteCheckpoint({ userId: 'user-e', clientId: undefined, batcher })
    ];

    await waitForAsyncWork();

    expect(api.createReplicationHead).toHaveBeenCalledTimes(3);
    expect(bucketStorage.createManagedWriteCheckpoints).toHaveBeenNthCalledWith(1, [
      { user_id: 'user-a/client-1', heads: { '1': 'head-1' } }
    ]);
    expect(bucketStorage.createManagedWriteCheckpoints).toHaveBeenNthCalledWith(2, [
      { user_id: 'user-b', heads: { '1': 'head-2' } }
    ]);
    expect(bucketStorage.createManagedWriteCheckpoints).toHaveBeenNthCalledWith(3, [
      { user_id: 'user-c/client-3', heads: { '1': 'head-3' } }
    ]);

    gates[0].resolve();
    await first;
    await waitForAsyncWork();

    expect(api.createReplicationHead).toHaveBeenCalledTimes(4);
    expect(bucketStorage.createManagedWriteCheckpoints).toHaveBeenNthCalledWith(4, [
      { user_id: 'user-d', heads: { '1': 'head-4' } },
      { user_id: 'user-e', heads: { '1': 'head-4' } }
    ]);

    gates[1].resolve();
    gates[2].resolve();
    gates[3].resolve();
    await expect(Promise.all([second, ...queued])).resolves.toEqual([
      { writeCheckpoint: '2', replicationHead: 'head-2' },
      { writeCheckpoint: '3', replicationHead: 'head-3' },
      { writeCheckpoint: '4', replicationHead: 'head-4' },
      { writeCheckpoint: '5', replicationHead: 'head-4' }
    ]);
  });

  test('allows three executing batches and queues later requests until one completes', async () => {
    const gates = [deferred(), deferred(), deferred(), deferred()];
    const { storage } = createStorage();
    const api = {
      createReplicationHead: vi.fn(async (callback: (head: string) => Promise<unknown>) => {
        const batch = api.createReplicationHead.mock.calls.length;
        const result = await callback(`head-${batch}`);
        await gates[batch - 1].promise;
        return result;
      })
    };
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
      })
    };
    const batcher = createBatcher(api, storage);

    await expect(createWriteCheckpoint({ userId: 'user-a', clientId: undefined, batcher })).rejects.toBe(error);
    expect(api.createReplicationHead).toHaveBeenCalledTimes(1);
  });
});
