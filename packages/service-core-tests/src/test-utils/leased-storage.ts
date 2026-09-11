import { storage } from '@powersync/service-core';

const leases = new WeakMap<storage.BucketStorageFactory, Map<number, Promise<storage.ReplicationLock>>>();

/**
 * Acquire a real replication lease before constructing writable test storage.
 * Writers in the same test job share a lease per factory/stream. The factory owns
 * these leases and releases them at teardown; takeover tests manage their own leases.
 */
export async function getTestStorage<T extends storage.BucketStorageFactory>(
  factory: T,
  stream: storage.PersistedReplicationStream,
  options?: Parameters<T['getInstance']>[1]
): Promise<ReturnType<T['getInstance']>> {
  let owned = leases.get(factory);
  if (owned == null) {
    owned = new Map();
    leases.set(factory, owned);
    const factoryLeases = owned;
    // `await using` captures the disposer before this helper runs. Register a
    // lifecycle callback instead of replacing that already-captured method.
    const unregister = factory.registerListener({
      beforeDispose: async () => {
        unregister();
        try {
          for (const lock of factoryLeases.values()) {
            await (await lock).release();
          }
        } finally {
          factoryLeases.clear();
        }
      }
    });
  }
  let pending = owned.get(stream.replicationStreamId);
  if (pending == null) {
    pending = stream.lock();
    owned.set(stream.replicationStreamId, pending);
  }
  let lock: storage.ReplicationLock;
  try {
    lock = await pending;
  } catch (error) {
    owned.delete(stream.replicationStreamId);
    throw error;
  }
  return factory.getInstance(stream, { ...options, replicationLock: lock }) as ReturnType<T['getInstance']>;
}

/** End a test job before starting a replacement through another factory. */
export async function releaseTestStorageLease(factory: storage.BucketStorageFactory, streamId: number) {
  const owned = leases.get(factory);
  const pending = owned?.get(streamId);
  if (pending != null) {
    await (await pending).release();
    owned!.delete(streamId);
  }
}
