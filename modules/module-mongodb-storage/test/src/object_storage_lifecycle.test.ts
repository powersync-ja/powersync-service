import { ObjectId } from 'bson';
import { createHash } from 'node:crypto';
import { describe, expect, test, vi } from 'vitest';
import {
  OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS,
  ObjectStorageLifecycle
} from '../../src/storage/implementation/v3/object-storage/ObjectStorageLifecycle.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';

describe('ObjectStorageLifecycle', () => {
  test('does not start PUTs when their protection has expired', async () => {
    const objectStorage = new MemoryObjectStorage();
    const lifecycle = new ObjectStorageLifecycle(null as any, 12, objectStorage);
    using put = vi.spyOn(objectStorage, 'put');
    await expect(
      lifecycle.storeUploads([
        {
          upload: {
            markerId: new ObjectId(),
            path: 'expired',
            deleteAfter: new Date(Date.now() + OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS)
          },
          payload: new Uint8Array()
        }
      ])
    ).rejects.toThrow('lease expired before PUT');
    expect(put).not.toHaveBeenCalled();
  });

  test('expiry cancels queued requests and joins an active request before rejecting', async () => {
    vi.useFakeTimers();
    using _clock = { [Symbol.dispose]: () => vi.useRealTimers() };
    const objectStorage = new MemoryObjectStorage();
    const lifecycle = new ObjectStorageLifecycle(null as any, 12, objectStorage);
    const release = Promise.withResolvers<void>();
    let activeSignal: AbortSignal | undefined;
    let queuedStarted = false;
    using _put = vi
      .spyOn(objectStorage, 'put')
      .mockImplementationOnce(async (_path, _data, _metadata, options) => {
        activeSignal = options!.signal;
        await release.promise;
        activeSignal!.throwIfAborted();
      })
      .mockImplementationOnce(async (_path, _data, _metadata, options) => {
        // Model waiting for the shared adapter's semaphore: observe abort before
        // starting the remote request, even when the active request is draining.
        await new Promise<void>((resolve) =>
          options!.signal!.addEventListener('abort', () => resolve(), { once: true })
        );
        options!.signal!.throwIfAborted();
        queuedStarted = true;
      });
    const uploads = ['active', 'queued'].map((path) => ({
      upload: {
        markerId: new ObjectId(),
        path,
        deleteAfter: new Date(Date.now() + OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS + 1000)
      },
      payload: new Uint8Array()
    }));
    const pending = lifecycle.storeUploads(uploads);
    let settled = false;
    const failed = expect(
      pending.finally(() => {
        settled = true;
      })
    ).rejects.toThrow('lease expired during PUT');
    await vi.advanceTimersByTimeAsync(1000);
    expect(activeSignal!.aborted).toBe(true);
    expect(queuedStarted).toBe(false);
    expect(settled).toBe(false);
    release.resolve();
    await failed;
  });

  test('escapes bucket names as path segments', () => {
    const lifecycle = new ObjectStorageLifecycle(null as any, 12, null as any);

    const path = lifecycle.allocatePath('definition', 'bucket/with spaces?and%escapes', 1n, 2n);

    expect(path).toMatch(/^bucket-data\/12\/definition\/bucket%2Fwith%20spaces%3Fand%25escapes\/1-2-[0-9a-f-]+\.bson$/);
  });

  test('hashes bucket path segments longer than 128 bytes', () => {
    const lifecycle = new ObjectStorageLifecycle(null as any, 12, null as any);
    const bucket = '💾/long bucket parameter/'.repeat(20);
    const hash = createHash('sha256').update(bucket).digest('base64url');

    const path = lifecycle.allocatePath('definition', bucket, 1n, 2n);

    expect(path).toMatch(new RegExp(`^bucket-data/12/definition/sha256-${hash}/1-2-[0-9a-f-]+\\.bson$`));
  });

  test('deletePrefix removes only objects under the exact prefix', async () => {
    const objectStorage = new MemoryObjectStorage();
    const lifecycle = new ObjectStorageLifecycle(null as any, 1, objectStorage);
    for (const path of [
      'bucket-data/1/definition/object-1.bson',
      'bucket-data/12/definition/object-2.bson',
      'bucket-data/1-other/definition/object-3.bson'
    ]) {
      await objectStorage.put(path, new Uint8Array(), {
        contentType: 'application/bson',
        contentEncoding: null
      });
    }

    await expect(lifecycle.deletePrefix(lifecycle.streamPrefix())).resolves.toEqual({ objectCount: 1 });
    expect([...objectStorage.store.keys()]).toEqual([
      'bucket-data/12/definition/object-2.bson',
      'bucket-data/1-other/definition/object-3.bson'
    ]);
  });

  test('definition prefixes do not match ids with the same string prefix', async () => {
    const objectStorage = new MemoryObjectStorage();
    const lifecycle = new ObjectStorageLifecycle(null as any, 7, objectStorage);
    for (const path of ['bucket-data/7/1/object-1.bson', 'bucket-data/7/12/object-2.bson']) {
      await objectStorage.put(path, new Uint8Array(), {
        contentType: 'application/bson',
        contentEncoding: null
      });
    }

    await lifecycle.deletePrefix(lifecycle.definitionPrefix('1'));
    expect([...objectStorage.store.keys()]).toEqual(['bucket-data/7/12/object-2.bson']);
  });
});
