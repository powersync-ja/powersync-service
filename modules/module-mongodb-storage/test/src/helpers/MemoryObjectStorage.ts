import {
  ObjectStorage,
  ObjectStoragePutMetadata
} from '@module/storage/implementation/v3/object-storage/ObjectStorage.js';

export class MemoryObjectStorage implements ObjectStorage {
  /**
   * Public for testing purposes.
   */
  public readonly store = new Map<string, { data: Uint8Array; metadata: ObjectStoragePutMetadata }>();

  async put(path: string, data: Uint8Array, metadata: ObjectStoragePutMetadata): Promise<void> {
    this.store.set(path, { data, metadata: metadata });
  }

  async get(path: string, signal?: AbortSignal): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }> {
    signal?.throwIfAborted();
    const data = this.store.get(path);
    if (!data) {
      throw new Error(`NotFound: ${path}`);
    }
    return data;
  }

  async *list(prefix: string, signal?: AbortSignal): AsyncIterable<string> {
    for (const path of this.store.keys()) {
      signal?.throwIfAborted();
      if (path.startsWith(prefix)) {
        yield path;
      }
    }
  }

  async delete(paths: string[]): Promise<void> {
    for (const p of paths) {
      this.store.delete(p);
    }
  }
}
