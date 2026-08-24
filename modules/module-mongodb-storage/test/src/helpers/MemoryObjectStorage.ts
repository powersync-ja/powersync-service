import {
  ObjectStorage,
  ObjectStorageOperationOptions,
  ObjectStoragePutMetadata
} from '@module/storage/implementation/v3/object-storage/ObjectStorage.js';

export class MemoryObjectStorage implements ObjectStorage {
  /**
   * Public for testing purposes.
   */
  public readonly store = new Map<string, { data: Uint8Array; metadata: ObjectStoragePutMetadata }>();

  async put(
    path: string,
    data: Uint8Array,
    metadata: ObjectStoragePutMetadata,
    options?: ObjectStorageOperationOptions
  ): Promise<void> {
    options?.signal?.throwIfAborted();
    this.store.set(path, { data, metadata: metadata });
  }

  async get(
    path: string,
    options?: ObjectStorageOperationOptions
  ): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }> {
    const signal = options?.signal;
    signal?.throwIfAborted();
    const data = this.store.get(path);
    if (!data) {
      throw new Error(`NotFound: ${path}`);
    }
    return data;
  }

  async *list(prefix: string, options?: ObjectStorageOperationOptions): AsyncIterable<string> {
    const signal = options?.signal;
    for (const path of this.store.keys()) {
      signal?.throwIfAborted();
      if (path.startsWith(prefix)) {
        yield path;
      }
    }
  }

  async delete(paths: string[], options?: ObjectStorageOperationOptions): Promise<void> {
    options?.signal?.throwIfAborted();
    for (const p of paths) {
      this.store.delete(p);
    }
  }

  async deletePrefix(prefix: string, options?: ObjectStorageOperationOptions): Promise<{ objectCount: number }> {
    const paths: string[] = [];
    for await (const path of this.list(prefix, options)) {
      paths.push(path);
    }
    await this.delete(paths, options);
    return { objectCount: paths.length };
  }
}
