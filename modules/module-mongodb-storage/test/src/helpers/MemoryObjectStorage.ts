import { ObjectStorage } from '@module/storage/implementation/v3/object-storage/ObjectStorage.js';

export class MemoryObjectStorage implements ObjectStorage {
  private store = new Map<string, Buffer>();

  async put(path: string, data: Buffer, _metadata?: any): Promise<void> {
    this.store.set(path, data);
  }

  async get(path: string): Promise<Buffer> {
    const data = this.store.get(path);
    if (!data) {
      throw new Error(`NotFound: ${path}`);
    }
    return data;
  }

  async delete(paths: string[]): Promise<void> {
    for (const p of paths) {
      this.store.delete(p);
    }
  }
}
