import * as zstd from '@mongodb-js/zstd';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketOperation } from '../models.js';
import { ObjectStorage } from './ObjectStorage.js';

export class BucketDataObjectStorage {
  constructor(private readonly storage: ObjectStorage) {}

  async store(path: string, ops: BucketOperation[]): Promise<{ compressedSize: number }> {
    const bsonBuffer = Buffer.from(bson.serialize({ ops }));
    const compressedUint8 = await zstd.compress(bsonBuffer);
    const compressed = Buffer.from(compressedUint8);
    await this.storage.put(path, compressed);
    return { compressedSize: compressed.byteLength };
  }

  async retrieve(path: string): Promise<BucketOperation[]> {
    const buffer = await this.storage.get(path);
    const decompressed = await zstd.decompress(buffer);
    const wrapper = bson.deserialize(decompressed, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS);
    return wrapper.ops;
  }

  async delete(paths: string[]): Promise<void> {
    return this.storage.delete(paths);
  }
}
