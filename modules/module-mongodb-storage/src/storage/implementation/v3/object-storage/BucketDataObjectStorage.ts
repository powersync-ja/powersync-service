import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketDataDocumentV3, BucketOperation } from '../models.js';
import { ObjectStorage, ObjectStorageOperationOptions } from './ObjectStorage.js';

export class BucketDataObjectStorage {
  constructor(private readonly storage: ObjectStorage) {}

  async store(
    path: string,
    ops: BucketOperation[],
    options?: ObjectStorageOperationOptions
  ): Promise<{ fileSize: number }> {
    const bsonBuffer = bson.serialize({ ops });
    await this.storage.put(path, bsonBuffer, { contentType: 'application/bson', contentEncoding: null }, options);
    return { fileSize: bsonBuffer.byteLength };
  }

  async retrieve(path: string, options: ObjectStorageOperationOptions): Promise<BucketOperation[]> {
    const { data, metadata } = await this.storage.get(path, { signal: options.signal });
    if (metadata.contentEncoding != null) {
      throw new Error(`Unexpected content encoding: ${metadata.contentEncoding}`);
    }
    if (metadata.contentType !== 'application/bson') {
      throw new Error(`Unexpected content type: ${metadata.contentType}`);
    }
    const wrapper = bson.deserialize(data, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS);
    return wrapper.ops;
  }

  async delete(paths: string[], options?: ObjectStorageOperationOptions): Promise<void> {
    return this.storage.delete(paths, options);
  }
}

/**
 * Load offloaded operations and patch them onto their MongoDB metadata documents.
 * The object-storage implementation is responsible for limiting request
 * concurrency across all callers.
 */
export async function hydrateBucketDataDocuments(
  documents: BucketDataDocumentV3[],
  objectStorage: ObjectStorage | undefined,
  options: ObjectStorageOperationOptions
): Promise<void> {
  if (!objectStorage) {
    return;
  }

  options.signal?.throwIfAborted();
  const store = new BucketDataObjectStorage(objectStorage);
  const storedDocuments = documents.filter((document) => document.storage_ref);
  await Promise.all(
    storedDocuments.map(async (document) => {
      document.ops = await store.retrieve(document.storage_ref!.path, { signal: options.signal });
    })
  );
}
