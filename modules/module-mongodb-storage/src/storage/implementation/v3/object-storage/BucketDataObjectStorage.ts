import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketDataDocumentV3, BucketOperation } from '../models.js';
import { ObjectStorage } from './ObjectStorage.js';

export class BucketDataObjectStorage {
  constructor(private readonly storage: ObjectStorage) {}

  async store(path: string, ops: BucketOperation[]): Promise<{ fileSize: number }> {
    const bsonBuffer = bson.serialize({ ops });
    await this.storage.put(path, bsonBuffer, { contentType: 'application/bson', contentEncoding: null });
    return { fileSize: bsonBuffer.byteLength };
  }

  async retrieve(path: string, signal?: AbortSignal): Promise<BucketOperation[]> {
    const { data, metadata } = await this.storage.get(path, signal);
    if (metadata.contentEncoding != null) {
      throw new Error(`Unexpected content encoding: ${metadata.contentEncoding}`);
    }
    if (metadata.contentType !== 'application/bson') {
      throw new Error(`Unexpected content type: ${metadata.contentType}`);
    }
    const wrapper = bson.deserialize(data, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS);
    return wrapper.ops;
  }

  async delete(paths: string[]): Promise<void> {
    return this.storage.delete(paths);
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
  signal?: AbortSignal
): Promise<void> {
  if (!objectStorage) {
    return;
  }

  const store = new BucketDataObjectStorage(objectStorage);
  const storedDocuments = documents.filter((document) => document.storage_ref);
  await Promise.all(
    storedDocuments.map(async (document) => {
      signal?.throwIfAborted();
      document.ops = await store.retrieve(document.storage_ref!.path, signal);
    })
  );
}
