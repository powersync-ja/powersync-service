import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketDataDocumentV3, BucketOperation } from '../models.js';
import { ObjectStorage } from './ObjectStorage.js';

const S3_DOWNLOAD_CONCURRENCY = 16;

export class BucketDataObjectStorage {
  constructor(private readonly storage: ObjectStorage) {}

  async store(path: string, ops: BucketOperation[]): Promise<{ fileSize: number }> {
    const bsonBuffer = bson.serialize({ ops });
    await this.storage.put(path, bsonBuffer, { contentType: 'application/bson', contentEncoding: null });
    return { fileSize: bsonBuffer.byteLength };
  }

  async retrieve(path: string): Promise<BucketOperation[]> {
    const { data, metadata } = await this.storage.get(path);
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
 *
 * A shared index distributes documents across a fixed number of async workers.
 * JavaScript runs each index increment synchronously before the worker awaits,
 * so every document is claimed exactly once. This caps active S3 requests at
 * {@link S3_DOWNLOAD_CONCURRENCY} without allocating one pending promise per
 * object in a large batch.
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
  let nextDocument = 0;

  const downloadNext = async () => {
    while (nextDocument < storedDocuments.length) {
      signal?.throwIfAborted();
      const document = storedDocuments[nextDocument++];
      document.ops = await store.retrieve(document.storage_ref!.path);
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(S3_DOWNLOAD_CONCURRENCY, storedDocuments.length) }, () => downloadNext())
  );
}
