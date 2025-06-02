import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { bson } from '@powersync/service-core';

/**
 * Performs a collection snapshot query, chunking by ranges of _id.
 *
 * This may miss some rows if they are modified during the snapshot query.
 * In that case, the change stream replication will pick up those rows afterwards.
 */
export class ChunkedSnapshotQuery implements AsyncDisposable {
  lastKey: any = null;
  private lastCursor: mongo.FindCursor | null = null;
  private collection: mongo.Collection;
  private batchSize: number;

  public constructor(options: { collection: mongo.Collection; batchSize?: number; key?: Uint8Array | null }) {
    this.lastKey = options.key ? bson.deserialize(options.key, { useBigInt64: true })._id : null;
    this.lastCursor = null;
    this.collection = options.collection;
    this.batchSize = options.batchSize ?? 6_000;
  }

  async nextChunk(): Promise<{ docs: mongo.Document[]; lastKey: Uint8Array } | { docs: []; lastKey: null }> {
    let cursor = this.lastCursor;
    let newCursor = false;
    if (cursor == null || cursor.closed) {
      const filter: mongo.Filter<mongo.Document> = this.lastKey == null ? {} : { _id: { $gt: this.lastKey as any } };
      cursor = this.collection.find(filter, {
        batchSize: this.batchSize,
        readConcern: 'majority',
        limit: this.batchSize,
        sort: { _id: 1 }
      });
      newCursor = true;
    }
    const hasNext = await cursor.hasNext();
    if (!hasNext) {
      this.lastCursor = null;
      if (newCursor) {
        return { docs: [], lastKey: null };
      } else {
        return this.nextChunk();
      }
    }
    const docBatch = cursor.readBufferedDocuments();
    this.lastCursor = cursor;
    if (docBatch.length == 0) {
      throw new ReplicationAssertionError(`MongoDB snapshot query returned an empty batch, but hasNext() was true.`);
    }
    const lastKey = docBatch[docBatch.length - 1]._id;
    this.lastKey = lastKey;
    return { docs: docBatch, lastKey: bson.serialize({ _id: lastKey }) };
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.lastCursor?.close();
  }
}
