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
  private snapshotFilter: any;

  public constructor(options: {
    collection: mongo.Collection;
    batchSize: number;
    key?: Uint8Array | null;
    filter?: any;
  }) {
    this.lastKey = options.key ? bson.deserialize(options.key, { useBigInt64: true })._id : null;
    this.lastCursor = null;
    this.collection = options.collection;
    this.batchSize = options.batchSize;
    this.snapshotFilter = options.filter;
  }

  async nextChunk(): Promise<{ docs: mongo.Document[]; lastKey: Uint8Array } | { docs: []; lastKey: null }> {
    let cursor = this.lastCursor;
    let newCursor = false;
    if (cursor == null || cursor.closed) {
      // This is subtly but importantly different from doing { _id: { $gt: this.lastKey } }.
      // If there are separate BSON types of _id, then the form above only returns documents in the same type,
      // while the $expr form will return all documents with _id greater than the lastKey, matching sort order.
      // Both forms use indexes efficiently.
      // For details, see:
      // https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/#comparison-sort-order
      // The $literal is necessary to ensure that the lastKey is treated as a literal value, and doesn't attempt
      // any parsing as an operator.
      // Starting in MongoDB 5.0, this filter can use the _id index. Source:
      // https://www.mongodb.com/docs/manual/release-notes/5.0/#general-aggregation-improvements

      // Build base filter for _id
      const idFilter: mongo.Filter<mongo.Document> =
        this.lastKey == null ? {} : { $expr: { $gt: ['$_id', { $literal: this.lastKey }] } };

      // Combine with snapshot filter if present
      let filter: mongo.Filter<mongo.Document>;
      if (this.snapshotFilter) {
        if (this.lastKey == null) {
          filter = this.snapshotFilter;
        } else {
          filter = { $and: [idFilter, this.snapshotFilter] };
        }
      } else {
        filter = idFilter;
      }

      cursor = this.collection.find(filter, {
        readConcern: 'majority',
        limit: this.batchSize,
        // batchSize is 1 more than limit to auto-close the cursor.
        // See https://github.com/mongodb/node-mongodb-native/pull/4580
        batchSize: this.batchSize + 1,
        sort: { _id: 1 }
      });
      newCursor = true;
    }
    const hasNext = await cursor.hasNext();
    if (!hasNext) {
      this.lastCursor = null;
      if (newCursor) {
        // We just created a new cursor and it has no results - we have finished the end of the query.
        return { docs: [], lastKey: null };
      } else {
        // The cursor may have hit the batch limit - retry
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
