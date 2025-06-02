import { mongo } from '@powersync/lib-service-mongodb';
import { bson } from '@powersync/service-core';

export interface MongoSnapshotQuery extends AsyncDisposable {
  /**
   * Returns an chunk of documents.
   *
   * If the last chunk has 0 rows, it indicates that there are no more rows to fetch.
   */
  nextChunk(): Promise<mongo.Document[]>;
}

/**
 * Snapshot query using a find.
 */
export class SimpleSnapshotQuery implements MongoSnapshotQuery, AsyncDisposable {
  private cursor: mongo.FindCursor;

  public constructor(collection: mongo.Collection, batchSize: number = 6_000) {
    this.cursor = collection.find({}, { batchSize: batchSize, readConcern: 'majority' });
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.cursor.close();
  }

  async nextChunk(): Promise<mongo.Document[]> {
    // hasNext() is the call that triggers fetching of the next batch,
    // then we read it with readBufferedDocuments(). This gives us semi-explicit
    // control over the fetching of each batch, and avoids a separate promise per document
    const hasNext = await this.cursor.hasNext();
    if (!hasNext) {
      return [];
    }
    const docBatch = this.cursor.readBufferedDocuments();
    return docBatch;
  }
}

/**
 * Performs a table snapshot query, chunking by ranges of primary key data.
 *
 * This may miss some rows if they are modified during the snapshot query.
 * In that case, logical replication will pick up those rows afterwards,
 * possibly resulting in an IdSnapshotQuery.
 *
 * Currently, this only supports a table with a single primary key column,
 * of a select few types.
 */
export class ChunkedSnapshotQuery implements MongoSnapshotQuery, AsyncDisposable {
  lastKey: mongo.Document | null = null;
  private lastCursor: mongo.FindCursor | null = null;

  public constructor(
    private readonly collection: mongo.Collection,
    private readonly batchSize: number = 6_000
  ) {}

  public setLastKeySerialized(key: Uint8Array) {
    const decoded = bson.deserialize(key, { useBigInt64: true });
    this.lastKey = decoded;
  }

  public getLastKeySerialized(): Uint8Array | null {
    if (this.lastKey == null) {
      return null;
    }
    return bson.serialize(this.lastKey);
  }

  async nextChunk(): Promise<mongo.Document[]> {
    let cursor = this.lastCursor;
    if (cursor == null || cursor.closed) {
      const filter: mongo.Filter<mongo.Document> = this.lastKey == null ? {} : { _id: { $gt: this.lastKey as any } };
      cursor = this.collection.find(filter, {
        batchSize: this.batchSize,
        readConcern: 'majority',
        limit: this.batchSize,
        sort: { _id: 1 }
      });
    }
    const hasNext = await cursor.hasNext();
    if (!hasNext) {
      this.lastCursor = null;
      return [];
    }
    const docBatch = cursor.readBufferedDocuments();
    this.lastCursor = cursor;
    return docBatch;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.lastCursor?.close();
  }
}
