import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { bson } from '@powersync/service-core';
import { getCursorBatchBytes } from './internal-mongodb-utils.js';

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

  public constructor(options: { collection: mongo.Collection; batchSize: number; key?: Uint8Array | null }) {
    this.lastKey = options.key ? bson.deserialize(options.key, { useBigInt64: true })._id : null;
    this.lastCursor = null;
    this.collection = options.collection;
    this.batchSize = options.batchSize;
  }

  async nextChunk(): Promise<
    { docs: Buffer[]; lastKey: Uint8Array; bytes: number } | { docs: []; lastKey: null; bytes: 0 }
  > {
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
      const filter: mongo.Filter<mongo.Document> =
        this.lastKey == null ? {} : { $expr: { $gt: ['$_id', { $literal: this.lastKey }] } };
      cursor = this.collection.find(filter, {
        readConcern: 'majority',
        limit: this.batchSize,
        // batchSize is 1 more than limit to auto-close the cursor.
        // See https://github.com/mongodb/node-mongodb-native/pull/4580
        batchSize: this.batchSize + 1,
        sort: { _id: 1 },
        raw: true
      });
      newCursor = true;
    }
    const hasNext = await cursor.hasNext();
    if (!hasNext) {
      this.lastCursor = null;
      if (newCursor) {
        // We just created a new cursor and it has no results - we have finished the end of the query.
        return { docs: [], lastKey: null, bytes: 0 };
      } else {
        // The cursor may have hit the batch limit - retry
        return this.nextChunk();
      }
    }
    const bytes = getCursorBatchBytes(cursor);
    const docBatch = cursor.readBufferedDocuments() as Buffer[];
    this.lastCursor = cursor;
    if (docBatch.length == 0) {
      throw new ReplicationAssertionError(`MongoDB snapshot query returned an empty batch, but hasNext() was true.`);
    }
    const lastDoc = docBatch[docBatch.length - 1];
    const { id: lastKey, idBuffer } = parseDocumentId(lastDoc);
    this.lastKey = lastKey;
    return { docs: docBatch, lastKey: idBuffer, bytes };
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.lastCursor?.close();
  }
}

const idKey = Buffer.from('_id');

/**
 * Parse an _id from a buffer, without parsing the entire document.
 *
 * @returns the parsed id, as well as a serialized document including only _id.
 */
export function parseDocumentId(buffer: Buffer): { id: any; idBuffer: Buffer } {
  // Use the "experimental" onDemand API to parse the _id without fully parsing
  // the entire document.
  // For reference, see how MongoDB uses the onDemand API here:
  // https://github.com/mongodb/node-mongodb-native/blob/f36b7546e937d980cc7decb760eb8f561334fa6a/src/cmap/wire_protocol/on_demand/document.ts#L54
  // This still performs more work than strictly required, since the onDemand API iterates through every key in the document.
  // We may replace this with a custom BSON parser later.

  // Note: The current js-bson implementation always returns an actual pre-poplated array.
  // That does not affect us directly (other than performance), since we're always only iterating through it once.
  const elementsIterable = mongo.BSON.onDemand.parseToElements(buffer);
  for (let [_type, nameOffset, nameLength, offset, length] of elementsIterable) {
    // Loop until we find the _id key
    if (nameLength != 3) {
      continue;
    }
    if (!buffer.subarray(nameOffset, nameOffset + nameLength).equals(idKey)) {
      continue;
    }

    // We create a new "document" containing only the _id, by directly manipulating buffers.
    // https://bsonspec.org/spec.html
    // document	::=	int32 e_list unsigned_byte(0)
    // e_list	::=	element e_list
    // element ::= signed_byte e_name ...

    // Subtract 1 to start at the type flag (signed_byte)
    const baseOffset = nameOffset - 1;
    // element ends at offset + length
    const baseLength = offset + length - baseOffset;
    // Our buffer wraps the _id element: 4 bytes before for the size, 1 null byte at the end.
    const genBuffer = Buffer.allocUnsafe(baseLength + 5);
    genBuffer.writeInt32LE(baseLength + 5, 0);
    buffer.copy(genBuffer, 4, baseOffset, baseOffset + baseLength);
    genBuffer[genBuffer.length - 1] = 0;
    return { idBuffer: genBuffer, id: bson.deserialize(genBuffer, { useBigInt64: true })._id };
  }

  throw new ReplicationAssertionError(`Document without _id`);
}
