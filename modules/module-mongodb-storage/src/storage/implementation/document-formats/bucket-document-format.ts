import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { chunkBucketData } from '../bucket-operations/chunking.js';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import { BucketDataKey, BucketDataProperties, OpType } from '../models.js';

export interface BucketOperation {
  o: bigint;
  op: OpType;
  source_table?: import('bson').ObjectId;
  source_key?: import('../models.js').ReplicaId;
  table?: string;
  row_id?: string;
  checksum: bigint;
  data: string | null;
}

export interface BucketDataDocument {
  _id: BucketDataKey;
  min_op: bigint;
  checksum: bigint;
  count: number;
  size: number;
  target_op?: bigint | null;
  ops: BucketOperation[];
}

export function serializeBucketData(bucket: string, operations: BucketDataDoc[]): BucketDataDocument {
  const minOp = operations[0].o;
  const maxOp = operations[operations.length - 1].o;

  let totalChecksum = 0n;
  let totalSize = 0;
  let maxTargetOp: bigint | null = null;

  const ops: BucketOperation[] = operations.map((op) => {
    totalChecksum += op.checksum;
    totalSize += op.data?.length ?? 0;

    if (op.target_op != null && (maxTargetOp == null || op.target_op > maxTargetOp)) {
      maxTargetOp = op.target_op;
    }

    return {
      o: op.o,
      op: op.op,
      source_table: op.source_table,
      source_key: op.source_key,
      table: op.table,
      row_id: op.row_id,
      checksum: op.checksum,
      data: op.data
    };
  });

  return {
    _id: {
      b: bucket,
      o: maxOp
    },
    min_op: minOp,
    checksum: totalChecksum,
    count: operations.length,
    size: totalSize,
    target_op: maxTargetOp,
    ops
  };
}

export function* loadBucketDataDocument(
  context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
  doc: BucketDataDocument
): Generator<BucketDataDoc> {
  const { _id, ops } = doc;
  const bucketKey = {
    ...context,
    bucket: _id.b
  };

  for (const op of ops) {
    yield {
      bucketKey,
      o: op.o,
      op: op.op,
      source_table: op.source_table,
      source_key: op.source_key,
      table: op.table,
      row_id: op.row_id,
      checksum: op.checksum,
      data: op.data,
      target_op: null
    };
  }
}

/**
 * Validates that a partial persisted document has the expected `_id.o` shape.
 * Used when the ops array is missing from a partial persisted document.
 */
function extractPartialDocumentFields(doc: unknown): { _id: { o: bigint }; [key: string]: unknown } {
  if (typeof doc !== 'object' || doc === null) {
    throw new Error('Invalid partial document: expected object');
  }
  const d = doc as Record<string, unknown>;
  const id = d._id;
  if (typeof id !== 'object' || id === null || !('o' in id)) {
    throw new Error('Invalid partial document: missing _id.o');
  }
  return d as { _id: { o: bigint }; [key: string]: unknown };
}

export class BucketDocumentFormatAdapter {
  serializeForBulkWrite(bucket: string, docs: BucketDataDoc[]): mongo.AnyBulkWriteOperation<BucketDataDocument>[] {
    const chunks = chunkBucketData(docs);
    return chunks.map((chunk) => ({
      insertOne: {
        document: serializeBucketData(bucket, chunk)
      }
    }));
  }

  *loadDocument(
    context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
    rawDoc: unknown
  ): Generator<BucketDataDoc> {
    yield* loadBucketDataDocument(context, rawDoc as BucketDataDocument);
  }

  toPersistedDocument(bucketKey: BucketKey, source: Omit<BucketDataDoc, 'bucketKey'>): BucketDataDocument {
    return serializeBucketData(bucketKey.bucket, [{ bucketKey, ...source }]);
  }

  fromPersistedDocument(bucketKey: BucketKey, doc: BucketDataDocument): BucketDataDoc {
    const generator = loadBucketDataDocument(bucketKey, doc);
    const first = generator.next();
    if (first.done) {
      throw new Error('Empty ops array in BucketDataDocument');
    }
    return first.value;
  }

  /**
   * Convert a partial persisted document (e.g., from an aggregation pipeline)
   * to a partial in-memory op.
   *
   * Fallback branch: when the `ops` array is missing (old documents or
   * partial projections that don't include the field), we pull fields directly
   * from the document. A runtime validation helper extracts `_id.o` because
   * `Pick<BucketDataDocument, '_id' | T>` doesn't give us access to
   * the underlying fields at compile time.
   */
  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    bucketKey: BucketKey,
    doc: Pick<BucketDataDocument & BucketDataProperties, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T> {
    // We know the concrete type is BucketDataDocument, but Pick prevents a direct cast.
    const document = doc as unknown as Pick<BucketDataDocument, '_id' | 'ops'>;
    const op = document.ops?.[0];
    if (op == null) {
      // Fallback for old documents or partial projections without ops field.
      const fields = extractPartialDocumentFields(doc);
      const { _id, ...rest } = fields;
      return {
        bucketKey,
        o: _id.o,
        ...rest
      } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
    }
    return {
      bucketKey,
      ...op
    } as Pick<BucketDataDoc, 'bucketKey' | 'o' | T>;
  }

  buildBucketDataQuery(options: { startOpId?: InternalOpId; endOpId: InternalOpId; remainingLimit: number }): {
    filter: mongo.Filter<BucketDataDocument>;
    cursorOptions: { limit?: number; batchSize?: number };
  } {
    // MongoDB Filter<T> doesn't accept dotted field paths like '_id.o' in its type,
    // so we need an explicit cast for the range filter on the nested op_id.
    const filter: mongo.Filter<BucketDataDocument> = {
      '_id.o': {
        $gt: options.startOpId,
        $lte: options.endOpId
      }
    } as any;

    return {
      filter,
      cursorOptions: {}
    };
  }
}
