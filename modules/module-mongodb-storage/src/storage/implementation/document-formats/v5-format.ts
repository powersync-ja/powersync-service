import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { chunkBucketData } from '../bucket-operations/chunking.js';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import { BucketDataDocumentGeneric } from '../common/SingleBucketStore.js';
import { BucketDataKey, BucketDataProperties, OpType } from '../models.js';
import { BucketDataFormatAdapter } from './format-interface.js';

export type BucketDataKeyV5 = BucketDataKey;

export interface BucketOperationV5 {
  o: bigint;
  op: OpType;
  source_table?: import('bson').ObjectId;
  source_key?: import('../models.js').ReplicaId;
  table?: string;
  row_id?: string;
  checksum: bigint;
  data: string | null;
}

export interface BucketDataDocumentV5 {
  _id: BucketDataKeyV5;
  min_op: bigint;
  checksum: bigint;
  count: number;
  size: number;
  target_op?: bigint | null;
  ops: BucketOperationV5[];
}

export function serializeBucketDataV5(bucket: string, operations: BucketDataDoc[]): BucketDataDocumentV5 {
  const minOp = operations[0].o;
  const maxOp = operations[operations.length - 1].o;

  let totalChecksum = 0n;
  let totalSize = 0;
  let maxTargetOp: bigint | null = null;

  const ops: BucketOperationV5[] = operations.map((op) => {
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

export function* loadBucketDataDocumentV5(
  context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
  doc: BucketDataDocumentV5
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

export class V5FormatAdapter implements BucketDataFormatAdapter {
  serializeForBulkWrite(
    bucket: string,
    docs: BucketDataDoc[]
  ): mongo.AnyBulkWriteOperation<BucketDataDocumentGeneric>[] {
    const chunks = chunkBucketData(docs);
    return chunks.map((chunk) => ({
      insertOne: {
        document: serializeBucketDataV5(bucket, chunk) as unknown as BucketDataDocumentGeneric
      }
    }));
  }

  *loadDocument(
    context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
    rawDoc: unknown
  ): Generator<BucketDataDoc> {
    yield* loadBucketDataDocumentV5(context, rawDoc as BucketDataDocumentV5);
  }

  toPersistedDocument(bucketKey: BucketKey, source: Omit<BucketDataDoc, 'bucketKey'>): BucketDataDocumentGeneric {
    return serializeBucketDataV5(bucketKey.bucket, [{ bucketKey, ...source }]) as unknown as BucketDataDocumentGeneric;
  }

  fromPersistedDocument(bucketKey: BucketKey, doc: BucketDataDocumentGeneric): BucketDataDoc {
    const generator = loadBucketDataDocumentV5(bucketKey, doc as unknown as BucketDataDocumentV5);
    const first = generator.next();
    if (first.done) {
      throw new Error('Empty ops array in BucketDataDocumentV5');
    }
    return first.value;
  }

  fromPartialPersistedDocument<T extends keyof BucketDataProperties>(
    bucketKey: BucketKey,
    doc: Pick<BucketDataDocumentGeneric, '_id' | T>
  ): Pick<BucketDataDoc, 'bucketKey' | 'o' | T> {
    const document = doc as unknown as Pick<BucketDataDocumentV5, '_id' | 'ops'>;
    const op = document.ops?.[0];
    if (op == null) {
      // Fallback for partial documents without ops array
      const { _id, ...rest } = doc as any;
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
    filter: mongo.Filter<BucketDataDocumentGeneric>;
    cursorOptions: { limit?: number; batchSize?: number };
  } {
    const filter: mongo.Filter<BucketDataDocumentGeneric> = {
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
