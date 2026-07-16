import { bson } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import { BucketDataDocumentV3, BucketOperation } from './models.js';

export function serializeBucketData(bucket: string, operations: BucketDataDoc[]): BucketDataDocumentV3 {
  const minOp = operations[0].o;
  const maxOp = operations[operations.length - 1].o;

  let totalChecksum = 0n;
  let maxTargetOp: bigint | null = null;

  const ops: BucketOperation[] = operations.map((op) => {
    totalChecksum += op.checksum;

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

  const size = bson.calculateObjectSize(ops);

  return {
    _id: {
      b: bucket,
      o: maxOp
    },
    min_op: minOp,
    checksum: totalChecksum,
    count: operations.length,
    size,
    target_op: maxTargetOp,
    ops
  };
}

export function* loadBucketDataDocument(
  context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
  doc: BucketDataDocumentV3
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
      target_op: doc.target_op ?? null
    };
  }
}
