import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { bson, InternalOpId } from '@powersync/service-core';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import { BucketDataDocumentV3, BucketOperation } from './models.js';

/**
 * Serialize a non-empty list of operations in strictly ascending `o` order.
 * Preserving that order establishes the {@link BucketDataDocumentV3} range
 * invariants used by metadata-only queries.
 */
export function serializeBucketData(
  bucket: string,
  operations: BucketDataDoc[],
  options?: { targetOp?: InternalOpId | null }
): BucketDataDocumentV3 {
  const minOp = operations[0].o;
  const maxOp = operations[operations.length - 1].o;

  let totalChecksum = 0n;
  let maxTargetOp: InternalOpId | null = options?.targetOp ?? null;
  let hasClearOp = false;

  const ops: BucketOperation[] = operations.map((op) => {
    totalChecksum += op.checksum;

    if (op.op == 'CLEAR') {
      hasClearOp = true;
    }

    return {
      o: op.o,
      op: op.op,
      source_table: op.source_table,
      source_key: op.source_key,
      subkey: op.subkey,
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
    ...(hasClearOp ? { has_clear_op: true as const } : {}),
    ops
  };
}

export function* loadBucketDataDocument(
  context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
  doc: BucketDataDocumentV3
): Generator<BucketDataDoc> {
  const { _id, ops } = doc;
  if (!ops) {
    throw new ServiceAssertionError(
      `Missing ops array on BucketDataDocumentV3 at _id.o=${_id.o}. Callers must patch doc.ops from S3 before calling this function.`
    );
  }
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
      subkey: op.subkey,
      table: op.table,
      row_id: op.row_id,
      checksum: op.checksum,
      data: op.data
    };
  }
}

export function maxOpId(a: InternalOpId | null | undefined, b: InternalOpId | null | undefined): InternalOpId | null {
  if (a == null) {
    return b ?? null;
  }
  if (b == null) {
    return a ?? null;
  }
  return a > b ? a : b;
}
