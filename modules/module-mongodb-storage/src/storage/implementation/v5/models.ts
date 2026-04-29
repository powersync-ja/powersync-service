import { InternalOpId } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import {
  BucketDataKey,
  BucketParameterDocumentBase,
  BucketStateDocumentBase,
  CurrentBucket,
  OpType,
  ReplicaId,
  SourceTableDocument,
  SourceTableKey,
  TaggedBucketParameterDocument
} from '../models.js';

export interface CurrentBucketV5 extends CurrentBucket {
  def: BucketDefinitionId;
}

export interface RecordedLookupV5 {
  i: ParameterIndexId;
  l: bson.Binary;
}

export interface CurrentDataDocumentV5 {
  _id: ReplicaId;
  data: bson.Binary | null;
  buckets: CurrentBucketV5[];
  lookups: RecordedLookupV5[];
  /**
   * If set, this can be deleted, once there is a consistent checkpoint >= pending_delete.
   *
   * This must only be set if buckets = [], lookups = [].
   */
  pending_delete?: bigint;
}

export interface BucketParameterDocumentV5 extends BucketParameterDocumentBase<SourceTableKey> {}

export type BucketDataKeyV5 = BucketDataKey;

export interface BucketOperationV5 {
  o: bigint;
  op: OpType;
  source_table?: bson.ObjectId;
  source_key?: ReplicaId;
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

export function taggedBucketParameterDocumentToV5(document: TaggedBucketParameterDocument): BucketParameterDocumentV5 {
  const { index: _index, ...rest } = document;
  return rest as BucketParameterDocumentV5;
}

export interface SourceTableDocumentV5 extends SourceTableDocument {
  bucket_data_source_ids: BucketDefinitionId[];
  parameter_lookup_source_ids: ParameterIndexId[];
  latest_pending_delete?: InternalOpId | undefined;
}

export interface BucketStateDocumentV5 extends BucketStateDocumentBase {
  _id: BucketStateDocumentBase['_id'] & {
    d: BucketDefinitionId;
  };
}
