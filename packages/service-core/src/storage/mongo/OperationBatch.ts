import * as bson from 'bson';
import { ToastableSqliteRow } from '@powersync/service-sync-rules';

import * as storage_utils from './mongo-storage-utils.js';

import { SaveOptions } from '../BucketStorage.js';

/**
 * Maximum number of operations in a batch.
 */
const MAX_BATCH_COUNT = 2000;

/**
 * Maximum size of operations in the batch (estimated).
 */
const MAX_RECORD_BATCH_SIZE = 5_000_000;

/**
 * Maximum size of size of current_data documents we lookup at a time.
 */
const MAX_CURRENT_DATA_BATCH_SIZE = 16_000_000;

/**
 * Batch of input operations.
 *
 * We accumulate operations up to MAX_RECORD_BATCH_SIZE,
 * then further split into sub-batches if MAX_CURRENT_DATA_BATCH_SIZE is exceeded.
 */
export class OperationBatch {
  batch: RecordOperation[] = [];
  currentSize: number = 0;

  get length() {
    return this.batch.length;
  }

  push(op: RecordOperation) {
    this.batch.push(op);
    this.currentSize += op.estimatedSize;
  }

  shouldFlush() {
    return this.batch.length >= MAX_BATCH_COUNT || this.currentSize > MAX_RECORD_BATCH_SIZE;
  }

  *batched(sizes: Map<string, number>): Generator<RecordOperation[]> {
    let currentBatch: RecordOperation[] = [];
    let currentBatchSize = 0;
    for (let op of this.batch) {
      const key = op.internalBeforeKey;
      const size = sizes.get(key) ?? 0;
      if (currentBatchSize + size > MAX_CURRENT_DATA_BATCH_SIZE && currentBatch.length > 0) {
        yield currentBatch;
        currentBatch = [];
        currentBatchSize = 0;
      }
      currentBatchSize += size;
      currentBatch.push(op);
    }
    if (currentBatch.length > 0) {
      yield currentBatch;
    }
  }
}

export class RecordOperation {
  public readonly afterId: bson.UUID | null;
  public readonly beforeId: bson.UUID;
  public readonly internalBeforeKey: string;
  public readonly internalAfterKey: string | null;
  public readonly estimatedSize: number;

  constructor(public readonly record: SaveOptions) {
    const after = record.after;
    const afterId = after
      ? storage_utils.getUuidReplicaIdentityBson(after, record.sourceTable.replicaIdColumns!)
      : null;
    const beforeId = record.before
      ? storage_utils.getUuidReplicaIdentityBson(record.before, record.sourceTable.replicaIdColumns!)
      : afterId!;
    this.afterId = afterId;
    this.beforeId = beforeId;
    this.internalBeforeKey = cacheKey(record.sourceTable.id, beforeId);
    this.internalAfterKey = afterId ? cacheKey(record.sourceTable.id, afterId) : null;

    this.estimatedSize = estimateRowSize(record.before) + estimateRowSize(record.after);
  }
}

export function cacheKey(table: bson.ObjectId, id: bson.UUID) {
  return `${table.toHexString()}.${id.toHexString()}`;
}

/**
 * Estimate in-memory size of row.
 */
function estimateRowSize(record: ToastableSqliteRow | undefined) {
  if (record == null) {
    return 12;
  }
  let size = 0;
  for (let [key, value] of Object.entries(record)) {
    size += 12 + key.length;
    // number | string | null | bigint | Uint8Array
    if (value == null) {
      size += 4;
    } else if (typeof value == 'number') {
      size += 8;
    } else if (typeof value == 'bigint') {
      size += 8;
    } else if (typeof value == 'string') {
      size += value.length;
    } else if (value instanceof Uint8Array) {
      size += value.byteLength;
    }
  }
  return size;
}
