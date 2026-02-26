/**
 * TODO share this implementation better in the core package.
 * There are some subtle differences in this implementation.
 */

import { storage, utils } from '@powersync/service-core';
import { RequiredOperationBatchLimits } from '../../types/types.js';
import { postgresTableId } from '../table-id.js';

/**
 * Batch of input operations.
 *
 * We accumulate operations up to MAX_RECORD_BATCH_SIZE,
 * then further split into sub-batches if MAX_CURRENT_DATA_BATCH_SIZE is exceeded.
 */
export class OperationBatch {
  batch: RecordOperation[] = [];
  currentSize: number = 0;

  readonly maxBatchCount: number;
  readonly maxRecordSize: number;
  readonly maxCurrentDataBatchSize: number;

  get length() {
    return this.batch.length;
  }

  constructor(protected options: RequiredOperationBatchLimits) {
    this.maxBatchCount = options.max_record_count;
    this.maxRecordSize = options.max_estimated_size;
    this.maxCurrentDataBatchSize = options.max_current_data_batch_size;
  }

  push(op: RecordOperation) {
    this.batch.push(op);
    this.currentSize += op.estimatedSize;
  }

  shouldFlush() {
    return this.batch.length >= this.maxBatchCount || this.currentSize > this.maxCurrentDataBatchSize;
  }

  /**
   *
   * @param sizes Map of source key to estimated size of the current_data document, or undefined if current_data is not persisted.
   *
   */
  *batched(sizes: Map<string, number> | undefined): Generator<RecordOperation[]> {
    if (sizes == null) {
      yield this.batch;
      return;
    }
    let currentBatch: RecordOperation[] = [];
    let currentBatchSize = 0;
    for (let op of this.batch) {
      const key = op.internalBeforeKey;
      const size = sizes.get(key) ?? 0;
      if (currentBatchSize + size > this.maxCurrentDataBatchSize && currentBatch.length > 0) {
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
  public readonly afterId: storage.ReplicaId | null;
  public readonly beforeId: storage.ReplicaId;
  public readonly internalBeforeKey: string;
  public readonly internalAfterKey: string | null;
  public readonly estimatedSize: number;

  constructor(public readonly record: storage.SaveOptions) {
    const afterId = record.afterReplicaId ?? null;
    const beforeId = record.beforeReplicaId ?? record.afterReplicaId;
    this.afterId = afterId;
    this.beforeId = beforeId;
    this.internalBeforeKey = cacheKey(record.sourceTable.id, beforeId);
    this.internalAfterKey = afterId ? cacheKey(record.sourceTable.id, afterId) : null;
    this.estimatedSize = utils.estimateRowSize(record.before) + utils.estimateRowSize(record.after);
  }
}

/**
 * In-memory cache key - must not be persisted.
 */
export function cacheKey(sourceTableId: storage.SourceTableId, id: storage.ReplicaId) {
  return encodedCacheKey(sourceTableId, storage.serializeReplicaId(id));
}

/**
 * Calculates a cache key for a stored ReplicaId. This is usually stored as a bytea/Buffer.
 */
export function encodedCacheKey(sourceTableId: storage.SourceTableId, storedKey: Buffer) {
  return `${postgresTableId(sourceTableId)}.${storedKey.toString('base64')}`;
}
