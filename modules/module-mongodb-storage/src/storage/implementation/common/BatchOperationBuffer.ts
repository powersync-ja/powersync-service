import { logger as defaultLogger, Logger } from '@powersync/lib-services-framework';
import { InternalOpId, storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { BucketDataSource, BucketDefinitionId } from '@powersync/service-sync-rules';
import { mongoTableId, replicaIdToSubkey } from '../../../utils/util.js';
import { currentBucketKey, MAX_ROW_SIZE } from '../MongoBucketBatchShared.js';
import { TaggedBucketParameterDocument } from '../models.js';
import { BucketDataDoc, BucketKey } from './BucketDataDoc.js';
import { BucketStateUpdate, SaveBucketDataOptions } from './PersistedBatch.js';
import { PublicationStats } from './PreparedPublication.js';
import { SourceRecordBucketState } from './SourceRecordStore.js';

/**
 * Maximum size of operations we write in a single transaction.
 *
 * It's tricky to find the exact limit, but from experience, over 100MB
 * can cause an error:
 * > transaction is too large and will not fit in the storage engine cache
 *
 * Additionally, unbounded size here can balloon our memory usage in some edge
 * cases.
 *
 * When we reach this threshold, we commit the transaction and start a new one.
 */
const MAX_TRANSACTION_BATCH_SIZE = 30_000_000;

/**
 * Limit number of documents to write in a single transaction.
 *
 * This has an effect on error message size in some cases.
 */
const MAX_TRANSACTION_DOC_COUNT = 2_000;

/**
 * Shared operation generation, size accounting and diagnostics. Each storage
 * version owns this buffer and decides when to write, retain or clear its data.
 * No transaction, sealing or payload-preparation state lives here.
 */
export class BatchOperationBuffer {
  bucketData: BucketDataDoc[] = [];
  bucketParameters: TaggedBucketParameterDocument[] = [];
  bucketStates = new Map<string, BucketStateUpdate>();

  /**
   * For debug logging only.
   */
  debugLastOpId: InternalOpId | null = null;

  /**
   * Very rough estimate of transaction size.
   */
  currentSize: number;

  constructor(
    private readonly group_id: number,
    writtenSize: number,
    private readonly definitions: {
      checkDefinitionId(id: BucketDefinitionId | null): BucketDefinitionId;
      getBucketDefinitionId(source: BucketDataSource): BucketDefinitionId;
    },
    private readonly logger: Logger = defaultLogger
  ) {
    this.currentSize = writtenSize;
  }

  saveBucketData(options: SaveBucketDataOptions) {
    const remaining_buckets = new Map<string, SourceRecordBucketState>();
    for (let bucket of options.before_buckets) {
      const mapped: SourceRecordBucketState = {
        bucket: bucket.bucket,
        definitionId: this.definitions.checkDefinitionId(bucket.definitionId),
        id: bucket.id,
        table: bucket.table
      };
      if (options.table.bucketDataSourceIds != null && !options.table.bucketDataSourceIds.has(mapped.definitionId!)) {
        // The bucket definition is not active anymore.
        // We don't cleanup these references upfront, but do need to ignore them after the definition is removed.
        continue;
      }
      remaining_buckets.set(currentBucketKey(mapped), mapped);
    }

    const subkey = this.persistedSubkey(options.table.id, options.sourceKey);
    const dchecksum = BigInt(utils.hashDelete(subkey));

    for (const evaluated of options.evaluated) {
      const definitionId = this.definitions.getBucketDefinitionId(evaluated.source);
      const key = currentBucketKey({
        definitionId: definitionId,
        bucket: evaluated.bucket,
        table: evaluated.table,
        id: evaluated.id
      });

      const recordData = JSONBig.stringify(evaluated.data);
      const checksum = utils.hashData(evaluated.table, evaluated.id, recordData);
      if (recordData.length > MAX_ROW_SIZE) {
        const message = `Row ${key} too large: ${recordData.length} bytes. Removing.`;
        this.logger.error(message);
        continue;
      }

      remaining_buckets.delete(key);
      const byteEstimate = recordData.length + 200;
      this.currentSize += byteEstimate;

      const op_id = options.op_seq.next();
      this.debugLastOpId = op_id;

      this.addBucketDataPut({
        bucketKey: {
          bucket: evaluated.bucket,
          definitionId: definitionId,
          replicationStreamId: this.group_id
        },
        op_id,
        bucket: evaluated.bucket,
        sourceTableId: options.table.id,
        sourceKey: options.sourceKey,
        subkey,
        table: evaluated.table,
        rowId: evaluated.id,
        checksum: BigInt(checksum),
        data: recordData
      });
      this.incrementBucket(definitionId, evaluated.bucket, op_id, byteEstimate);
    }

    for (let bucket of remaining_buckets.values()) {
      const definitionId = bucket.definitionId!;
      const op_id = options.op_seq.next();
      this.debugLastOpId = op_id;

      this.addBucketDataRemove({
        bucketKey: {
          replicationStreamId: this.group_id,
          definitionId,
          bucket: bucket.bucket
        },
        op_id,
        sourceTableId: options.table.id,
        sourceKey: options.sourceKey,
        subkey,
        table: bucket.table,
        rowId: bucket.id,
        checksum: dchecksum
      });
      this.currentSize += 200;
      this.incrementBucket(definitionId, bucket.bucket, op_id, 200);
    }
  }

  /**
   * New bucket operations persist protocol subkeys during replication. Older
   * V1 documents may not have one, so sync reads retain a fallback.
   */
  persistedSubkey(table: storage.SourceTableId, key: storage.ReplicaId): string {
    return replicaIdToSubkey(table, key);
  }

  get bucketDataCount(): number {
    return this.bucketData.length;
  }

  incrementBucket(definitionId: BucketDefinitionId, bucket: string, op_id: InternalOpId, bytes: number) {
    const key = `${definitionId ?? ''}:${bucket}`;
    let existingState = this.bucketStates.get(key);
    if (existingState) {
      existingState.lastOp = op_id;
      existingState.incrementCount += 1;
      existingState.incrementBytes += bytes;
    } else {
      this.bucketStates.set(key, {
        definitionId,
        bucket,
        lastOp: op_id,
        incrementCount: 1,
        incrementBytes: bytes,
        incrementChunks: 0
      });
    }
  }

  /**
   * V3 persists operations in chunks. Keep this separate from incrementBucket:
   * operation counts are known while evaluating rows, while chunk counts and
   * exact persisted bytes are only known after the writer has chunked a flush.
   */
  incrementBucketPersistedChunk(definitionId: BucketDefinitionId, bucket: string, bytes: number) {
    const key = `${definitionId ?? ''}:${bucket}`;
    const existingState = this.bucketStates.get(key);
    if (existingState != null) {
      existingState.incrementChunks += 1;
      existingState.incrementBytes += bytes;
    }
  }

  /**
   * V3's compactor calculates byte deltas from persisted chunk metadata.
   * Replace the writer's per-operation estimate with those exact sizes before
   * flushing the corresponding bucket state.
   */
  resetBucketPersistedBytes(definitionId: BucketDefinitionId, bucket: string) {
    const state = this.bucketStates.get(`${definitionId ?? ''}:${bucket}`);
    if (state != null) {
      state.incrementBytes = 0;
    }
  }

  addBucketDataPut(options: {
    op_id: InternalOpId;
    bucketKey: BucketKey;
    bucket: string;
    sourceTableId: storage.SourceTable['id'];
    sourceKey: storage.ReplicaId;
    subkey: string;
    table: string;
    rowId: string;
    checksum: bigint;
    data: string;
  }) {
    this.bucketData.push({
      bucketKey: options.bucketKey,
      o: options.op_id,
      op: 'PUT',
      source_table: mongoTableId(options.sourceTableId),
      source_key: options.sourceKey,
      subkey: options.subkey,
      table: options.table,
      row_id: options.rowId,
      checksum: options.checksum,
      data: options.data
    });
  }

  addBucketDataRemove(options: {
    op_id: InternalOpId;
    bucketKey: BucketKey;
    sourceTableId: storage.SourceTable['id'];
    sourceKey: storage.ReplicaId;
    subkey: string;
    table: string;
    rowId: string;
    checksum: bigint;
  }) {
    this.bucketData.push({
      bucketKey: options.bucketKey,
      o: options.op_id,
      op: 'REMOVE',
      source_table: mongoTableId(options.sourceTableId),
      source_key: options.sourceKey,
      subkey: options.subkey,
      table: options.table,
      row_id: options.rowId,
      checksum: options.checksum,
      data: null
    });
  }

  shouldFlushTransaction(currentDataCount: number) {
    return (
      this.currentSize >= MAX_TRANSACTION_BATCH_SIZE ||
      this.bucketDataCount >= MAX_TRANSACTION_DOC_COUNT ||
      currentDataCount >= MAX_TRANSACTION_DOC_COUNT ||
      this.bucketParameters.length >= MAX_TRANSACTION_DOC_COUNT
    );
  }

  /** Adopt one fully evaluated row; unfinished rows never mutate this buffer. */
  append(row: BatchOperationBuffer): void {
    // A single high-fanout row may exceed a normal batch target. Avoid the
    // JavaScript argument-count limit while retaining its operation order.
    for (const operation of row.bucketData) {
      this.bucketData.push(operation);
    }
    for (const parameter of row.bucketParameters) {
      this.bucketParameters.push(parameter);
    }
    for (const [key, state] of row.bucketStates) {
      const previous = this.bucketStates.get(key);
      this.bucketStates.set(key, {
        ...state,
        incrementCount: (previous?.incrementCount ?? 0) + state.incrementCount,
        incrementBytes: (previous?.incrementBytes ?? 0) + state.incrementBytes,
        incrementChunks: (previous?.incrementChunks ?? 0) + state.incrementChunks
      });
    }
    this.currentSize += row.currentSize;
    this.debugLastOpId = row.debugLastOpId ?? this.debugLastOpId;
  }

  clear(): void {
    this.bucketData = [];
    this.bucketParameters = [];
    this.bucketStates.clear();
    this.currentSize = 0;
    this.debugLastOpId = null;
  }

  logFlush(startAt: number, currentDataCount: number, options?: storage.BucketBatchCommitOptions): PublicationStats {
    const flushedSomething =
      this.bucketDataCount > 0 ||
      this.bucketParameters.length > 0 ||
      currentDataCount > 0 ||
      this.bucketStates.size > 0;
    if (flushedSomething) {
      const duration = Math.round(performance.now() - startAt);
      if (options?.oldestUncommittedChange != null) {
        const replicationLag = Math.round((Date.now() - options.oldestUncommittedChange.getTime()) / 1000);

        this.logger.info(
          `Flushed: ${this.bucketDataCount} ops, ${this.bucketParameters.length} index entries, ${
            currentDataCount
          } records. ${Math.round(this.currentSize / 1024)}kb in ${duration}ms. Last op_id: ${this.debugLastOpId}. Replication lag: ${replicationLag}s`,
          {
            flushed: {
              duration: duration,
              size: this.currentSize,
              bucket_ops_count: this.bucketDataCount,
              parameter_indexes_count: this.bucketParameters.length,
              source_records_count: currentDataCount,
              replication_lag_seconds: replicationLag
            }
          }
        );
      } else {
        this.logger.info(
          `Flushed: ${this.bucketDataCount} ops, ${this.bucketParameters.length} index entries, ${
            currentDataCount
          } records. ${Math.round(this.currentSize / 1024)}kb in ${duration}ms. Last op_id: ${this.debugLastOpId}`,
          {
            flushed: {
              duration: duration,
              size: this.currentSize,
              bucket_ops_count: this.bucketDataCount,
              parameter_indexes_count: this.bucketParameters.length,
              source_records_count: currentDataCount
            }
          }
        );
      }
    }

    const stats = {
      bucketDataCount: this.bucketDataCount,
      parameterDataCount: this.bucketParameters.length,
      currentDataCount: currentDataCount,
      flushedAny: flushedSomething
    };

    return stats;
  }
}
