import { mongo } from '@powersync/lib-service-mongodb';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';

import { logger as defaultLogger, Logger } from '@powersync/lib-services-framework';
import { InternalOpId, storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { mongoTableId, replicaIdToSubkey } from '../../../utils/util.js';
import { BucketDefinitionId, BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import { currentBucketKey, MAX_ROW_SIZE } from '../MongoBucketBatchShared.js';
import { MongoIdSequence } from '../MongoIdSequence.js';
import type { VersionedPowerSyncMongo } from '../db.js';
import { TaggedBucketParameterDocument } from '../models.js';
import { BucketDataDoc, BucketKey } from './BucketDataDoc.js';
import { SourceRecordBucketState, SourceRecordLookupState } from './SourceRecordStore.js';

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

export interface SaveBucketDataOptions {
  op_seq: MongoIdSequence;
  sourceKey: storage.ReplicaId;
  table: storage.SourceTable;
  evaluated: EvaluatedRow[];
  before_buckets: SourceRecordBucketState[];
}

export interface SaveParameterDataOptions {
  op_seq: MongoIdSequence;
  sourceKey: storage.ReplicaId;
  sourceTable: storage.SourceTable;
  evaluated: EvaluatedParameters[];
  existing_lookups: SourceRecordLookupState[];
}

export interface UpsertCurrentDataOptions {
  sourceTableId: bson.ObjectId;
  replicaId: storage.ReplicaId;
  data: bson.Binary | null;
  buckets: SourceRecordBucketState[];
  lookups: SourceRecordLookupState[];
}

export interface PersistedBatchOptions {
  logger?: Logger;
}

/**
 * Keeps track of bulkwrite operations within a transaction.
 *
 * There may be multiple of these batches per transaction, but it may not span
 * multiple transactions.
 */
export abstract class PersistedBatch {
  logger: Logger;
  bucketData: BucketDataDoc[] = [];
  bucketParameters: TaggedBucketParameterDocument[] = [];
  bucketStates: Map<string, BucketStateUpdate> = new Map();

  /**
   * For debug logging only.
   */
  debugLastOpId: InternalOpId | null = null;

  /**
   * Very rough estimate of transaction size.
   */
  currentSize = 0;

  constructor(
    protected readonly db: VersionedPowerSyncMongo,
    protected readonly group_id: number,
    protected readonly mapping: BucketDefinitionMapping,
    writtenSize: number,
    options?: PersistedBatchOptions
  ) {
    this.currentSize = writtenSize;
    this.logger = options?.logger ?? defaultLogger;
  }

  saveBucketData(options: SaveBucketDataOptions) {
    const remaining_buckets = new Map<string, SaveBucketDataOptions['before_buckets'][number]>();
    for (let bucket of options.before_buckets) {
      remaining_buckets.set(currentBucketKey(bucket), bucket);
    }

    const dchecksum = BigInt(utils.hashDelete(replicaIdToSubkey(options.table.id, options.sourceKey)));

    for (const evaluated of options.evaluated) {
      const sourceDefinitionId = this.mapping.bucketSourceId(evaluated.source);
      const key = currentBucketKey({
        definitionId: sourceDefinitionId,
        bucket: evaluated.bucket,
        table: evaluated.table,
        id: evaluated.id
      });

      const recordData = JSONBig.stringify(evaluated.data);
      const checksum = utils.hashData(evaluated.table, evaluated.id, recordData);
      if (recordData.length > MAX_ROW_SIZE) {
        this.logger.error(`Row ${key} too large: ${recordData.length} bytes. Removing.`);
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
          definitionId: sourceDefinitionId,
          replicationStreamId: this.group_id
        },
        op_id,
        bucket: evaluated.bucket,
        sourceTableId: options.table.id,
        sourceKey: options.sourceKey,
        table: evaluated.table,
        rowId: evaluated.id,
        checksum: BigInt(checksum),
        data: recordData
      });
      this.incrementBucket(sourceDefinitionId, evaluated.bucket, op_id, byteEstimate);
    }

    for (let bucket of remaining_buckets.values()) {
      const definitionId = this.checkDefinitionId(bucket.definitionId);
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
        table: bucket.table,
        rowId: bucket.id,
        checksum: dchecksum
      });
      this.currentSize += 200;
      this.incrementBucket(definitionId, bucket.bucket, op_id, 200);
    }
  }

  abstract saveParameterData(data: SaveParameterDataOptions): void;

  abstract hardDeleteCurrentData(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): void;

  abstract softDeleteCurrentData(
    sourceTableId: bson.ObjectId,
    replicaId: storage.ReplicaId,
    checkpointGreaterThan: bigint
  ): void;

  abstract upsertCurrentData(values: UpsertCurrentDataOptions): void;

  protected abstract get currentDataCount(): number;

  protected abstract flushBucketData(session: mongo.ClientSession): Promise<void>;

  protected abstract flushBucketParameters(session: mongo.ClientSession): Promise<void>;

  protected abstract flushCurrentData(session: mongo.ClientSession): Promise<void>;

  protected abstract flushBucketStates(session: mongo.ClientSession): Promise<void>;

  protected abstract resetCurrentData(): void;

  protected abstract checkDefinitionId(definitionId: BucketDefinitionId | null): BucketDefinitionId;

  protected get bucketDataCount(): number {
    return this.bucketData.length;
  }

  protected incrementBucket(definitionId: BucketDefinitionId, bucket: string, op_id: InternalOpId, bytes: number) {
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
        incrementBytes: bytes
      });
    }
  }

  protected addBucketDataPut(options: {
    op_id: InternalOpId;
    bucketKey: BucketKey;
    bucket: string;
    sourceTableId: storage.SourceTable['id'];
    sourceKey: storage.ReplicaId;
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
      table: options.table,
      row_id: options.rowId,
      checksum: options.checksum,
      data: options.data
    });
  }

  protected addBucketDataRemove(options: {
    op_id: InternalOpId;
    bucketKey: BucketKey;
    sourceTableId: storage.SourceTable['id'];
    sourceKey: storage.ReplicaId;
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
      table: options.table,
      row_id: options.rowId,
      checksum: options.checksum,
      data: null
    });
  }

  shouldFlushTransaction() {
    return (
      this.currentSize >= MAX_TRANSACTION_BATCH_SIZE ||
      this.bucketDataCount >= MAX_TRANSACTION_DOC_COUNT ||
      this.currentDataCount >= MAX_TRANSACTION_DOC_COUNT ||
      this.bucketParameters.length >= MAX_TRANSACTION_DOC_COUNT
    );
  }

  async flush(session: mongo.ClientSession, options?: storage.BucketBatchCommitOptions) {
    const startAt = performance.now();
    let flushedSomething = false;
    if (this.bucketDataCount > 0) {
      flushedSomething = true;
      await this.flushBucketData(session);
    }
    if (this.bucketParameters.length > 0) {
      flushedSomething = true;
      await this.flushBucketParameters(session);
    }
    if (this.currentDataCount > 0) {
      flushedSomething = true;
      await this.flushCurrentData(session);
    }

    if (this.bucketStates.size > 0) {
      flushedSomething = true;
      await this.flushBucketStates(session);
    }

    if (flushedSomething) {
      const duration = Math.round(performance.now() - startAt);
      if (options?.oldestUncommittedChange != null) {
        const replicationLag = Math.round((Date.now() - options.oldestUncommittedChange.getTime()) / 1000);

        this.logger.info(
          `Flushed ${this.bucketDataCount} + ${this.bucketParameters.length} + ${
            this.currentDataCount
          } updates, ${Math.round(this.currentSize / 1024)}kb in ${duration}ms. Last op_id: ${this.debugLastOpId}. Replication lag: ${replicationLag}s`,
          {
            flushed: {
              duration: duration,
              size: this.currentSize,
              bucket_data_count: this.bucketDataCount,
              parameter_data_count: this.bucketParameters.length,
              current_data_count: this.currentDataCount,
              replication_lag_seconds: replicationLag
            }
          }
        );
      } else {
        this.logger.info(
          `Flushed ${this.bucketDataCount} + ${this.bucketParameters.length} + ${
            this.currentDataCount
          } updates, ${Math.round(this.currentSize / 1024)}kb in ${duration}ms. Last op_id: ${this.debugLastOpId}`,
          {
            flushed: {
              duration: duration,
              size: this.currentSize,
              bucket_data_count: this.bucketDataCount,
              parameter_data_count: this.bucketParameters.length,
              current_data_count: this.currentDataCount
            }
          }
        );
      }
    }

    const stats = {
      bucketDataCount: this.bucketDataCount,
      parameterDataCount: this.bucketParameters.length,
      currentDataCount: this.currentDataCount,
      flushedAny: flushedSomething
    };

    this.bucketData = [];
    this.bucketParameters = [];
    this.resetCurrentData();
    this.bucketStates.clear();
    this.currentSize = 0;
    this.debugLastOpId = null;

    return stats;
  }
}

export interface BucketStateUpdate {
  definitionId: BucketDefinitionId | null;
  bucket: string;
  lastOp: InternalOpId;
  incrementCount: number;
  incrementBytes: number;
}
