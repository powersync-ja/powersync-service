import { mongo } from '@powersync/lib-service-mongodb';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';

import { Logger, logger as defaultLogger } from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import { MongoIdSequence } from './MongoIdSequence.js';
import { VersionedPowerSyncMongo } from './db.js';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';
import {
  BucketDataDocument,
  BucketStateDocument,
  CommonBucketParameterDocument,
  CommonCurrentBucket,
  CommonCurrentLookup,
  SourceKey
} from './models.js';
import { mongoTableId } from '../../utils/util.js';

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
  before_buckets: CommonCurrentBucket[];
}

export interface SaveParameterDataOptions {
  op_seq: MongoIdSequence;
  sourceKey: storage.ReplicaId;
  sourceTable: storage.SourceTable;
  evaluated: EvaluatedParameters[];
  existing_lookups: CommonCurrentLookup[];
}

export interface UpsertCurrentDataOptions {
  id: SourceKey;
  data: bson.Binary | undefined;
  buckets: CommonCurrentBucket[];
  lookups: CommonCurrentLookup[];
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
  bucketData: mongo.AnyBulkWriteOperation<BucketDataDocument>[] = [];
  bucketParameters: mongo.AnyBulkWriteOperation<CommonBucketParameterDocument>[] = [];
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

  abstract saveBucketData(options: SaveBucketDataOptions): void;

  abstract saveParameterData(data: SaveParameterDataOptions): void;

  abstract hardDeleteCurrentData(id: SourceKey): void;

  abstract softDeleteCurrentData(id: SourceKey, checkpointGreaterThan: bigint): void;

  abstract upsertCurrentData(values: UpsertCurrentDataOptions): void;

  protected abstract get currentDataCount(): number;

  protected abstract flushCurrentData(session: mongo.ClientSession): Promise<void>;

  protected abstract resetCurrentData(): void;

  protected incrementBucket(bucket: string, op_id: InternalOpId, bytes: number) {
    let existingState = this.bucketStates.get(bucket);
    if (existingState) {
      existingState.lastOp = op_id;
      existingState.incrementCount += 1;
      existingState.incrementBytes += bytes;
    } else {
      this.bucketStates.set(bucket, {
        lastOp: op_id,
        incrementCount: 1,
        incrementBytes: bytes
      });
    }
  }

  protected addBucketDataPut(options: {
    op_id: InternalOpId;
    bucket: string;
    sourceTableId: storage.SourceTable['id'];
    sourceKey: storage.ReplicaId;
    table: string;
    rowId: string;
    checksum: bigint;
    data: string;
  }) {
    this.bucketData.push({
      insertOne: {
        document: {
          _id: {
            g: this.group_id,
            b: options.bucket,
            o: options.op_id
          },
          op: 'PUT',
          source_table: mongoTableId(options.sourceTableId),
          source_key: options.sourceKey,
          table: options.table,
          row_id: options.rowId,
          checksum: options.checksum,
          data: options.data
        }
      }
    });
  }

  protected addBucketDataRemove(options: {
    op_id: InternalOpId;
    bucket: string;
    sourceTableId: storage.SourceTable['id'];
    sourceKey: storage.ReplicaId;
    table: string;
    rowId: string;
    checksum: bigint;
  }) {
    this.bucketData.push({
      insertOne: {
        document: {
          _id: {
            g: this.group_id,
            b: options.bucket,
            o: options.op_id
          },
          op: 'REMOVE',
          source_table: mongoTableId(options.sourceTableId),
          source_key: options.sourceKey,
          table: options.table,
          row_id: options.rowId,
          checksum: options.checksum,
          data: null
        }
      }
    });
  }

  protected flushBucketParameters() {
    return this.bucketParameters.length > 0;
  }

  shouldFlushTransaction() {
    return (
      this.currentSize >= MAX_TRANSACTION_BATCH_SIZE ||
      this.bucketData.length >= MAX_TRANSACTION_DOC_COUNT ||
      this.currentDataCount >= MAX_TRANSACTION_DOC_COUNT ||
      this.bucketParameters.length >= MAX_TRANSACTION_DOC_COUNT
    );
  }

  async flush(session: mongo.ClientSession, options?: storage.BucketBatchCommitOptions) {
    const db = this.db;
    const startAt = performance.now();
    let flushedSomething = false;
    if (this.bucketData.length > 0) {
      flushedSomething = true;
      await db.bucket_data.bulkWrite(this.bucketData, {
        session,
        ordered: false
      });
    }
    if (this.flushBucketParameters()) {
      flushedSomething = true;
      await db.bucket_parameters.bulkWrite(this.bucketParameters, {
        session,
        ordered: false
      });
    }
    if (this.currentDataCount > 0) {
      flushedSomething = true;
      await this.flushCurrentData(session);
    }

    if (this.bucketStates.size > 0) {
      flushedSomething = true;
      await db.bucket_state.bulkWrite(this.getBucketStateUpdates(), {
        session,
        ordered: false
      });
    }

    if (flushedSomething) {
      const duration = Math.round(performance.now() - startAt);
      if (options?.oldestUncommittedChange != null) {
        const replicationLag = Math.round((Date.now() - options.oldestUncommittedChange.getTime()) / 1000);

        this.logger.info(
          `Flushed ${this.bucketData.length} + ${this.bucketParameters.length} + ${
            this.currentDataCount
          } updates, ${Math.round(this.currentSize / 1024)}kb in ${duration}ms. Last op_id: ${this.debugLastOpId}. Replication lag: ${replicationLag}s`,
          {
            flushed: {
              duration: duration,
              size: this.currentSize,
              bucket_data_count: this.bucketData.length,
              parameter_data_count: this.bucketParameters.length,
              current_data_count: this.currentDataCount,
              replication_lag_seconds: replicationLag
            }
          }
        );
      } else {
        this.logger.info(
          `Flushed ${this.bucketData.length} + ${this.bucketParameters.length} + ${
            this.currentDataCount
          } updates, ${Math.round(this.currentSize / 1024)}kb in ${duration}ms. Last op_id: ${this.debugLastOpId}`,
          {
            flushed: {
              duration: duration,
              size: this.currentSize,
              bucket_data_count: this.bucketData.length,
              parameter_data_count: this.bucketParameters.length,
              current_data_count: this.currentDataCount
            }
          }
        );
      }
    }

    const stats = {
      bucketDataCount: this.bucketData.length,
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

  private getBucketStateUpdates(): mongo.AnyBulkWriteOperation<BucketStateDocument>[] {
    return Array.from(this.bucketStates.entries()).map(([bucket, state]) => {
      return {
        updateOne: {
          filter: {
            _id: {
              g: this.group_id,
              b: bucket
            }
          },
          update: {
            $set: {
              last_op: state.lastOp
            },
            $inc: {
              'estimate_since_compact.count': state.incrementCount,
              'estimate_since_compact.bytes': state.incrementBytes
            }
          },
          upsert: true
        }
      } satisfies mongo.AnyBulkWriteOperation<BucketStateDocument>;
    });
  }
}

interface BucketStateUpdate {
  lastOp: InternalOpId;
  incrementCount: number;
  incrementBytes: number;
}
