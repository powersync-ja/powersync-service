import { mongo } from '@powersync/lib-service-mongodb';
import { JSONBig } from '@powersync/service-jsonbig';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';

import { Logger, logger as defaultLogger } from '@powersync/lib-services-framework';
import { InternalOpId, storage, utils } from '@powersync/service-core';
import { currentBucketKey, EMPTY_DATA, MAX_ROW_SIZE } from './MongoBucketBatch.js';
import { MongoIdSequence } from './MongoIdSequence.js';
import { PowerSyncMongo } from './db.js';
import {
  BucketDataDocument,
  BucketParameterDocument,
  BucketStateDocument,
  CurrentBucket,
  CurrentDataDocument,
  SourceKey
} from './models.js';
import { replicaIdToSubkey } from '../../utils/util.js';

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
 * Keeps track of bulkwrite operations within a transaction.
 *
 * There may be multiple of these batches per transaction, but it may not span
 * multiple transactions.
 */
export class PersistedBatch {
  logger: Logger;
  bucketData: mongo.AnyBulkWriteOperation<BucketDataDocument>[] = [];
  bucketParameters: mongo.AnyBulkWriteOperation<BucketParameterDocument>[] = [];
  currentData: mongo.AnyBulkWriteOperation<CurrentDataDocument>[] = [];
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
    private group_id: number,
    writtenSize: number,
    options?: { logger?: Logger }
  ) {
    this.currentSize = writtenSize;
    this.logger = options?.logger ?? defaultLogger;
  }

  private incrementBucket(bucket: string, op_id: InternalOpId, bytes: number) {
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

  saveBucketData(options: {
    op_seq: MongoIdSequence;
    sourceKey: storage.ReplicaId;
    table: storage.SourceTable;
    evaluated: EvaluatedRow[];
    before_buckets: CurrentBucket[];
  }) {
    const remaining_buckets = new Map<string, CurrentBucket>();
    for (let b of options.before_buckets) {
      const key = currentBucketKey(b);
      remaining_buckets.set(key, b);
    }

    const dchecksum = BigInt(utils.hashDelete(replicaIdToSubkey(options.table.id, options.sourceKey)));

    for (const k of options.evaluated) {
      const key = currentBucketKey(k);

      // INSERT
      const recordData = JSONBig.stringify(k.data);
      const checksum = utils.hashData(k.table, k.id, recordData);
      if (recordData.length > MAX_ROW_SIZE) {
        // In many cases, the raw data size would have been too large already. But there are cases where
        // the BSON size is small enough, but the JSON size is too large.
        // In these cases, we can't store the data, so we skip it, or generate a REMOVE operation if the row
        // was synced previously.
        this.logger.error(`Row ${key} too large: ${recordData.length} bytes. Removing.`);
        continue;
      }

      remaining_buckets.delete(key);
      const byteEstimate = recordData.length + 200;
      this.currentSize += byteEstimate;

      const op_id = options.op_seq.next();
      this.debugLastOpId = op_id;

      this.bucketData.push({
        insertOne: {
          document: {
            _id: {
              g: this.group_id,
              b: k.bucket,
              o: op_id
            },
            op: 'PUT',
            source_table: options.table.id,
            source_key: options.sourceKey,
            table: k.table,
            row_id: k.id,
            checksum: BigInt(checksum),
            data: recordData
          }
        }
      });
      this.incrementBucket(k.bucket, op_id, byteEstimate);
    }

    for (let bd of remaining_buckets.values()) {
      // REMOVE

      const op_id = options.op_seq.next();
      this.debugLastOpId = op_id;

      this.bucketData.push({
        insertOne: {
          document: {
            _id: {
              g: this.group_id,
              b: bd.bucket,
              o: op_id
            },
            op: 'REMOVE',
            source_table: options.table.id,
            source_key: options.sourceKey,
            table: bd.table,
            row_id: bd.id,
            checksum: dchecksum,
            data: null
          }
        }
      });
      this.currentSize += 200;
      this.incrementBucket(bd.bucket, op_id, 200);
    }
  }

  saveParameterData(data: {
    op_seq: MongoIdSequence;
    sourceKey: storage.ReplicaId;
    sourceTable: storage.SourceTable;
    evaluated: EvaluatedParameters[];
    existing_lookups: bson.Binary[];
  }) {
    // This is similar to saving bucket data.
    // A key difference is that we don't need to keep the history intact.
    // We do need to keep track of recent history though - enough that we can get consistent data for any specific checkpoint.
    // Instead of storing per bucket id, we store per "lookup".
    // A key difference is that we don't need to store or keep track of anything per-bucket - the entire record is
    // either persisted or removed.
    // We also don't need to keep history intact.
    const { sourceTable, sourceKey, evaluated } = data;

    const remaining_lookups = new Map<string, bson.Binary>();
    for (let l of data.existing_lookups) {
      remaining_lookups.set(l.toString('base64'), l);
    }

    // 1. Insert new entries
    for (let result of evaluated) {
      const binLookup = storage.serializeLookup(result.lookup);
      const hex = binLookup.toString('base64');
      remaining_lookups.delete(hex);

      const op_id = data.op_seq.next();
      this.debugLastOpId = op_id;
      this.bucketParameters.push({
        insertOne: {
          document: {
            _id: op_id,
            key: {
              g: this.group_id,
              t: sourceTable.id,
              k: sourceKey
            },
            lookup: binLookup,
            bucket_parameters: result.bucketParameters
          }
        }
      });

      this.currentSize += 200;
    }

    // 2. "REMOVE" entries for any lookup not touched.
    for (let lookup of remaining_lookups.values()) {
      const op_id = data.op_seq.next();
      this.debugLastOpId = op_id;
      this.bucketParameters.push({
        insertOne: {
          document: {
            _id: op_id,
            key: {
              g: this.group_id,
              t: sourceTable.id,
              k: sourceKey
            },
            lookup: lookup,
            bucket_parameters: []
          }
        }
      });

      this.currentSize += 200;
    }
  }

  deleteCurrentData(id: SourceKey, checkpointGreaterThan: bigint) {
    const op: mongo.AnyBulkWriteOperation<CurrentDataDocument> = {
      updateOne: {
        filter: { _id: id },
        update: {
          $set: {
            data: EMPTY_DATA,
            buckets: [],
            lookups: [],
            pending_delete: checkpointGreaterThan
          }
        },
        upsert: true
      }
    };
    this.currentData.push(op);
    this.currentSize += 50;
  }

  upsertCurrentData(id: SourceKey, values: Partial<CurrentDataDocument>) {
    const op: mongo.AnyBulkWriteOperation<CurrentDataDocument> = {
      updateOne: {
        filter: { _id: id },
        update: {
          $set: values
        },
        upsert: true
      }
    };
    this.currentData.push(op);
    this.currentSize += (values.data?.length() ?? 0) + 100;
  }

  shouldFlushTransaction() {
    return (
      this.currentSize >= MAX_TRANSACTION_BATCH_SIZE ||
      this.bucketData.length >= MAX_TRANSACTION_DOC_COUNT ||
      this.currentData.length >= MAX_TRANSACTION_DOC_COUNT ||
      this.bucketParameters.length >= MAX_TRANSACTION_DOC_COUNT
    );
  }

  async flush(db: PowerSyncMongo, session: mongo.ClientSession, options?: storage.BucketBatchCommitOptions) {
    const startAt = performance.now();
    let flushedSomething = false;
    if (this.bucketData.length > 0) {
      flushedSomething = true;
      await db.bucket_data.bulkWrite(this.bucketData, {
        session,
        // inserts only - order doesn't matter
        ordered: false
      });
    }
    if (this.bucketParameters.length > 0) {
      flushedSomething = true;
      await db.bucket_parameters.bulkWrite(this.bucketParameters, {
        session,
        // inserts only - order doesn't matter
        ordered: false
      });
    }
    if (this.currentData.length > 0) {
      flushedSomething = true;
      await db.current_data.bulkWrite(this.currentData, {
        session,
        // may update and delete data within the same batch - order matters
        ordered: true
      });
    }

    if (this.bucketStates.size > 0) {
      flushedSomething = true;
      await db.bucket_state.bulkWrite(this.getBucketStateUpdates(), {
        session,
        // Per-bucket operation - order doesn't matter
        ordered: false
      });
    }

    if (flushedSomething) {
      const duration = Math.round(performance.now() - startAt);
      if (options?.oldestUncommittedChange != null) {
        const replicationLag = Math.round((Date.now() - options.oldestUncommittedChange.getTime()) / 1000);

        this.logger.info(
          `Flushed ${this.bucketData.length} + ${this.bucketParameters.length} + ${
            this.currentData.length
          } updates, ${Math.round(this.currentSize / 1024)}kb in ${duration}ms. Last op_id: ${this.debugLastOpId}. Replication lag: ${replicationLag}s`,
          {
            flushed: {
              duration: duration,
              size: this.currentSize,
              bucket_data_count: this.bucketData.length,
              parameter_data_count: this.bucketParameters.length,
              current_data_count: this.currentData.length,
              replication_lag_seconds: replicationLag
            }
          }
        );
      } else {
        this.logger.info(
          `Flushed ${this.bucketData.length} + ${this.bucketParameters.length} + ${
            this.currentData.length
          } updates, ${Math.round(this.currentSize / 1024)}kb in ${duration}ms. Last op_id: ${this.debugLastOpId}`,
          {
            flushed: {
              duration: duration,
              size: this.currentSize,
              bucket_data_count: this.bucketData.length,
              parameter_data_count: this.bucketParameters.length,
              current_data_count: this.currentData.length
            }
          }
        );
      }
    }

    this.bucketData = [];
    this.bucketParameters = [];
    this.currentData = [];
    this.bucketStates.clear();
    this.currentSize = 0;
    this.debugLastOpId = null;
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
