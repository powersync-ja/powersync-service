import { JSONBig } from '@powersync/service-jsonbig';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import * as mongo from 'mongodb';

import * as util from '../../util/util-index.js';
import { SourceTable } from '../SourceTable.js';
import { currentBucketKey } from './MongoBucketBatch.js';
import { MongoIdSequence } from './MongoIdSequence.js';
import { PowerSyncMongo } from './db.js';
import {
  BucketDataDocument,
  BucketParameterDocument,
  CurrentBucket,
  CurrentDataDocument,
  SourceKey,
  ReplicaId
} from './models.js';
import { replicaIdToSubkey, serializeLookup } from './util.js';
import { logger } from '@powersync/lib-services-framework';

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
 * Keeps track of bulkwrite operations within a transaction.
 *
 * There may be multiple of these batches per transaction, but it may not span
 * multiple transactions.
 */
export class PersistedBatch {
  bucketData: mongo.AnyBulkWriteOperation<BucketDataDocument>[] = [];
  bucketParameters: mongo.AnyBulkWriteOperation<BucketParameterDocument>[] = [];
  currentData: mongo.AnyBulkWriteOperation<CurrentDataDocument>[] = [];

  /**
   * For debug logging only.
   */
  debugLastOpId: bigint | null = null;

  /**
   * Very rough estimate of transaction size.
   */
  currentSize = 0;

  constructor(
    private group_id: number,
    writtenSize: number
  ) {
    this.currentSize = writtenSize;
  }

  saveBucketData(options: {
    op_seq: MongoIdSequence;
    sourceKey: ReplicaId;
    table: SourceTable;
    evaluated: EvaluatedRow[];
    before_buckets: CurrentBucket[];
  }) {
    const remaining_buckets = new Map<string, CurrentBucket>();
    for (let b of options.before_buckets) {
      const key = currentBucketKey(b);
      remaining_buckets.set(key, b);
    }

    const dchecksum = util.hashDelete(replicaIdToSubkey(options.table.id, options.sourceKey));

    for (let k of options.evaluated) {
      const key = currentBucketKey(k);
      remaining_buckets.delete(key);

      // INSERT
      const recordData = JSONBig.stringify(k.data);
      const checksum = util.hashData(k.table, k.id, recordData);
      this.currentSize += recordData.length + 200;

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
            checksum: checksum,
            data: recordData
          }
        }
      });
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
    }
  }

  saveParameterData(data: {
    op_seq: MongoIdSequence;
    sourceKey: ReplicaId;
    sourceTable: SourceTable;
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
      const binLookup = serializeLookup(result.lookup);
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
            bucket_parameters: result.bucket_parameters
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

  deleteCurrentData(id: SourceKey) {
    const op: mongo.AnyBulkWriteOperation<CurrentDataDocument> = {
      deleteOne: {
        filter: { _id: id }
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
    return this.currentSize >= MAX_TRANSACTION_BATCH_SIZE;
  }

  async flush(db: PowerSyncMongo, session: mongo.ClientSession) {
    if (this.bucketData.length > 0) {
      await db.bucket_data.bulkWrite(this.bucketData, {
        session,
        // inserts only - order doesn't matter
        ordered: false
      });
    }
    if (this.bucketParameters.length > 0) {
      await db.bucket_parameters.bulkWrite(this.bucketParameters, {
        session,
        // inserts only - order doesn't matter
        ordered: false
      });
    }
    if (this.currentData.length > 0) {
      await db.current_data.bulkWrite(this.currentData, {
        session,
        // may update and delete data within the same batch - order matters
        ordered: true
      });
    }

    logger.info(
      `powersync_${this.group_id} Flushed ${this.bucketData.length} + ${this.bucketParameters.length} + ${
        this.currentData.length
      } updates, ${Math.round(this.currentSize / 1024)}kb. Last op_id: ${this.debugLastOpId}`
    );

    this.bucketData = [];
    this.bucketParameters = [];
    this.currentData = [];
    this.currentSize = 0;
    this.debugLastOpId = null;
  }
}
