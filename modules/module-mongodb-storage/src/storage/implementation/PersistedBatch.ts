import { mongo } from '@powersync/lib-service-mongodb';
import { JSONBig } from '@powersync/service-jsonbig';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';

import { logger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, storage, utils } from '@powersync/service-core';
import { currentBucketKey } from './MongoBucketBatch.js';
import { MongoIdSequence } from './MongoIdSequence.js';
import { PowerSyncMongo } from './db.js';
import {
  BucketDataDocument,
  BucketDataRangeDocument,
  BucketDataSingleDocument,
  BucketParameterDocument,
  CurrentBucket,
  CurrentDataDocument,
  EmbeddedOpData,
  SourceKey
} from './models.js';
import { replicaIdToSubkey, safeBulkWrite } from './util.js';

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

const MAX_EMBEDDED_DOC_COUNT = 100;

/**
 * Must be less than 16MB/2.
 *
 * This is not a hard limit, but we don't do any further batching if this limit is exceeded.
 */
const EMBEDDED_DOC_SIZE_THRESHOLD = 1_000_000;

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
  lastOpByBucket: Map<string, BucketDataDocument> = new Map();
  bucketDataOps: number = 0;

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

  private addBucketDataOp(doc: BucketDataSingleDocument) {
    const bucket = doc._id.b;
    if (doc.op === 'CLEAR') {
      // No batching for CLEAR ops
      this.lastOpByBucket.delete(bucket);
      this.bucketData.push({
        insertOne: {
          document: doc
        }
      });
      return;
    }
    const lastOp = this.lastOpByBucket.get(bucket);
    this.bucketDataOps += 1;
    if (lastOp != null && (doc.data == null || doc.data.length < EMBEDDED_DOC_SIZE_THRESHOLD)) {
      // Merge ops
      const rangeOp = lastOp as unknown as BucketDataRangeDocument;
      if (lastOp.op != 'RANGE') {
        // Convert to BucketDataRangeDocument in-place
        rangeOp.op_count = 1;
        rangeOp.data_range = [makeEmbeddedOp(lastOp)];
        rangeOp.start_op_id = lastOp._id.o;
        rangeOp.data = null;
        rangeOp.op = 'RANGE';
        // Unchanged at this point: _id, checksum, target_op
      }

      // Modify this in-place
      rangeOp._id = doc._id;
      rangeOp.checksum = addChecksums(lastOp.checksum, doc.checksum);
      rangeOp.op_count += 1;
      rangeOp.data_range.push(makeEmbeddedOp(doc));
      if (rangeOp.target_op == null || (doc.target_op != null && doc.target_op > rangeOp.target_op)) {
        rangeOp.target_op = doc.target_op;
      }
      if (rangeOp.data_range.length >= MAX_EMBEDDED_DOC_COUNT) {
        // Don't do any further batching on this
        this.lastOpByBucket.delete(bucket);
      }
    } else {
      // New op for the bucket
      this.bucketData.push({
        insertOne: {
          document: doc
        }
      });
      if (doc.data != null && doc.data.length >= EMBEDDED_DOC_SIZE_THRESHOLD) {
        // Don't do any further batching on this
        this.lastOpByBucket.delete(bucket);
      } else {
        this.lastOpByBucket.set(bucket, doc);
      }
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

    const dchecksum = utils.hashDelete(replicaIdToSubkey(options.table.id, options.sourceKey));

    for (const k of options.evaluated) {
      const key = currentBucketKey(k);
      remaining_buckets.delete(key);

      // INSERT
      const recordData = JSONBig.stringify(k.data);
      const checksum = utils.hashData(k.table, k.id, recordData);
      this.currentSize += recordData.length + 200;

      const op_id = options.op_seq.next();
      this.debugLastOpId = op_id;

      this.addBucketDataOp({
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
      });
    }

    for (let bd of remaining_buckets.values()) {
      // REMOVE

      const op_id = options.op_seq.next();
      this.debugLastOpId = op_id;

      this.addBucketDataOp({
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
      });

      this.currentSize += 200;
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
    return (
      this.currentSize >= MAX_TRANSACTION_BATCH_SIZE ||
      this.bucketData.length >= MAX_TRANSACTION_DOC_COUNT ||
      this.currentData.length >= MAX_TRANSACTION_DOC_COUNT ||
      this.bucketParameters.length >= MAX_TRANSACTION_DOC_COUNT
    );
  }

  async flush(db: PowerSyncMongo, session: mongo.ClientSession) {
    if (this.bucketData.length > 0) {
      // calculate total size
      await safeBulkWrite(db.bucket_data, this.bucketData, {
        session,
        // inserts only - order doesn't matter
        ordered: false
      });
    }
    if (this.bucketParameters.length > 0) {
      await safeBulkWrite(db.bucket_parameters, this.bucketParameters, {
        session,
        // inserts only - order doesn't matter
        ordered: false
      });
    }
    if (this.currentData.length > 0) {
      await safeBulkWrite(db.current_data, this.currentData, {
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

function makeEmbeddedOp(op: BucketDataSingleDocument): EmbeddedOpData {
  if (op.op === 'CLEAR') {
    throw new ReplicationAssertionError('CLEAR operations cannot be embedded');
  }
  return {
    o: op._id.o,
    op: op.op,
    checksum: op.checksum,
    data: op.data,
    source_table: op.source_table,
    source_key: op.source_key,
    table: op.table,
    row_id: op.row_id
  };
}
