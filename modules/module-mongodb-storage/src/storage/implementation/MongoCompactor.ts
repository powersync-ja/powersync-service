import { mongo } from '@powersync/lib-service-mongodb';
import { logger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { InternalOpId, storage, utils } from '@powersync/service-core';

import { PowerSyncMongo } from './db.js';
import { BucketDataDocument, BucketDataKey } from './models.js';
import { cacheKey } from './OperationBatch.js';

interface CurrentBucketState {
  /** Bucket name */
  bucket: string;
  /**
   * Rows seen in the bucket, with the last op_id of each.
   */
  seen: Map<string, InternalOpId>;
  /**
   * Estimated memory usage of the seen Map.
   */
  trackingSize: number;

  /**
   * Last (lowest) seen op_id that is not a PUT.
   */
  lastNotPut: InternalOpId | null;

  /**
   * Number of REMOVE/MOVE operations seen since lastNotPut.
   */
  opsSincePut: number;
}

/**
 * Additional options, primarily for testing.
 */
export interface MongoCompactOptions extends storage.CompactOptions {}

const DEFAULT_CLEAR_BATCH_LIMIT = 5000;
const DEFAULT_MOVE_BATCH_LIMIT = 2000;
const DEFAULT_MOVE_BATCH_QUERY_LIMIT = 10_000;

/** This default is primarily for tests. */
const DEFAULT_MEMORY_LIMIT_MB = 64;

export class MongoCompactor {
  private updates: mongo.AnyBulkWriteOperation<BucketDataDocument>[] = [];

  private idLimitBytes: number;
  private moveBatchLimit: number;
  private moveBatchQueryLimit: number;
  private clearBatchLimit: number;
  private maxOpId: bigint | undefined;
  private buckets: string[] | undefined;

  constructor(
    private db: PowerSyncMongo,
    private group_id: number,
    options?: MongoCompactOptions
  ) {
    this.idLimitBytes = (options?.memoryLimitMB ?? DEFAULT_MEMORY_LIMIT_MB) * 1024 * 1024;
    this.moveBatchLimit = options?.moveBatchLimit ?? DEFAULT_MOVE_BATCH_LIMIT;
    this.moveBatchQueryLimit = options?.moveBatchQueryLimit ?? DEFAULT_MOVE_BATCH_QUERY_LIMIT;
    this.clearBatchLimit = options?.clearBatchLimit ?? DEFAULT_CLEAR_BATCH_LIMIT;
    this.maxOpId = options?.maxOpId;
    this.buckets = options?.compactBuckets;
  }

  /**
   * Compact buckets by converting operations into MOVE and/or CLEAR operations.
   *
   * See /docs/compacting-operations.md for details.
   */
  async compact() {
    if (this.buckets) {
      for (let bucket of this.buckets) {
        // We can make this more efficient later on by iterating
        // through the buckets in a single query.
        // That makes batching more tricky, so we leave for later.
        await this.compactInternal(bucket);
      }
    } else {
      await this.compactInternal(undefined);
    }
  }

  async compactInternal(bucket: string | undefined) {
    const idLimitBytes = this.idLimitBytes;

    let currentState: CurrentBucketState | null = null;

    let bucketLower: string | mongo.MinKey;
    let bucketUpper: string | mongo.MaxKey;

    if (bucket == null) {
      bucketLower = new mongo.MinKey();
      bucketUpper = new mongo.MaxKey();
    } else if (bucket.includes('[')) {
      // Exact bucket name
      bucketLower = bucket;
      bucketUpper = bucket;
    } else {
      // Bucket definition name
      bucketLower = `${bucket}[`;
      bucketUpper = `${bucket}[\uFFFF`;
    }

    // Constant lower bound
    const lowerBound: BucketDataKey = {
      g: this.group_id,
      b: bucketLower as string,
      o: new mongo.MinKey() as any
    };

    // Upper bound is adjusted for each batch
    let upperBound: BucketDataKey = {
      g: this.group_id,
      b: bucketUpper as string,
      o: new mongo.MaxKey() as any
    };

    while (true) {
      // Query one batch at a time, to avoid cursor timeouts
      const batch = await this.db.bucket_data
        .find(
          {
            _id: {
              $gte: lowerBound,
              $lt: upperBound
            }
          },
          {
            projection: {
              _id: 1,
              op: 1,
              table: 1,
              row_id: 1,
              source_table: 1,
              source_key: 1
            },
            limit: this.moveBatchQueryLimit,
            sort: { _id: -1 },
            singleBatch: true
          }
        )
        .toArray();

      if (batch.length == 0) {
        // We've reached the end
        break;
      }

      // Set upperBound for the next batch
      upperBound = batch[batch.length - 1]._id;

      for (let doc of batch) {
        if (currentState == null || doc._id.b != currentState.bucket) {
          if (currentState != null && currentState.lastNotPut != null && currentState.opsSincePut >= 1) {
            // Important to flush before clearBucket()
            await this.flush();
            logger.info(
              `Inserting CLEAR at ${this.group_id}:${currentState.bucket}:${currentState.lastNotPut} to remove ${currentState.opsSincePut} operations`
            );

            const bucket = currentState.bucket;
            const clearOp = currentState.lastNotPut;
            // Free memory before clearing bucket
            currentState = null;
            await this.clearBucket(bucket, clearOp);
          }
          currentState = {
            bucket: doc._id.b,
            seen: new Map(),
            trackingSize: 0,
            lastNotPut: null,
            opsSincePut: 0
          };
        }

        if (this.maxOpId != null && doc._id.o > this.maxOpId) {
          continue;
        }

        let isPersistentPut = doc.op == 'PUT';

        if (doc.op == 'REMOVE' || doc.op == 'PUT') {
          const key = `${doc.table}/${doc.row_id}/${cacheKey(doc.source_table!, doc.source_key!)}`;
          const targetOp = currentState.seen.get(key);
          if (targetOp) {
            // Will convert to MOVE, so don't count as PUT
            isPersistentPut = false;

            this.updates.push({
              updateOne: {
                filter: {
                  _id: doc._id
                },
                update: {
                  $set: {
                    op: 'MOVE',
                    target_op: targetOp
                  },
                  $unset: {
                    source_table: 1,
                    source_key: 1,
                    table: 1,
                    row_id: 1,
                    data: 1
                  }
                }
              }
            });
          } else {
            if (currentState.trackingSize >= idLimitBytes) {
              // Reached memory limit.
              // Keep the highest seen values in this case.
            } else {
              // flatstr reduces the memory usage by flattening the string
              currentState.seen.set(utils.flatstr(key), doc._id.o);
              // length + 16 for the string
              // 24 for the bigint
              // 50 for map overhead
              // 50 for additional overhead
              currentState.trackingSize += key.length + 140;
            }
          }
        }

        if (isPersistentPut) {
          currentState.lastNotPut = null;
          currentState.opsSincePut = 0;
        } else if (doc.op != 'CLEAR') {
          if (currentState.lastNotPut == null) {
            currentState.lastNotPut = doc._id.o;
          }
          currentState.opsSincePut += 1;
        }

        if (this.updates.length >= this.moveBatchLimit) {
          await this.flush();
        }
      }
    }

    await this.flush();
    currentState?.seen.clear();
    if (currentState?.lastNotPut != null && currentState?.opsSincePut > 1) {
      logger.info(
        `Inserting CLEAR at ${this.group_id}:${currentState.bucket}:${currentState.lastNotPut} to remove ${currentState.opsSincePut} operations`
      );
      const bucket = currentState.bucket;
      const clearOp = currentState.lastNotPut;
      // Free memory before clearing bucket
      currentState = null;
      await this.clearBucket(bucket, clearOp);
    }
  }

  private async flush() {
    if (this.updates.length > 0) {
      logger.info(`Compacting ${this.updates.length} ops`);
      await this.db.bucket_data.bulkWrite(this.updates, {
        // Order is not important.
        // Since checksums are not affected, these operations can happen in any order,
        // and it's fine if the operations are partially applied.
        // Each individual operation is atomic.
        ordered: false
      });
      this.updates = [];
    }
  }

  /**
   * Perform a CLEAR compact for a bucket.
   *
   * @param bucket bucket name
   * @param op op_id of the last non-PUT operation, which will be converted to CLEAR.
   */
  private async clearBucket(bucket: string, op: InternalOpId) {
    const opFilter = {
      _id: {
        $gte: {
          g: this.group_id,
          b: bucket,
          o: new mongo.MinKey() as any
        },
        $lte: {
          g: this.group_id,
          b: bucket,
          o: op
        }
      }
    };

    const session = this.db.client.startSession();
    try {
      let done = false;
      while (!done) {
        // Do the CLEAR operation in batches, with each batch a separate transaction.
        // The state after each batch is fully consistent.
        // We need a transaction per batch to make sure checksums stay consistent.
        await session.withTransaction(
          async () => {
            const query = this.db.bucket_data.find(opFilter, {
              session,
              sort: { _id: 1 },
              projection: {
                _id: 1,
                op: 1,
                checksum: 1,
                target_op: 1
              },
              limit: this.clearBatchLimit
            });
            let checksum = 0;
            let lastOpId: BucketDataKey | null = null;
            let targetOp: bigint | null = null;
            let gotAnOp = false;
            let numberOfOpsToClear = 0;
            for await (let op of query.stream()) {
              if (op.op == 'MOVE' || op.op == 'REMOVE' || op.op == 'CLEAR') {
                checksum = utils.addChecksums(checksum, op.checksum);
                lastOpId = op._id;
                numberOfOpsToClear += 1;
                if (op.op != 'CLEAR') {
                  gotAnOp = true;
                }
                if (op.target_op != null) {
                  if (targetOp == null || op.target_op > targetOp) {
                    targetOp = op.target_op;
                  }
                }
              } else {
                throw new ReplicationAssertionError(
                  `Unexpected ${op.op} operation at ${op._id.g}:${op._id.b}:${op._id.o}`
                );
              }
            }
            if (!gotAnOp) {
              done = true;
              return;
            }

            logger.info(`Flushing CLEAR for ${numberOfOpsToClear} ops at ${lastOpId?.o}`);
            await this.db.bucket_data.deleteMany(
              {
                _id: {
                  $gte: {
                    g: this.group_id,
                    b: bucket,
                    o: new mongo.MinKey() as any
                  },
                  $lte: lastOpId!
                }
              },
              { session }
            );

            await this.db.bucket_data.insertOne(
              {
                _id: lastOpId!,
                op: 'CLEAR',
                checksum: checksum,
                data: null,
                target_op: targetOp
              },
              { session }
            );
          },
          {
            writeConcern: { w: 'majority' },
            readConcern: { level: 'snapshot' }
          }
        );
      }
    } finally {
      await session.endSession();
    }
  }
}
