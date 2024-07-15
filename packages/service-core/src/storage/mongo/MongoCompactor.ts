import { logger } from '@powersync/lib-services-framework';
import { AnyBulkWriteOperation, MaxKey, MinKey } from 'mongodb';
import { addChecksums } from '../../util/utils.js';
import { PowerSyncMongo } from './db.js';
import { BucketDataDocument, BucketDataKey } from './models.js';
import { CompactOptions } from '../BucketStorage.js';

interface CurrentBucketState {
  /** Bucket name */
  bucket: string;
  /**
   * Rows seen in the bucket, with the last op_id of each.
   */
  seen: Map<string, bigint>;
  /**
   * Estimated memory usage of the seen Map.
   */
  trackingSize: number;

  /**
   * Last (lowest) seen op_id that is not a PUT.
   */
  lastNotPut: bigint | null;

  /**
   * Number of REMOVE/MOVE operations seen since lastNotPut.
   */
  opsSincePut: number;
}

const CLEAR_BATCH_LIMIT = 5000;
const MOVE_BATCH_LIMIT = 2000;
const MOVE_BATCH_QUERY_LIMIT = 10_000;

/** This default is primarily for tests. */
const DEFAULT_MEMORY_LIMIT_MB = 64;

export class MongoCompactor {
  private updates: AnyBulkWriteOperation<BucketDataDocument>[] = [];

  constructor(private db: PowerSyncMongo, private group_id: number) {}

  /**
   * Compact buckets by converting operatoins into MOVE and/or CLEAR operations.
   *
   * See /docs/compacting-operations.md for details.
   */
  async compact(options?: CompactOptions) {
    const idLimitBytes = (options?.memoryLimitMB ?? DEFAULT_MEMORY_LIMIT_MB) * 1024 * 1024;

    let currentState: CurrentBucketState | null = null;

    // Constant lower bound
    const lowerBound: BucketDataKey = {
      g: this.group_id,
      b: new MinKey() as any,
      o: new MinKey() as any
    };

    // Upper bound is adjusted for each batch
    let upperBound: BucketDataKey = {
      g: this.group_id,
      b: new MaxKey() as any,
      o: new MaxKey() as any
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
            limit: MOVE_BATCH_QUERY_LIMIT,
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

        let isPersistentPut = doc.op == 'PUT';

        if (doc.op == 'REMOVE' || doc.op == 'PUT') {
          const key = `${doc.table}/${doc.row_id}/${doc.source_table}/${doc.source_key?.toHexString()}`;
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
            if (currentState.trackingSize > idLimitBytes) {
              // Reached memory limit.
              // Keep the highest seen values in this case.
            } else {
              // flatstr reduces the memory usage by flattening the string
              currentState.seen.set(flatstr(key), doc._id.o);
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

        if (this.updates.length >= MOVE_BATCH_LIMIT) {
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
  private async clearBucket(bucket: string, op: bigint) {
    const opFilter = {
      _id: {
        $gte: {
          g: this.group_id,
          b: bucket,
          o: new MinKey() as any
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
              limit: CLEAR_BATCH_LIMIT
            });
            let checksum = 0;
            let lastOpId: BucketDataKey | null = null;
            let targetOp: bigint | null = null;
            let gotAnOp = false;
            for await (let op of query.stream()) {
              if (op.op == 'MOVE' || op.op == 'REMOVE' || op.op == 'CLEAR') {
                checksum = addChecksums(checksum, op.checksum);
                lastOpId = op._id;
                if (op.op != 'CLEAR') {
                  gotAnOp = true;
                }
                if (op.target_op != null) {
                  if (targetOp == null || op.target_op > targetOp) {
                    targetOp = op.target_op;
                  }
                }
              } else {
                throw new Error(`Unexpected ${op.op} operation at ${op._id.g}:${op._id.b}:${op._id.o}`);
              }
            }
            if (!gotAnOp) {
              done = true;
              return;
            }

            logger.info(`Flushing CLEAR at ${lastOpId?.o}`);
            await this.db.bucket_data.deleteMany(
              {
                _id: {
                  $gte: {
                    g: this.group_id,
                    b: bucket,
                    o: new MinKey() as any
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

/**
 * Flattens string to reduce memory usage (around 320 bytes -> 120 bytes),
 * at the cost of some upfront CPU usage.
 *
 * From: https://github.com/davidmarkclements/flatstr/issues/8
 */
function flatstr(s: string) {
  s.match(/\n/g);
  return s;
}
