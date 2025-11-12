import { mongo, MONGO_OPERATION_TIMEOUT_MS } from '@powersync/lib-service-mongodb';
import { logger, ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  addChecksums,
  InternalOpId,
  isPartialChecksum,
  PopulateChecksumCacheResults,
  storage,
  utils
} from '@powersync/service-core';

import { PowerSyncMongo } from './db.js';
import { BucketDataDocument, BucketDataKey, BucketStateDocument } from './models.js';
import { MongoSyncBucketStorage } from './MongoSyncBucketStorage.js';
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

  /**
   * Incrementally-updated checksum, up to maxOpId
   */
  checksum: number;

  /**
   * op count for the checksum
   */
  opCount: number;

  /**
   * Byte size of ops covered by the checksum.
   */
  opBytes: number;
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
  private bucketStateUpdates: mongo.AnyBulkWriteOperation<BucketStateDocument>[] = [];

  private idLimitBytes: number;
  private moveBatchLimit: number;
  private moveBatchQueryLimit: number;
  private clearBatchLimit: number;
  private maxOpId: bigint;
  private buckets: string[] | undefined;
  private signal?: AbortSignal;
  private group_id: number;

  constructor(
    private storage: MongoSyncBucketStorage,
    private db: PowerSyncMongo,
    options?: MongoCompactOptions
  ) {
    this.group_id = storage.group_id;
    this.idLimitBytes = (options?.memoryLimitMB ?? DEFAULT_MEMORY_LIMIT_MB) * 1024 * 1024;
    this.moveBatchLimit = options?.moveBatchLimit ?? DEFAULT_MOVE_BATCH_LIMIT;
    this.moveBatchQueryLimit = options?.moveBatchQueryLimit ?? DEFAULT_MOVE_BATCH_QUERY_LIMIT;
    this.clearBatchLimit = options?.clearBatchLimit ?? DEFAULT_CLEAR_BATCH_LIMIT;
    this.maxOpId = options?.maxOpId ?? 0n;
    this.buckets = options?.compactBuckets;
    this.signal = options?.signal;
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
        await this.compactSingleBucket(bucket);
      }
    } else {
      await this.compactDirtyBuckets();
    }
  }

  private async compactDirtyBuckets() {
    while (!this.signal?.aborted) {
      // Process all buckets with 10 or more changes since last time.
      // We exclude the last 100 compacted buckets, to avoid repeatedly re-compacting the same buckets over and over
      // if they are modified while compacting.
      const TRACK_RECENTLY_COMPACTED_NUMBER = 100;

      let recentlyCompacted: string[] = [];
      const buckets = await this.dirtyBucketBatch({ minBucketChanges: 10, exclude: recentlyCompacted });
      if (buckets.length == 0) {
        // All done
        break;
      }
      for (let { bucket } of buckets) {
        await this.compactSingleBucket(bucket);
        recentlyCompacted.push(bucket);
      }
      if (recentlyCompacted.length > TRACK_RECENTLY_COMPACTED_NUMBER) {
        recentlyCompacted = recentlyCompacted.slice(-TRACK_RECENTLY_COMPACTED_NUMBER);
      }
    }
  }

  private async compactSingleBucket(bucket: string) {
    const idLimitBytes = this.idLimitBytes;

    let currentState: CurrentBucketState = {
      bucket,
      seen: new Map(),
      trackingSize: 0,
      lastNotPut: null,
      opsSincePut: 0,

      checksum: 0,
      opCount: 0,
      opBytes: 0
    };

    // Constant lower bound
    const lowerBound: BucketDataKey = {
      g: this.group_id,
      b: bucket,
      o: new mongo.MinKey() as any
    };

    // Upper bound is adjusted for each batch
    let upperBound: BucketDataKey = {
      g: this.group_id,
      b: bucket,
      o: new mongo.MaxKey() as any
    };

    while (!this.signal?.aborted) {
      // Query one batch at a time, to avoid cursor timeouts
      const cursor = this.db.bucket_data.aggregate<BucketDataDocument & { size: number | bigint }>(
        [
          {
            $match: {
              _id: {
                $gte: lowerBound,
                $lt: upperBound
              }
            }
          },
          { $sort: { _id: -1 } },
          { $limit: this.moveBatchQueryLimit },
          {
            $project: {
              _id: 1,
              op: 1,
              table: 1,
              row_id: 1,
              source_table: 1,
              source_key: 1,
              checksum: 1,
              size: { $bsonSize: '$$ROOT' }
            }
          }
        ],
        {
          // batchSize is 1 more than limit to auto-close the cursor.
          // See https://github.com/mongodb/node-mongodb-native/pull/4580
          batchSize: this.moveBatchQueryLimit + 1
        }
      );
      // We don't limit to a single batch here, since that often causes MongoDB to scan through more than it returns.
      // Instead, we load up to the limit.
      const batch = await cursor.toArray();

      if (batch.length == 0) {
        // We've reached the end
        break;
      }

      // Set upperBound for the next batch
      upperBound = batch[batch.length - 1]._id;

      for (let doc of batch) {
        if (doc._id.o > this.maxOpId) {
          continue;
        }

        currentState.checksum = addChecksums(currentState.checksum, Number(doc.checksum));
        currentState.opCount += 1;

        let isPersistentPut = doc.op == 'PUT';

        currentState.opBytes += Number(doc.size);
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

            currentState.opBytes += 200 - Number(doc.size); // TODO: better estimate for this
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

        if (this.updates.length + this.bucketStateUpdates.length >= this.moveBatchLimit) {
          await this.flush();
        }
      }

      logger.info(`Processed batch of length ${batch.length} current bucket: ${bucket}`);
    }

    // Free memory before clearing bucket
    currentState.seen.clear();
    if (currentState.lastNotPut != null && currentState.opsSincePut >= 1) {
      logger.info(
        `Inserting CLEAR at ${this.group_id}:${bucket}:${currentState.lastNotPut} to remove ${currentState.opsSincePut} operations`
      );
      // Need flush() before clear()
      await this.flush();
      await this.clearBucket(currentState);
    }

    // Do this _after_ clearBucket so that we have accurate counts.
    this.updateBucketChecksums(currentState);

    // Need another flush after updateBucketChecksums()
    await this.flush();
  }

  /**
   * Call when done with a bucket.
   */
  private updateBucketChecksums(state: CurrentBucketState) {
    if (state.opCount < 0) {
      throw new ServiceAssertionError(
        `Invalid opCount: ${state.opCount} checksum ${state.checksum} opsSincePut: ${state.opsSincePut} maxOpId: ${this.maxOpId}`
      );
    }
    this.bucketStateUpdates.push({
      updateOne: {
        filter: {
          _id: {
            g: this.group_id,
            b: state.bucket
          }
        },
        update: {
          $set: {
            compacted_state: {
              op_id: this.maxOpId,
              count: state.opCount,
              checksum: BigInt(state.checksum),
              bytes: state.opBytes
            },
            estimate_since_compact: {
              // Note: There could have been a whole bunch of new operations added to the bucket _while_ compacting,
              // which we don't currently cater for.
              // We could potentially query for that, but that could add overhead.
              count: 0,
              bytes: 0
            }
          }
        },
        // We generally expect this to have been created before.
        // We don't create new ones here, to avoid issues with the unique index on bucket_updates.
        upsert: false
      }
    });
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
    if (this.bucketStateUpdates.length > 0) {
      logger.info(`Updating ${this.bucketStateUpdates.length} bucket states`);
      await this.db.bucket_state.bulkWrite(this.bucketStateUpdates, {
        ordered: false
      });
      this.bucketStateUpdates = [];
    }
  }

  /**
   * Perform a CLEAR compact for a bucket.
   *
   *
   * @param bucket bucket name
   * @param op op_id of the last non-PUT operation, which will be converted to CLEAR.
   */
  private async clearBucket(currentState: CurrentBucketState) {
    const bucket = currentState.bucket;
    const clearOp = currentState.lastNotPut!;

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
          o: clearOp
        }
      }
    };

    const session = this.db.client.startSession();
    try {
      let done = false;
      while (!done && !this.signal?.aborted) {
        let opCountDiff = 0;
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
                checksum = utils.addChecksums(checksum, Number(op.checksum));
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
                checksum: BigInt(checksum),
                data: null,
                target_op: targetOp
              },
              { session }
            );

            opCountDiff = -numberOfOpsToClear + 1;
          },
          {
            writeConcern: { w: 'majority' },
            readConcern: { level: 'snapshot' }
          }
        );
        // Update _outside_ the transaction, since the transaction can be retried multiple times.
        currentState.opCount += opCountDiff;
      }
    } finally {
      await session.endSession();
    }
  }

  /**
   * Subset of compact, only populating checksums where relevant.
   */
  async populateChecksums(options: { minBucketChanges: number }): Promise<PopulateChecksumCacheResults> {
    let count = 0;
    while (!this.signal?.aborted) {
      const buckets = await this.dirtyBucketBatch(options);
      if (buckets.length == 0) {
        // All done
        break;
      }
      const start = Date.now();
      logger.info(`Calculating checksums for batch of ${buckets.length} buckets`);

      // Filter batch by estimated bucket size, to reduce possibility of timeouts
      let checkBuckets: typeof buckets = [];
      let totalCountEstimate = 0;
      for (let bucket of buckets) {
        checkBuckets.push(bucket);
        totalCountEstimate += bucket.estimatedCount;
        if (totalCountEstimate > 50_000) {
          break;
        }
      }
      await this.updateChecksumsBatch(checkBuckets.map((b) => b.bucket));
      logger.info(`Updated checksums for batch of ${checkBuckets.length} buckets in ${Date.now() - start}ms`);
      count += buckets.length;
    }
    return { buckets: count };
  }

  /**
   * Returns a batch of dirty buckets - buckets with most changes first.
   *
   * This cannot be used to iterate on its own - the client is expected to process these buckets and
   * set estimate_since_compact.count: 0 when done, before fetching the next batch.
   */
  private async dirtyBucketBatch(options: {
    minBucketChanges: number;
    exclude?: string[];
  }): Promise<{ bucket: string; estimatedCount: number }[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    // We make use of an index on {_id.g: 1, 'estimate_since_compact.count': -1}
    const dirtyBuckets = await this.db.bucket_state
      .find(
        {
          '_id.g': this.group_id,
          'estimate_since_compact.count': { $gte: options.minBucketChanges },
          '_id.b': { $nin: options.exclude ?? [] }
        },
        {
          projection: {
            _id: 1,
            estimate_since_compact: 1,
            compacted_state: 1
          },
          sort: {
            'estimate_since_compact.count': -1
          },
          limit: 200,
          maxTimeMS: MONGO_OPERATION_TIMEOUT_MS
        }
      )
      .toArray();

    return dirtyBuckets.map((bucket) => ({
      bucket: bucket._id.b,
      estimatedCount: bucket.estimate_since_compact!.count + (bucket.compacted_state?.count ?? 0)
    }));
  }

  private async updateChecksumsBatch(buckets: string[]) {
    const checksums = await this.storage.checksums.computePartialChecksumsDirect(
      buckets.map((bucket) => {
        return {
          bucket,
          end: this.maxOpId
        };
      })
    );

    for (let bucketChecksum of checksums.values()) {
      if (isPartialChecksum(bucketChecksum)) {
        // Should never happen since we don't specify `start`
        throw new ServiceAssertionError(`Full checksum expected, got ${JSON.stringify(bucketChecksum)}`);
      }

      this.bucketStateUpdates.push({
        updateOne: {
          filter: {
            _id: {
              g: this.group_id,
              b: bucketChecksum.bucket
            }
          },
          update: {
            $set: {
              compacted_state: {
                op_id: this.maxOpId,
                count: bucketChecksum.count,
                checksum: BigInt(bucketChecksum.checksum),
                bytes: null
              },
              estimate_since_compact: {
                count: 0,
                bytes: 0
              }
            }
          },
          // We don't create new ones here - it gets tricky to get the last_op right with the unique index on:
          // bucket_updates: {'id.g': 1, 'last_op': 1}
          upsert: false
        }
      });
    }

    await this.flush();
  }
}
