import { isMongoServerError, mongo, MONGO_OPERATION_TIMEOUT_MS } from '@powersync/lib-service-mongodb';
import { logger, ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  addChecksums,
  InternalOpId,
  isPartialChecksum,
  PopulateChecksumCacheResults,
  storage,
  utils
} from '@powersync/service-core';

import { VersionedPowerSyncMongo } from './db.js';
import { BucketDefinitionId } from './BucketDefinitionMapping.js';
import {
  BucketDataDocumentV1,
  BucketDataDocumentV3,
  BucketStateDocumentV1,
  BucketStateDocumentV3,
  LEGACY_BUCKET_DATA_DEFINITION_ID,
  TaggedBucketDataDocument,
  bucketDataDocumentToTagged,
  taggedBucketDataDocumentToV1,
  taggedBucketDataDocumentToV3
} from './models.js';
import { MongoSyncBucketStorage } from './MongoSyncBucketStorage.js';
import { cacheKey } from './OperationBatch.js';

interface CurrentBucketState {
  /** Bucket name */
  bucket: string;
  definitionId: BucketDefinitionId;
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
   * Incrementally-updated checksum, up to maxOpId.
   */
  checksum: number;
  /**
   * Op count for the checksum.
   */
  opCount: number;
  /**
   * Byte size of ops covered by the checksum.
   */
  opBytes: number;
}

type CompactBucketDataDocument = Pick<
  TaggedBucketDataDocument,
  '_id' | 'def' | 'op' | 'table' | 'row_id' | 'source_table' | 'source_key' | 'checksum' | 'target_op'
> & {
  size: number | bigint;
};

type CompactClearBucketDataDocument = Pick<TaggedBucketDataDocument, '_id' | 'def' | 'op' | 'checksum' | 'target_op'>;
type BucketDataCollectionDocument = BucketDataDocumentV1 | BucketDataDocumentV3;
type BucketDataClearProjection = {
  _id: BucketDataCollectionDocument['_id'];
  op: CompactClearBucketDataDocument['op'];
  checksum: bigint;
  target_op?: bigint | null;
};

type BucketStateProjection = {
  _id: { b: string };
  estimate_since_compact?: {
    count: number;
    bytes: number | bigint;
  };
  compacted_state?: {
    count: number;
    bytes: number | bigint | null;
  };
};

export interface MongoCompactOptions extends storage.CompactOptions {}

const DEFAULT_CLEAR_BATCH_LIMIT = 5000;
const DEFAULT_MOVE_BATCH_LIMIT = 2000;
const DEFAULT_MOVE_BATCH_QUERY_LIMIT = 10_000;
const DEFAULT_MIN_BUCKET_CHANGES = 10;
const DEFAULT_MIN_CHANGE_RATIO = 0.1;
const DIRTY_BUCKET_SCAN_BATCH_SIZE = 2_000;
/** This default is primarily for tests. */
const DEFAULT_MEMORY_LIMIT_MB = 64;

interface DirtyBucket {
  bucket: string;
  definitionId: BucketDefinitionId | null;
  estimatedCount: number;
  dirtyRatio?: number;
}

abstract class BaseMongoCompactor {
  protected updates: mongo.AnyBulkWriteOperation<mongo.Document>[] = [];
  protected bucketStateUpdates: mongo.AnyBulkWriteOperation<mongo.Document>[] = [];
  protected activeBucketDataCollection: mongo.Collection<mongo.Document> | null = null;
  protected activeBucketDefinitionId: BucketDefinitionId = LEGACY_BUCKET_DATA_DEFINITION_ID;

  protected readonly idLimitBytes: number;
  protected readonly moveBatchLimit: number;
  protected readonly moveBatchQueryLimit: number;
  protected readonly clearBatchLimit: number;
  protected readonly minBucketChanges: number;
  protected readonly minChangeRatio: number;
  protected readonly maxOpId: bigint;
  protected readonly buckets: string[] | undefined;
  protected readonly signal?: AbortSignal;
  protected readonly group_id: number;

  constructor(
    protected readonly storage: MongoSyncBucketStorage,
    protected readonly db: VersionedPowerSyncMongo,
    options: MongoCompactOptions
  ) {
    this.group_id = storage.group_id;
    this.idLimitBytes = (options.memoryLimitMB ?? DEFAULT_MEMORY_LIMIT_MB) * 1024 * 1024;
    this.moveBatchLimit = options.moveBatchLimit ?? DEFAULT_MOVE_BATCH_LIMIT;
    this.moveBatchQueryLimit = options.moveBatchQueryLimit ?? DEFAULT_MOVE_BATCH_QUERY_LIMIT;
    this.clearBatchLimit = options.clearBatchLimit ?? DEFAULT_CLEAR_BATCH_LIMIT;
    this.minBucketChanges = options.minBucketChanges ?? DEFAULT_MIN_BUCKET_CHANGES;
    this.minChangeRatio = options.minChangeRatio ?? DEFAULT_MIN_CHANGE_RATIO;
    this.maxOpId = options.maxOpId ?? 0n;
    this.buckets = options.compactBuckets;
    this.signal = options.signal;
  }

  /**
   * Compact buckets by converting operations into MOVE and/or CLEAR operations.
   *
   * See /docs/compacting-operations.md for details.
   */
  async compact() {
    if (this.buckets) {
      for (const bucket of this.buckets) {
        // We can make this more efficient later on by iterating through the buckets in a single query.
        // That makes batching more tricky, so we leave for later.
        await this.compactSingleBucketRetried(bucket);
      }
    } else {
      await this.compactDirtyBuckets();
    }
  }

  /**
   * Subset of compact, only populating checksums where relevant.
   */
  async populateChecksums(options: { minBucketChanges: number }): Promise<PopulateChecksumCacheResults> {
    let count = 0;
    while (true) {
      this.signal?.throwIfAborted();
      const buckets = await this.dirtyBucketBatchForChecksums(options);
      if (buckets.length == 0) {
        break;
      }
      this.signal?.throwIfAborted();

      const start = Date.now();
      // Filter batch by estimated bucket size, to reduce possibility of timeouts.
      const checkBuckets: typeof buckets = [];
      let totalCountEstimate = 0;
      for (const bucket of buckets) {
        checkBuckets.push(bucket);
        totalCountEstimate += bucket.estimatedCount;
        if (totalCountEstimate > 50_000) {
          break;
        }
      }
      logger.info(
        `Calculating checksums for batch of ${buckets.length} buckets, estimated count of ${totalCountEstimate}`
      );
      await this.updateChecksumsBatch(checkBuckets);
      logger.info(`Updated checksums for batch of ${checkBuckets.length} buckets in ${Date.now() - start}ms`);
      count += checkBuckets.length;
    }
    return { buckets: count };
  }

  protected async *dirtyBucketBatchesForCollection<TBucketState extends BucketStateProjection>(
    collection: mongo.Collection<TBucketState>,
    lastId: mongo.Document,
    maxId: mongo.Document,
    options: {
      minBucketChanges: number;
      minChangeRatio: number;
    },
    getDefinitionId: (state: TBucketState) => BucketDefinitionId | null
  ): AsyncGenerator<DirtyBucket[]> {
    while (true) {
      // To avoid timeouts from too many buckets not meeting the minBucketChanges criteria, use an aggregation pipeline
      // to scan a fixed batch of buckets at a time, but only return buckets that meet the criteria.
      const [result] = await collection
        .aggregate<{
          buckets: TBucketState[];
          cursor: Pick<TBucketState, '_id'>[];
        }>(
          [
            {
              $match: {
                _id: { $gt: lastId, $lt: maxId }
              }
            },
            {
              $sort: { _id: 1 }
            },
            {
              // Scan a fixed number of docs each query so sparse matches don't block progress.
              $limit: DIRTY_BUCKET_SCAN_BATCH_SIZE
            },
            {
              $facet: {
                buckets: [
                  {
                    $match: {
                      'estimate_since_compact.count': { $gte: options.minBucketChanges }
                    }
                  },
                  {
                    $project: {
                      _id: 1,
                      estimate_since_compact: 1,
                      compacted_state: 1
                    }
                  }
                ],
                // This is used for the next query.
                cursor: [{ $sort: { _id: -1 } }, { $limit: 1 }, { $project: { _id: 1 } }]
              }
            }
          ],
          { maxTimeMS: MONGO_OPERATION_TIMEOUT_MS }
        )
        .toArray();

      const cursor = result?.cursor?.[0];
      if (cursor == null) {
        break;
      }
      lastId = cursor._id as mongo.Document;

      const mapped = (result?.buckets ?? []).map((bucketState) => {
        // The numbers, specifically the bytes, could be a bigint. Convert to Number to allow calculating ratios.
        // BigInt precision is not needed here since this is only an estimate.
        const updatedCount = bucketState.estimate_since_compact?.count ?? 0;
        const totalCount = (bucketState.compacted_state?.count ?? 0) + updatedCount;
        const updatedBytes = Number(bucketState.estimate_since_compact?.bytes ?? 0);
        const totalBytes = Number(bucketState.compacted_state?.bytes ?? 0) + updatedBytes;
        const dirtyChangeNumber = totalCount > 0 ? updatedCount / totalCount : 0;
        const dirtyChangeBytes = totalBytes > 0 ? updatedBytes / totalBytes : 0;
        return {
          bucket: bucketState._id.b,
          definitionId: getDefinitionId(bucketState),
          estimatedCount: totalCount,
          dirtyRatio: Math.max(dirtyChangeNumber, dirtyChangeBytes)
        };
      });

      yield mapped.filter(
        (bucket) => bucket.estimatedCount >= options.minBucketChanges && bucket.dirtyRatio >= options.minChangeRatio
      );
    }
  }

  protected async dirtyBucketBatchForChecksumsForCollection<TBucketState extends BucketStateProjection>(
    collection: mongo.Collection<TBucketState>,
    filter: mongo.Filter<TBucketState>,
    getDefinitionId: (state: mongo.WithId<TBucketState>) => BucketDefinitionId | null
  ): Promise<DirtyBucket[]> {
    const dirtyBuckets = await collection
      .find(filter, {
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
      })
      .toArray();

    return dirtyBuckets.map((bucket) => ({
      bucket: bucket._id.b,
      definitionId: getDefinitionId(bucket),
      estimatedCount: Number(bucket.estimate_since_compact!.count) + Number(bucket.compacted_state?.count ?? 0)
    }));
  }

  public abstract dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]>;

  public abstract dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]>;

  protected async compactDirtyBuckets() {
    for await (const buckets of this.dirtyBucketBatches({
      minBucketChanges: this.minBucketChanges,
      minChangeRatio: this.minChangeRatio
    })) {
      this.signal?.throwIfAborted();
      if (buckets.length == 0) {
        continue;
      }

      for (const { bucket, definitionId } of buckets) {
        await this.compactSingleBucketRetried(bucket, definitionId);
      }
    }
  }

  /**
   * Compaction for a single bucket, with retries on failure.
   *
   * This covers against occasional network or other database errors during a long compact job.
   */
  protected async compactSingleBucketRetried(bucket: string, definitionId: BucketDefinitionId | null = null) {
    let retryCount = 0;
    while (true) {
      try {
        await this.compactSingleBucket(bucket, definitionId);
        break;
      } catch (e) {
        if (retryCount < 3 && isMongoServerError(e)) {
          logger.warn(`Error compacting bucket ${bucket}, retrying...`, e);
          retryCount++;
          await new Promise((resolve) => setTimeout(resolve, 1000 * retryCount));
        } else {
          throw e;
        }
      }
    }
  }

  protected async compactSingleBucket(bucket: string, definitionId: BucketDefinitionId | null = null) {
    const idLimitBytes = this.idLimitBytes;
    const bucketCollection = await this.getBucketDataCollection(bucket, definitionId);
    if (bucketCollection == null) {
      return;
    }
    this.activeBucketDataCollection = bucketCollection.collection;
    this.activeBucketDefinitionId = bucketCollection.definitionId;
    try {
      const currentState: CurrentBucketState = {
        bucket,
        definitionId: bucketCollection.definitionId,
        seen: new Map(),
        trackingSize: 0,
        lastNotPut: null,
        opsSincePut: 0,
        checksum: 0,
        opCount: 0,
        opBytes: 0
      };

      // Constant lower bound.
      const lowerBound = this.bucketDataKey(bucket, new mongo.MinKey() as any);
      // Upper bound is adjusted for each batch.
      let upperBound = this.bucketDataKey(bucket, new mongo.MaxKey() as any);

      while (true) {
        this.signal?.throwIfAborted();

        // Query one batch at a time, to avoid cursor timeouts.
        const pipeline = [
          {
            $match: {
              _id: {
                $gte: lowerBound,
                $lt: upperBound
              },
              // Workaround for a clustered collection bug where the $lt operator may include upperBound.
              // https://jira.mongodb.org/browse/SERVER-121822
              '_id.o': { $lt: upperBound.o }
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
        ];

        const cursor = bucketCollection.collection.aggregate<BucketDataCollectionDocument & { size: number | bigint }>(
          pipeline,
          {
            // batchSize is 1 more than limit to auto-close the cursor.
            // See https://github.com/mongodb/node-mongodb-native/pull/4580
            batchSize: this.moveBatchQueryLimit + 1
          }
        );
        // We don't limit to a single batch here, since that often causes MongoDB to scan through more than it returns.
        // Instead, we load up to the limit.
        const rawBatch = await cursor.toArray();
        const batch = rawBatch.map((document) => this.tagBucketDataDocument(document, bucketCollection.definitionId));

        if (batch.length == 0) {
          // We've reached the end.
          break;
        }

        // Reuse the exact collection _id value from Mongo for the next bound.
        upperBound = rawBatch[rawBatch.length - 1]._id;

        for (const doc of batch) {
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
              // Will convert to MOVE, so don't count as PUT.
              isPersistentPut = false;

              this.updates.push({
                updateOne: {
                  filter: { _id: this.bucketDataKey(doc._id.b, doc._id.o) },
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

              // TODO: better estimate for this.
              currentState.opBytes += 200 - Number(doc.size);
            } else if (currentState.trackingSize < idLimitBytes) {
              // flatstr reduces the memory usage by flattening the string.
              currentState.seen.set(utils.flatstr(key), doc._id.o);
              // length + 16 for the string
              // 24 for the bigint
              // 50 for map overhead
              // 50 for additional overhead
              currentState.trackingSize += key.length + 140;
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

      // Free memory before clearing the bucket.
      currentState.seen.clear();
      if (currentState.lastNotPut != null && currentState.opsSincePut >= 1) {
        logger.info(
          `Inserting CLEAR at ${this.group_id}:${bucket}:${currentState.lastNotPut} to remove ${currentState.opsSincePut} operations`
        );
        // Need flush() before clear().
        await this.flush();
        await this.clearBucket(currentState);
      }

      // Do this after clearBucket so we have accurate counts.
      this.updateBucketChecksums(currentState);
      // Need another flush after updateBucketChecksums().
      await this.flush();
    } finally {
      this.activeBucketDataCollection = null;
      this.activeBucketDefinitionId = LEGACY_BUCKET_DATA_DEFINITION_ID;
    }
  }

  protected updateBucketChecksums(state: CurrentBucketState) {
    if (state.opCount < 0) {
      throw new ServiceAssertionError(
        `Invalid opCount: ${state.opCount} checksum ${state.checksum} opsSincePut: ${state.opsSincePut} maxOpId: ${this.maxOpId}`
      );
    }
    this.bucketStateUpdates.push({
      updateOne: {
        filter: this.bucketStateFilter(state.bucket, state.definitionId),
        update: {
          $set: {
            compacted_state: {
              op_id: this.maxOpId,
              count: state.opCount,
              checksum: BigInt(state.checksum),
              bytes: state.opBytes
            },
            estimate_since_compact: {
              // There could have been a whole bunch of new operations added to the bucket while compacting,
              // which we don't currently cater for. We could potentially query for that, but that adds overhead.
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

  protected async flush() {
    if (this.updates.length > 0) {
      logger.info(`Compacting ${this.updates.length} ops`);
      if (this.activeBucketDataCollection == null) {
        throw new ServiceAssertionError('No bucket_data collection selected for compaction');
      }
      await this.activeBucketDataCollection.bulkWrite(this.updates, {
        // Order is not important. Since checksums are not affected, these operations can happen in any order,
        // and it's fine if the operations are partially applied. Each individual operation is atomic.
        ordered: false
      });
      this.updates = [];
    }
    if (this.bucketStateUpdates.length > 0) {
      logger.info(`Updating ${this.bucketStateUpdates.length} bucket states`);
      await this.flushBucketStateUpdates();
      this.bucketStateUpdates = [];
    }
  }

  /**
   * Perform a CLEAR compact for a bucket.
   *
   * @param currentState tracks the last non-PUT op, which will be converted to CLEAR.
   */
  protected async clearBucket(currentState: CurrentBucketState) {
    const bucket = currentState.bucket;
    const clearOp = currentState.lastNotPut!;
    const bucketCollection = this.activeBucketDataCollection;
    if (bucketCollection == null) {
      throw new ServiceAssertionError('No bucket_data collection selected for compaction');
    }

    const opFilter = {
      _id: {
        $gte: this.bucketDataKey(bucket, new mongo.MinKey() as any),
        $lte: this.bucketDataKey(bucket, clearOp)
      }
    };

    const session = this.db.client.startSession();
    try {
      let done = false;
      while (!done) {
        this.signal?.throwIfAborted();
        let opCountDiff = 0;
        // Do the CLEAR operation in batches, with each batch a separate transaction.
        // The state after each batch is fully consistent.
        // We need a transaction per batch to make sure checksums stay consistent.
        await session.withTransaction(
          async () => {
            const query = bucketCollection.find(opFilter as any, {
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
            let lastOp: CompactClearBucketDataDocument | null = null;
            let targetOp: bigint | null = null;
            let gotAnOp = false;
            let numberOfOpsToClear = 0;
            for await (const rawOp of query.stream()) {
              const op = this.tagClearBucketDataDocument(
                rawOp as unknown as BucketDataClearProjection,
                this.activeBucketDefinitionId
              );

              if (op.op == 'MOVE' || op.op == 'REMOVE' || op.op == 'CLEAR') {
                checksum = utils.addChecksums(checksum, Number(op.checksum));
                lastOp = op;
                numberOfOpsToClear += 1;
                if (op.op != 'CLEAR') {
                  gotAnOp = true;
                }
                if (op.target_op != null && (targetOp == null || op.target_op > targetOp)) {
                  targetOp = op.target_op;
                }
              } else {
                throw new ReplicationAssertionError(
                  `Unexpected ${op.op} operation at ${this.formatBucketDataKey(op._id as unknown as mongo.Document)}`
                );
              }
            }
            if (!gotAnOp) {
              done = true;
              return;
            }

            logger.info(`Flushing CLEAR for ${numberOfOpsToClear} ops at ${lastOp?._id.o}`);
            await bucketCollection.deleteMany(
              {
                _id: {
                  $gte: this.bucketDataKey(bucket, new mongo.MinKey() as any),
                  $lte: this.bucketDataKey(lastOp!._id.b, lastOp!._id.o)
                }
              } as any,
              { session } as any
            );

            await bucketCollection.insertOne(
              this.collectionBucketDataDocument({
                def: this.activeBucketDefinitionId,
                _id: lastOp!._id,
                op: 'CLEAR',
                checksum: BigInt(checksum),
                data: null,
                target_op: targetOp
              }) as unknown as mongo.OptionalId<mongo.Document>,
              { session } as any
            );

            opCountDiff = -numberOfOpsToClear + 1;
          },
          {
            writeConcern: { w: 'majority' },
            readConcern: { level: 'snapshot' }
          }
        );
        // Update outside the transaction, since the transaction can be retried multiple times.
        currentState.opCount += opCountDiff;
      }
    } finally {
      await session.endSession();
    }
  }

  protected async updateChecksumsBatch(buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]) {
    const checksums = await this.computeChecksumsForBuckets(buckets);
    const definitionIdByBucket = new Map(buckets.map((bucket) => [bucket.bucket, bucket.definitionId]));

    for (const bucketChecksum of checksums.values()) {
      if (isPartialChecksum(bucketChecksum)) {
        // Should never happen since we don't specify `start`.
        throw new ServiceAssertionError(`Full checksum expected, got ${JSON.stringify(bucketChecksum)}`);
      }

      this.bucketStateUpdates.push({
        updateOne: {
          filter: this.bucketStateFilter(
            bucketChecksum.bucket,
            definitionIdByBucket.get(bucketChecksum.bucket) ?? null
          ),
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
          // We don't create new ones here - it gets tricky to get the last_op right with the unique index on
          // bucket_updates.
          upsert: false
        }
      });
    }

    await this.flush();
  }

  protected tagBucketDataDocument(
    document: BucketDataCollectionDocument & { size: number | bigint },
    definitionId: BucketDefinitionId
  ): CompactBucketDataDocument {
    const tagged = bucketDataDocumentToTagged(document, definitionId);
    return {
      ...tagged,
      size: document.size
    };
  }

  protected tagClearBucketDataDocument(
    document: BucketDataClearProjection,
    definitionId: BucketDefinitionId
  ): CompactClearBucketDataDocument {
    return {
      def: definitionId,
      _id: {
        b: document._id.b,
        o: document._id.o
      },
      op: document.op,
      checksum: document.checksum,
      target_op: document.target_op
    };
  }

  protected formatBucketDataKey(key: mongo.Document) {
    const bucket = (key.b ?? key._id?.b) as string | undefined;
    const op = (key.o ?? key._id?.o) as bigint | undefined;
    return `${this.group_id}:${bucket ?? '?'}:${op ?? '?'}`;
  }

  protected abstract flushBucketStateUpdates(): Promise<void>;
  protected abstract computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap>;
  protected abstract bucketStateFilter(bucket: string, definitionId: BucketDefinitionId | null): mongo.Document;
  protected abstract bucketDataKey(bucket: string, opId: InternalOpId | mongo.MinKey | mongo.MaxKey): mongo.Document;
  protected abstract getBucketDataCollection(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): Promise<{ collection: mongo.Collection<mongo.Document>; definitionId: BucketDefinitionId } | null>;
  protected abstract collectionBucketDataDocument(
    document: TaggedBucketDataDocument
  ): BucketDataDocumentV1 | BucketDataDocumentV3;
}

class MongoCompactorV1 extends BaseMongoCompactor {
  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    // Previously, we used an index on {_id.g: 1, estimate_since_compact.count: 1} to only scan buckets with changes.
    // That works well if there are only a small number of dirty buckets, but it causes repeated rescans while data is
    // still changing. We now iterate through all V1 bucket_state rows for the group and filter after projecting.
    yield* this.dirtyBucketBatchesForCollection(
      this.db.bucketStateV1,
      { g: this.group_id, b: new mongo.MinKey() as any },
      { g: this.group_id, b: new mongo.MaxKey() as any },
      options,
      () => null
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    // Unlike dirtyBucketBatches, this path is resumable after restart because populateChecksums resets
    // estimate_since_compact as it progresses.
    return this.dirtyBucketBatchForChecksumsForCollection(
      this.db.bucketStateV1,
      {
        '_id.g': this.group_id,
        'estimate_since_compact.count': { $gte: options.minBucketChanges }
      },
      () => null
    );
  }

  protected async flushBucketStateUpdates(): Promise<void> {
    await this.db.bucketStateV1.bulkWrite(
      this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV1>[],
      {
        ordered: false
      }
    );
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return this.storage.checksums.computePartialChecksumsDirectV1(
      buckets.map(({ bucket }) => ({
        bucket,
        end: this.maxOpId
      }))
    );
  }

  protected bucketStateFilter(bucket: string, _definitionId: BucketDefinitionId | null): mongo.Document {
    return {
      _id: {
        g: this.group_id,
        b: bucket
      }
    };
  }

  protected bucketDataKey(bucket: string, opId: InternalOpId | mongo.MinKey | mongo.MaxKey): mongo.Document {
    return {
      g: this.group_id,
      b: bucket,
      o: opId as any
    };
  }

  protected async getBucketDataCollection(
    _bucket: string,
    _definitionId: BucketDefinitionId | null
  ): Promise<{ collection: mongo.Collection<mongo.Document>; definitionId: BucketDefinitionId } | null> {
    return {
      collection: this.db.v1_bucket_data as unknown as mongo.Collection<mongo.Document>,
      definitionId: LEGACY_BUCKET_DATA_DEFINITION_ID
    };
  }

  protected collectionBucketDataDocument(document: TaggedBucketDataDocument): BucketDataDocumentV1 {
    return taggedBucketDataDocumentToV1(this.group_id, document);
  }
}

class MongoCompactorV3 extends BaseMongoCompactor {
  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    // Same scan strategy as V1, but with the V3 bucket_state key shape.
    yield* this.dirtyBucketBatchesForCollection(
      this.db.bucketStateV3(this.group_id),
      { d: new mongo.MinKey() as any, b: new mongo.MinKey() as any },
      { d: new mongo.MaxKey() as any, b: new mongo.MaxKey() as any },
      options,
      (bucketState) => (bucketState as BucketStateDocumentV3)._id.d
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    return this.dirtyBucketBatchForChecksumsForCollection(
      this.db.bucketStateV3(this.group_id),
      {
        'estimate_since_compact.count': { $gte: options.minBucketChanges }
      },
      (bucketState) => (bucketState as BucketStateDocumentV3)._id.d
    );
  }

  protected async flushBucketStateUpdates(): Promise<void> {
    await this.db
      .bucketStateV3(this.group_id)
      .bulkWrite(this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV3>[], {
        ordered: false
      });
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return this.storage.checksums.computePartialChecksumsDirectV3(
      buckets.map(({ bucket, definitionId }) => {
        if (definitionId == null) {
          throw new ServiceAssertionError(`Missing definitionId for V3 bucket checksum update on bucket ${bucket}`);
        }
        return {
          bucket,
          definitionId,
          end: this.maxOpId
        };
      })
    );
  }

  protected bucketStateFilter(bucket: string, definitionId: BucketDefinitionId | null): mongo.Document {
    if (definitionId == null) {
      throw new ServiceAssertionError(`Missing definitionId for V3 bucket state filter on bucket ${bucket}`);
    }
    return {
      _id: {
        d: definitionId,
        b: bucket
      }
    };
  }

  protected bucketDataKey(bucket: string, opId: InternalOpId | mongo.MinKey | mongo.MaxKey): mongo.Document {
    return { b: bucket, o: opId as any };
  }

  protected async getBucketDataCollection(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): Promise<{ collection: mongo.Collection<mongo.Document>; definitionId: BucketDefinitionId } | null> {
    if (definitionId != null) {
      return {
        collection: this.db.bucket_data_v3(this.group_id, definitionId) as unknown as mongo.Collection<mongo.Document>,
        definitionId
      };
    }

    // FIXME: This is slow. It is only used when compacting a single bucket without a known definition id.
    for (const collection of await this.db.listBucketDataCollectionsV3(this.group_id)) {
      const existing = await collection.findOne(
        { '_id.b': bucket },
        { projection: { _id: 1 }, maxTimeMS: MONGO_OPERATION_TIMEOUT_MS }
      );
      if (existing != null) {
        const resolvedDefinitionId = collection.collectionName.replace(`bucket_data_${this.group_id}_`, '');
        return {
          collection: collection as unknown as mongo.Collection<mongo.Document>,
          definitionId: resolvedDefinitionId
        };
      }
    }

    return null;
  }

  protected collectionBucketDataDocument(document: TaggedBucketDataDocument): BucketDataDocumentV3 {
    return taggedBucketDataDocumentToV3(document);
  }
}

export class MongoCompactor {
  private readonly impl: BaseMongoCompactor;

  constructor(storage: MongoSyncBucketStorage, db: VersionedPowerSyncMongo, options: MongoCompactOptions) {
    if (db.storageConfig.incrementalReprocessing) {
      this.impl = new MongoCompactorV3(storage, db, options);
    } else {
      this.impl = new MongoCompactorV1(storage, db, options);
    }
  }

  async compact() {
    return this.impl.compact();
  }

  async populateChecksums(options: { minBucketChanges: number }): Promise<PopulateChecksumCacheResults> {
    return this.impl.populateChecksums(options);
  }

  dirtyBucketBatches(options: { minBucketChanges: number; minChangeRatio: number }): AsyncGenerator<DirtyBucket[]> {
    return this.impl.dirtyBucketBatches(options);
  }

  dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    return this.impl.dirtyBucketBatchForChecksums(options);
  }
}
