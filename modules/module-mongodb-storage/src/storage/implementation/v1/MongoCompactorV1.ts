import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, storage, utils } from '@powersync/service-core';
import { BucketDefinitionId } from '@powersync/service-sync-rules';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { BucketStateDocumentBase, LEGACY_BUCKET_DATA_DEFINITION_ID } from '../models.js';
import { CurrentBucketState, DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import { cacheKey } from '../OperationBatch.js';
import { BucketDataDocumentV1, BucketStateDocumentV1 } from './models.js';
import type { MongoSyncBucketStorageV1 } from './MongoSyncBucketStorageV1.js';
import { SingleBucketStoreV1 } from './SingleBucketStoreV1.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';

type CompactClearProperties = 'op' | 'checksum' | 'target_op';

export class MongoCompactorV1 extends MongoCompactor {
  // Override types to the more specific ones
  declare protected readonly db: VersionedPowerSyncMongoV1;
  declare protected readonly storage: MongoSyncBucketStorageV1;

  private updates: mongo.AnyBulkWriteOperation<BucketDataDocumentV1>[] = [];

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

  protected async writeBucketStateUpdates(): Promise<void> {
    await this.db.bucketStateV1.bulkWrite(
      this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV1>[],
      { ordered: false }
    );
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return this.storage.checksums.computePartialChecksumsDirectByBucket(
      buckets.map(({ bucket }) => ({
        bucket,
        end: this.maxOpId
      })),
      { readOptions: { readConcern: 'snapshot' } }
    );
  }

  protected bucketStateFilter(
    bucket: string,
    _definitionId: BucketDefinitionId | null
  ): mongo.Filter<BucketStateDocumentBase> {
    return {
      _id: {
        g: this.group_id,
        b: bucket
      }
    };
  }

  private getBucketDataContext(bucket: string, _definitionId: BucketDefinitionId | null): SingleBucketStoreV1 {
    return new SingleBucketStoreV1(this.db, {
      replicationStreamId: this.group_id,
      definitionId: LEGACY_BUCKET_DATA_DEFINITION_ID,
      bucket
    });
  }

  protected async compactSingleBucket(bucket: string, definitionId: BucketDefinitionId | null = null) {
    // Do not carry queued writes from a failed attempt into the rescan.
    this.updates = [];

    const idLimitBytes = this.idLimitBytes;
    const bucketContext = this.getBucketDataContext(bucket, definitionId);
    const currentState: CurrentBucketState = {
      bucket,
      definitionId: bucketContext.key.definitionId,
      seen: new Map(),
      trackingSize: 0,
      lastNotPut: null,
      opsSincePut: 0,
      checksum: 0,
      opCount: 0,
      opBytes: 0
    };

    // Constant lower bound.
    const lowerBound = bucketContext.minId;
    // Upper bound is adjusted for each batch.
    let upperBound = bucketContext.maxId;

    // Paginate through bucket data in batches to avoid cursor timeouts.
    while (true) {
      this.signal?.throwIfAborted();

      // Query one batch at a time, to avoid cursor timeouts.
      const pipeline = [
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
      ];

      const cursor = bucketContext.collection.aggregate<BucketDataDocumentV1 & { size: number | bigint }>(pipeline, {
        // batchSize is 1 more than limit to auto-close the cursor.
        // See https://github.com/mongodb/node-mongodb-native/pull/4580
        batchSize: this.moveBatchQueryLimit + 1
      });
      // We don't limit to a single batch here, since that often causes MongoDB to scan through more than it returns.
      // Instead, we load up to the limit.
      const rawBatch = await cursor.toArray();
      const batch = rawBatch.map((document) => {
        const { size, ...rest } = document;
        return {
          doc: bucketContext.fromPersistedDocument(rest),
          size
        };
      });

      if (batch.length == 0) {
        // We've reached the end.
        break;
      }

      // Reuse the exact collection _id value from Mongo for the next bound.
      upperBound = rawBatch[rawBatch.length - 1]._id;

      for (const { doc, size } of batch) {
        if (doc.o > this.maxOpId) {
          continue;
        }

        currentState.checksum = addChecksums(currentState.checksum, Number(doc.checksum));
        currentState.opCount += 1;

        let isPersistentPut = doc.op == 'PUT';

        currentState.opBytes += Number(size);
        if (doc.op == 'REMOVE' || doc.op == 'PUT') {
          const key = `${doc.table}/${doc.row_id}/${cacheKey(doc.source_table!, doc.source_key!)}`;
          const targetOp = currentState.seen.get(key);
          if (targetOp) {
            // Will convert to MOVE, so don't count as PUT.
            isPersistentPut = false;

            this.updates.push({
              updateOne: {
                filter: { _id: bucketContext.docId(doc.o) },
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
                } satisfies mongo.UpdateFilter<BucketDataDocumentV1>
              }
            });

            // TODO: better estimate for this.
            currentState.opBytes += 200 - Number(size);
          } else if (currentState.trackingSize < idLimitBytes) {
            // flatstr reduces the memory usage by flattening the string.
            currentState.seen.set(utils.flatstr(key), doc.o);
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
            currentState.lastNotPut = doc.o;
          }
          currentState.opsSincePut += 1;
        }

        if (this.updates.length + this.bucketStateUpdates.length >= this.moveBatchLimit) {
          await this.flush(bucketContext);
        }
      }

      this.logger.info(`Processed batch of length ${batch.length} current bucket: ${bucket}`);
    }

    // Free memory before clearing the bucket.
    currentState.seen.clear();
    if (currentState.lastNotPut != null && currentState.opsSincePut >= 1) {
      this.logger.info(
        `Inserting CLEAR at ${this.group_id}:${bucket}:${currentState.lastNotPut} to remove ${currentState.opsSincePut} operations`
      );
      // Need flush() before clear().
      await this.flush(bucketContext);
      await this.clearBucket(currentState, bucketContext);
    }

    // Do this after clearBucket so we have accurate counts.
    this.updateBucketChecksums(currentState, this.maxOpId);
    // Need another flush after updateBucketChecksums().
    await this.flush(bucketContext);
  }

  private async flush(bucketContext: SingleBucketStoreV1) {
    if (this.updates.length > 0) {
      this.logger.info(`Compacting ${this.updates.length} ops`);
      await bucketContext.collection.bulkWrite(this.updates, {
        // Order is not important. Since checksums are not affected, these operations can happen in any order,
        // and it's fine if the operations are partially applied. Each individual operation is atomic.
        ordered: false
      });
      this.updates = [];
    }

    await this.flushBucketStateUpdates();
  }

  /**
   * Perform a CLEAR compact for a bucket.
   *
   * @param currentState tracks the last non-PUT op, which will be converted to CLEAR.
   */
  private async clearBucket(currentState: CurrentBucketState, bucketContext: SingleBucketStoreV1) {
    const clearOp = currentState.lastNotPut!;

    const opFilter = {
      _id: {
        $gte: bucketContext.minId,
        $lte: bucketContext.docId(clearOp)
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
            const query = bucketContext.collection.find<Pick<BucketDataDocumentV1, '_id' | CompactClearProperties>>(
              opFilter,
              {
                session,
                sort: { _id: 1 },
                projection: {
                  _id: 1,
                  op: 1,
                  checksum: 1,
                  target_op: 1
                },
                limit: this.clearBatchLimit
              }
            );
            let checksum = 0;
            let lastOp: Pick<BucketDataDoc, 'o' | CompactClearProperties> | null = null;
            let targetOp: bigint | null = null;
            let gotAnOp = false;
            let numberOfOpsToClear = 0;
            for await (const rawOp of query.stream()) {
              const op = bucketContext.fromPartialPersistedDocument(rawOp);

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
                throw new ReplicationAssertionError(`Unexpected ${op.op} operation at ${this.formatBucketDataKey(op)}`);
              }
            }
            if (!gotAnOp) {
              done = true;
              return;
            }

            this.logger.info(`Flushing CLEAR for ${numberOfOpsToClear} ops at ${lastOp?.o}`);
            await bucketContext.collection.deleteMany(
              {
                _id: {
                  $gte: bucketContext.minId,
                  $lte: bucketContext.docId(lastOp!.o)
                }
              },
              { session }
            );

            const op = bucketContext.toPersistedDocument({
              o: lastOp!.o,
              op: 'CLEAR',
              checksum: BigInt(checksum),
              data: null,
              target_op: targetOp
            });
            await bucketContext.collection.insertOne(op, { session });

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

  private formatBucketDataKey(doc: Pick<BucketDataDoc, 'bucketKey' | 'o'>) {
    return `${doc.bucketKey.replicationStreamId}:${doc.bucketKey.bucket}:${doc.o}`;
  }
}
