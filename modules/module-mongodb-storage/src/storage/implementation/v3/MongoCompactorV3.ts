import { mongo } from '@powersync/lib-service-mongodb';
import { logger, ServiceAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, storage, utils } from '@powersync/service-core';
import { chunkBucketData } from '../bucket-operations/chunking.js';
import {
  computeChecksumsForBuckets,
  dirtyBucketBatches,
  dirtyBucketBatchForChecksums
} from '../bucket-operations/compaction-scaffolding.js';
import { bucketStateFilter, resolveBucketDefinitionId } from '../bucket-operations/query-builders.js';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { BucketStateDocument } from '../common/models.js';
import { BucketDataDocumentGeneric, SingleBucketStore } from '../common/SingleBucketStore.js';
import {
  BucketDataDocument,
  loadBucketDataDocument,
  serializeBucketData
} from '../document-formats/bucket-document-format.js';
import { BucketStateDocumentBase } from '../models.js';
import { DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import { cacheKey } from '../OperationBatch.js';
import { MongoChecksumsV3 } from './MongoChecksumsV3.js';
import type { MongoSyncBucketStorageV3 } from './MongoSyncBucketStorageV3.js';
import { SingleBucketStoreV3 } from './SingleBucketStoreV3.js';

export class MongoCompactorV3 extends MongoCompactor {
  get db(): VersionedPowerSyncMongo {
    return super.db as VersionedPowerSyncMongo;
  }

  get storage(): MongoSyncBucketStorageV3 {
    return super.storage as MongoSyncBucketStorageV3;
  }

  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    yield* dirtyBucketBatches(
      this,
      this.db.bucketState<BucketStateDocumentBase>(this.group_id),
      options,
      (bucketState) => (bucketState as BucketStateDocument)._id.d
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    return dirtyBucketBatchForChecksums(
      this,
      this.db.bucketState<BucketStateDocumentBase>(this.group_id),
      options,
      (bucketState) => (bucketState as BucketStateDocument)._id.d
    );
  }

  protected async writeBucketStateUpdates(): Promise<void> {
    await this.db
      .bucketState<BucketStateDocument>(this.group_id)
      .bulkWrite(this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocument>[], {
        ordered: false
      });
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return computeChecksumsForBuckets(
      (batch) => (this.storage.checksums as MongoChecksumsV3).computePartialChecksumsDirectByDefinition(batch),
      this.maxOpId,
      buckets
    );
  }

  protected bucketStateFilter(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): mongo.Filter<BucketStateDocumentBase> {
    if (definitionId == null) {
      throw new ServiceAssertionError(`Missing definitionId for V3 bucket state filter on bucket ${bucket}`);
    }
    return bucketStateFilter(bucket, definitionId);
  }

  protected async getBucketDataContext(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): Promise<SingleBucketStore | null> {
    const resolvedDefinitionId = await resolveBucketDefinitionId(
      {
        bucket,
        definitionId,
        allDefinitionIds: this.storage.mapping.allBucketDefinitionIds(),
        groupId: this.group_id
      },
      async (potentialIds) => {
        const bucketState = await this.db.bucketState<BucketStateDocument>(this.group_id).findOne({
          _id: { $in: potentialIds }
        });
        return bucketState ? { definitionId: bucketState._id.d } : null;
      }
    );

    if (resolvedDefinitionId == null) {
      return null;
    }

    return new SingleBucketStoreV3(this.db, {
      bucket,
      definitionId: resolvedDefinitionId,
      replicationStreamId: this.group_id
    });
  }

  protected override async compactSingleBucket(bucket: string, definitionId: BucketDefinitionId | null = null) {
    const bucketContext = await this.getBucketDataContext(bucket, definitionId);
    if (bucketContext == null) {
      return;
    }

    const resolvedDefinitionId = bucketContext.key.definitionId;
    const collection = this.db.bucketData(this.group_id, resolvedDefinitionId);
    const context = { replicationStreamId: this.group_id, definitionId: resolvedDefinitionId };

    const lowerBound = bucketContext.minId;
    let upperBound = bucketContext.maxId;

    let totalChecksum = 0;
    let totalOpCount = 0;
    let totalOpBytes = 0;

    const seen = new Map<string, bigint>();
    let trackingSize = 0;

    while (true) {
      this.signal?.throwIfAborted();

      const pipeline: mongo.Document[] = [
        {
          $match: {
            '_id.b': bucket,
            _id: {
              $gte: lowerBound,
              $lt: upperBound
            },
            '_id.o': { $lt: upperBound.o }
          }
        },
        { $sort: { _id: -1 } },
        { $limit: this.moveBatchQueryLimit },
        {
          $project: {
            _id: 1,
            min_op: 1,
            checksum: 1,
            count: 1,
            size: 1,
            target_op: 1,
            ops: 1,
            bsonSize: { $bsonSize: '$$ROOT' }
          }
        }
      ];

      const rawBatch = await collection
        .aggregate<BucketDataDocument & { bsonSize: number | bigint }>(pipeline, {
          batchSize: this.moveBatchQueryLimit + 1
        })
        .toArray();

      if (rawBatch.length == 0) {
        break;
      }

      let cumulativeBytes = 0;
      let batchCutIndex = rawBatch.length;

      for (let i = 0; i < rawBatch.length; i++) {
        cumulativeBytes += Number(rawBatch[i].bsonSize);
        if (cumulativeBytes > this.moveBatchByteLimit && i > 0) {
          batchCutIndex = i;
          break;
        }
      }

      const batchDocs = rawBatch.slice(0, batchCutIndex);

      // Decode ops from batch documents, filtering out ops above maxOpId.
      // Track which documents have at least one op <= maxOpId — only those
      // are included in the scoped delete range.
      const batchOps: BucketDataDoc[] = [];
      const processableDocs: (BucketDataDocument & { bsonSize: number | bigint })[] = [];

      for (const doc of batchDocs) {
        let hasRelevantOp = false;
        for (const op of loadBucketDataDocument(context, doc as unknown as BucketDataDocument)) {
          if (op.o <= this.maxOpId) {
            batchOps.push(op);
            hasRelevantOp = true;
          }
        }
        if (hasRelevantOp) {
          processableDocs.push(doc);
        }
      }

      if (processableDocs.length == 0) {
        // No documents with relevant ops in this batch; paginate to next batch
        upperBound = batchDocs[batchDocs.length - 1]._id as typeof upperBound;
        if (batchCutIndex >= rawBatch.length && rawBatch.length < this.moveBatchQueryLimit) {
          break;
        }
        continue;
      }

      // Scoped replace in a bounded transaction.
      // Delete by individual _id values instead of a continuous range.
      // A continuous range could catch non-processable documents (all ops > maxOpId)
      // that happen to fall between processable documents in _id.o sort order.
      const idsToDelete = processableDocs.map((d) => d._id);

      // Sort ops by o descending for newest-first dedup
      batchOps.sort((a, b) => (b.o > a.o ? 1 : b.o < a.o ? -1 : 0));

      // Dedup: process newest-to-oldest
      const surviving: BucketDataDoc[] = [];

      for (const op of batchOps) {
        if (op.op == 'PUT' || op.op == 'REMOVE') {
          const key = `${op.table}/${op.row_id}/${cacheKey(op.source_table!, op.source_key!)}`;
          const targetOp = seen.get(key);
          if (targetOp != null) {
            surviving.push({
              ...op,
              op: 'MOVE',
              target_op: targetOp,
              table: undefined,
              row_id: undefined,
              source_table: undefined,
              source_key: undefined,
              data: null
            });
          } else {
            if (trackingSize < this.idLimitBytes) {
              seen.set(utils.flatstr(key), op.o);
              trackingSize += key.length + 140;
            }
            surviving.push(op);
          }
        } else {
          surviving.push(op);
        }
      }

      // Reverse back to ascending order for rechunking
      surviving.reverse();

      // Rechunk surviving ops
      const chunks = chunkBucketData(surviving);
      const newDocs = chunks.map((chunk) => serializeBucketData(bucket, chunk));

      const session = this.db.client.startSession();
      try {
        await session.withTransaction(
          async () => {
            await bucketContext.collection.deleteMany(
              {
                _id: { $in: idsToDelete }
              } as any,
              { session }
            );
            if (newDocs.length > 0) {
              await bucketContext.collection.insertMany(newDocs as unknown as BucketDataDocumentGeneric[], { session });
            }
          },
          {
            writeConcern: { w: 'majority' },
            readConcern: { level: 'snapshot' }
          }
        );
      } finally {
        await session.endSession();
      }

      // Accumulate bucket state
      for (const chunk of chunks) {
        for (const op of chunk) {
          totalChecksum = addChecksums(totalChecksum, Number(op.checksum));
          totalOpBytes += op.data?.length ?? 0;
        }
      }
      totalOpCount += surviving.length;

      // Update upperBound for next batch pagination
      upperBound = rawBatch[batchCutIndex - 1]._id as typeof upperBound;

      if (batchCutIndex < rawBatch.length) {
        // We cut the batch short due to byte limit — don't advance past cut point
        // The upperBound is already set to the last doc we processed
      } else {
        // Processed all docs in the raw batch — check if we got fewer than the limit
        if (rawBatch.length < this.moveBatchQueryLimit) {
          break;
        }
      }

      this.logger.info(`Compacted batch of ${batchDocs.length} documents for bucket ${bucket}`);
    }

    this.updateBucketChecksums({
      bucket,
      definitionId: resolvedDefinitionId,
      seen: new Map(),
      trackingSize: 0,
      lastNotPut: null,
      opsSincePut: 0,
      checksum: totalChecksum,
      opCount: totalOpCount,
      opBytes: totalOpBytes
    });
    if (this.bucketStateUpdates.length > 0) {
      await this.writeBucketStateUpdates();
      this.bucketStateUpdates = [];
    }

    logger.info(`Compacted bucket ${bucket}: ${totalOpCount} surviving ops`);
  }
}
