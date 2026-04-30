import { mongo } from '@powersync/lib-service-mongodb';
import { logger, ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, storage, utils } from '@powersync/service-core';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { SingleBucketStore } from '../common/SingleBucketStore.js';
import { BucketStateDocumentBase } from '../models.js';
import { DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import { cacheKey } from '../OperationBatch.js';
import {
  BucketDataDocumentV5,
  BucketStateDocumentV5,
  loadBucketDataDocumentV5,
  serializeBucketDataV5
} from './models.js';
import type { MongoSyncBucketStorageV5 } from './MongoSyncBucketStorageV5.js';
import { SingleBucketStoreV5 } from './SingleBucketStoreV5.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoCompactorV5 extends MongoCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV5;
  declare protected readonly storage: MongoSyncBucketStorageV5;

  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    // Same scan strategy as V1, but with the V5 bucket_state key shape.
    yield* this.dirtyBucketBatchesForCollection(
      this.db.bucketStateV5(this.group_id),
      { d: new mongo.MinKey() as any, b: new mongo.MinKey() as any },
      { d: new mongo.MaxKey() as any, b: new mongo.MaxKey() as any },
      options,
      (bucketState) => (bucketState as BucketStateDocumentV5)._id.d
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    return this.dirtyBucketBatchForChecksumsForCollection(
      this.db.bucketStateV5(this.group_id),
      {
        'estimate_since_compact.count': { $gte: options.minBucketChanges }
      },
      (bucketState) => (bucketState as BucketStateDocumentV5)._id.d
    );
  }

  protected async writeBucketStateUpdates(): Promise<void> {
    await this.db
      .bucketStateV5(this.group_id)
      .bulkWrite(this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV5>[], { ordered: false });
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return this.storage.checksums.computePartialChecksumsDirectByDefinition(
      buckets.map(({ bucket, definitionId }) => {
        if (definitionId == null) {
          throw new ServiceAssertionError(`Missing definitionId for V5 bucket checksum update on bucket ${bucket}`);
        }
        return {
          bucket,
          definitionId,
          end: this.maxOpId
        };
      })
    );
  }

  protected bucketStateFilter(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): mongo.Filter<BucketStateDocumentBase> {
    if (definitionId == null) {
      throw new ServiceAssertionError(`Missing definitionId for V5 bucket state filter on bucket ${bucket}`);
    }
    return {
      _id: {
        d: definitionId,
        b: bucket
      }
    };
  }

  protected async getBucketDataContext(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): Promise<SingleBucketStore | null> {
    if (definitionId == null) {
      // Not the _most_ efficient approach, but this is not used often
      const allDefinitionIds = this.storage.mapping.allBucketDefinitionIds();
      if (allDefinitionIds.length == 0) {
        return null;
      }
      const potentialIds = allDefinitionIds.map((definitionId) => ({ d: definitionId, b: bucket }));
      const bucketState = await this.db.bucketStateV5(this.group_id).findOne({
        _id: { $in: potentialIds }
      });
      if (bucketState == null) {
        return null;
      }
      definitionId = bucketState._id.d;
    }

    return new SingleBucketStoreV5(this.db, { bucket, definitionId, replicationStreamId: this.group_id });
  }

  protected override async compactSingleBucket(bucket: string, definitionId: BucketDefinitionId | null = null) {
    const bucketContext = await this.getBucketDataContext(bucket, definitionId);
    if (bucketContext == null) {
      return;
    }

    const resolvedDefinitionId = bucketContext.key.definitionId;

    // 1. Read all documents in the bucket sorted by max op_id ascending
    const docs = await this.db
      .bucketDataV5(this.group_id, resolvedDefinitionId)
      .find({ '_id.b': bucket })
      .sort({ '_id.o': 1 })
      .toArray();

    if (docs.length == 0) {
      return;
    }

    this.signal?.throwIfAborted();

    // 2. Load all operations from all documents
    const context = { replicationStreamId: this.group_id, definitionId: resolvedDefinitionId };
    const allOps: BucketDataDoc[] = [];

    for (const doc of docs) {
      for (const op of loadBucketDataDocumentV5(context, doc as unknown as BucketDataDocumentV5)) {
        if (op.o > this.maxOpId) {
          continue;
        }
        allOps.push(op);
      }
    }

    if (allOps.length == 0) {
      return;
    }

    this.signal?.throwIfAborted();

    // 3. Filter superseded operations using the same row_id logic as v3.
    //    We iterate newest-to-oldest and keep only the latest PUT/REMOVE per row.
    const seen = new Map<string, bigint>();
    const surviving = new Array<BucketDataDoc | null>(allOps.length);

    for (let i = allOps.length - 1; i >= 0; i--) {
      const op = allOps[i];

      if (op.op == 'PUT' || op.op == 'REMOVE') {
        const key = `${op.table}/${op.row_id}/${cacheKey(op.source_table!, op.source_key!)}`;
        if (seen.has(key)) {
          // Superseded by a newer operation for the same row — drop it.
          surviving[i] = null;
        } else {
          seen.set(utils.flatstr(key), op.o);
          surviving[i] = op;
        }
      } else {
        // MOVE / CLEAR — preserve (these only exist if compaction was run before).
        surviving[i] = op;
      }
    }

    const survivingOps = surviving.filter((op): op is BucketDataDoc => op != null);

    if (survivingOps.length == 0) {
      // Nothing left — delete all old documents.
      await bucketContext.collection.deleteMany({ '_id.b': bucket });
      this.updateBucketChecksums({
        bucket,
        definitionId: resolvedDefinitionId,
        seen: new Map(),
        trackingSize: 0,
        lastNotPut: null,
        opsSincePut: 0,
        checksum: 0,
        opCount: 0,
        opBytes: 0
      });
      await this.flushBucketStateUpdatesOnly();
      return;
    }

    this.signal?.throwIfAborted();

    // 4. Re-chunk surviving operations by 1MB data-size threshold.
    const chunks = this.chunkBucketData(survivingOps);
    const newDocs = chunks.map((chunk) => serializeBucketDataV5(bucket, chunk));

    // 5. Replace old documents with new chunked documents in a transaction.
    const session = this.db.client.startSession();
    try {
      await session.withTransaction(
        async () => {
          await bucketContext.collection.deleteMany({ '_id.b': bucket }, { session });
          if (newDocs.length > 0) {
            await bucketContext.collection.insertMany(newDocs as any, { session });
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

    // 6. Update bucket state with new aggregates.
    let totalChecksum = 0;
    let totalOpBytes = 0;
    for (const chunk of chunks) {
      for (const op of chunk) {
        totalChecksum = addChecksums(totalChecksum, Number(op.checksum));
        totalOpBytes += op.data?.length ?? 0;
      }
    }

    this.updateBucketChecksums({
      bucket,
      definitionId: resolvedDefinitionId,
      seen: new Map(),
      trackingSize: 0,
      lastNotPut: null,
      opsSincePut: 0,
      checksum: totalChecksum,
      opCount: survivingOps.length,
      opBytes: totalOpBytes
    });
    await this.flushBucketStateUpdatesOnly();

    logger.info(
      `Compacted bucket ${bucket}: ${allOps.length} ops → ${survivingOps.length} ops in ${newDocs.length} documents`
    );
  }

  private chunkBucketData(operations: BucketDataDoc[]): BucketDataDoc[][] {
    const chunks: BucketDataDoc[][] = [];
    let currentChunk: BucketDataDoc[] = [];
    let currentSize = 0;
    const maxDocSize = 1024 * 1024; // 1MB threshold

    for (const op of operations) {
      const opSize = op.data?.length ?? 0;

      if (currentSize + opSize > maxDocSize && currentChunk.length > 0) {
        chunks.push(currentChunk);
        currentChunk = [];
        currentSize = 0;
      }

      currentChunk.push(op);
      currentSize += opSize;
    }

    if (currentChunk.length > 0) {
      chunks.push(currentChunk);
    }

    return chunks;
  }

  private async flushBucketStateUpdatesOnly() {
    if (this.bucketStateUpdates.length > 0) {
      logger.info(`Updating ${this.bucketStateUpdates.length} bucket states`);
      await this.writeBucketStateUpdates();
      this.bucketStateUpdates = [];
    }
  }
}
