import { mongo } from '@powersync/lib-service-mongodb';
import { logger, ServiceAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, storage, utils } from '@powersync/service-core';
import { chunkBucketData } from '../bucket-operations/chunking.js';
import {
  computeChecksumsForBuckets,
  dirtyBucketBatches,
  dirtyBucketBatchForChecksums,
  writeBucketStateUpdates
} from '../bucket-operations/compaction-scaffolding.js';
import { bucketStateFilter, resolveBucketDefinitionId } from '../bucket-operations/query-builders.js';
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
    yield* dirtyBucketBatches(
      this,
      this.db.bucketStateV5(this.group_id),
      options,
      (bucketState) => (bucketState as BucketStateDocumentV5)._id.d
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    return dirtyBucketBatchForChecksums(
      this,
      this.db.bucketStateV5(this.group_id),
      options,
      (bucketState) => (bucketState as BucketStateDocumentV5)._id.d
    );
  }

  protected async writeBucketStateUpdates(): Promise<void> {
    await writeBucketStateUpdates(
      this.db.bucketStateV5(this.group_id),
      this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV5>[]
    );
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return computeChecksumsForBuckets(
      (batch) => this.storage.checksums.computePartialChecksumsDirectByDefinition(batch),
      this.maxOpId,
      buckets
    );
  }

  protected bucketStateFilter(
    bucket: string,
    definitionId: BucketDefinitionId | null
  ): mongo.Filter<BucketStateDocumentBase> {
    if (definitionId == null) {
      throw new ServiceAssertionError(`Missing definitionId for V5 bucket state filter on bucket ${bucket}`);
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
        const bucketState = await this.db.bucketStateV5(this.group_id).findOne({
          _id: { $in: potentialIds }
        });
        return bucketState ? { definitionId: bucketState._id.d } : null;
      }
    );

    if (resolvedDefinitionId == null) {
      return null;
    }

    return new SingleBucketStoreV5(this.db, {
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
    const chunks = chunkBucketData(survivingOps);
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

  private async flushBucketStateUpdatesOnly() {
    if (this.bucketStateUpdates.length > 0) {
      logger.info(`Updating ${this.bucketStateUpdates.length} bucket states`);
      await this.writeBucketStateUpdates();
      this.bucketStateUpdates = [];
    }
  }
}
