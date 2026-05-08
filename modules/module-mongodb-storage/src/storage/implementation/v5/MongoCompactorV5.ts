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
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { SingleBucketStore } from '../common/SingleBucketStore.js';
import { BucketStateDocumentBase } from '../models.js';
import { DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import type { MongoSyncBucketStorage } from '../MongoSyncBucketStorage.js';
import { cacheKey } from '../OperationBatch.js';
import {
  BucketDataDocumentV5,
  BucketStateDocument,
  loadBucketDataDocumentV5,
  serializeBucketDataV5
} from './models.js';
import { MongoChecksumsV5 } from './MongoChecksumsV5.js';
import { SingleBucketStoreV5 } from './SingleBucketStoreV5.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoCompactorV5 extends MongoCompactor {
  get db(): VersionedPowerSyncMongoV5 {
    return super.db as VersionedPowerSyncMongoV5;
  }

  get storage(): MongoSyncBucketStorage {
    return super.storage as MongoSyncBucketStorage;
  }

  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    yield* dirtyBucketBatches(
      this,
      this.db.bucketStateV5(this.group_id),
      options,
      (bucketState) => (bucketState as BucketStateDocument)._id.d
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    return dirtyBucketBatchForChecksums(
      this,
      this.db.bucketStateV5(this.group_id),
      options,
      (bucketState) => (bucketState as BucketStateDocument)._id.d
    );
  }

  protected async writeBucketStateUpdates(): Promise<void> {
    await this.db
      .bucketStateV5(this.group_id)
      .bulkWrite(this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocument>[], {
        ordered: false
      });
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return computeChecksumsForBuckets(
      (batch) => (this.storage.checksums as MongoChecksumsV5).computePartialChecksumsDirectByDefinition(batch),
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

  private async loadBucketOps(bucket: string, definitionId: BucketDefinitionId): Promise<BucketDataDoc[] | null> {
    const docs = await this.db
      .bucketDataV5(this.group_id, definitionId)
      .find({ '_id.b': bucket })
      .sort({ '_id.o': 1 })
      .toArray();

    if (docs.length == 0) {
      return null;
    }

    this.signal?.throwIfAborted();

    const bucketLoadContext = { replicationStreamId: this.group_id, definitionId };
    const allOps: BucketDataDoc[] = [];

    for (const doc of docs) {
      for (const op of loadBucketDataDocumentV5(bucketLoadContext, doc as unknown as BucketDataDocumentV5)) {
        if (op.o > this.maxOpId) {
          continue;
        }
        allOps.push(op);
      }
    }

    if (allOps.length == 0) {
      return null;
    }

    this.signal?.throwIfAborted();
    return allOps;
  }

  private filterSupersededOps(allOps: BucketDataDoc[]): BucketDataDoc[] {
    const lastOpByRowKey = new Map<string, bigint>();
    const retained = new Array<BucketDataDoc | null>(allOps.length);

    for (let i = allOps.length - 1; i >= 0; i--) {
      const op = allOps[i];

      if (op.op == 'PUT' || op.op == 'REMOVE') {
        const key = `${op.table}/${op.row_id}/${cacheKey(op.source_table!, op.source_key!)}`;
        if (lastOpByRowKey.has(key)) {
          // Superseded by a newer operation for the same row — drop it.
          retained[i] = null;
        } else {
          lastOpByRowKey.set(utils.flatstr(key), op.o);
          retained[i] = op;
        }
      } else {
        // MOVE / CLEAR — preserve (these only exist if compaction was run before).
        retained[i] = op;
      }
    }

    return retained.filter((op): op is BucketDataDoc => op != null);
  }

  private async prepareRetainedOps(
    bucketContext: SingleBucketStore,
    bucket: string,
    definitionId: BucketDefinitionId
  ): Promise<void> {
    await bucketContext.collection.deleteMany({ '_id.b': bucket });
    const update = this.collectBucketStateUpdates({
      bucket,
      definitionId,
      seen: new Map(),
      trackingSize: 0,
      lastNotPut: null,
      opsSincePut: 0,
      checksum: 0,
      opCount: 0,
      opBytes: 0
    });
    await this.flushPendingBucketStateUpdates([update]);
  }

  private async writeCompactedChunks(bucketContext: SingleBucketStore, chunks: BucketDataDoc[][]): Promise<void> {
    const bucket = bucketContext.key.bucket;
    const newDocs = chunks.map((chunk) => serializeBucketDataV5(bucket, chunk));

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
  }

  private computeChecksumAndBytes(chunks: BucketDataDoc[][]): { checksum: number; opBytes: number } {
    let totalChecksum = 0;
    let totalOpBytes = 0;
    for (const chunk of chunks) {
      for (const op of chunk) {
        totalChecksum = addChecksums(totalChecksum, Number(op.checksum));
        totalOpBytes += op.data?.length ?? 0;
      }
    }
    return { checksum: totalChecksum, opBytes: totalOpBytes };
  }

  private async updateBucketStateAndFlush(
    bucket: string,
    definitionId: BucketDefinitionId,
    checksum: number,
    opCount: number,
    opBytes: number
  ): Promise<void> {
    const update = this.collectBucketStateUpdates({
      bucket,
      definitionId,
      seen: new Map(),
      trackingSize: 0,
      lastNotPut: null,
      opsSincePut: 0,
      checksum,
      opCount,
      opBytes
    });
    await this.flushPendingBucketStateUpdates([update]);
  }

  protected override async compactSingleBucket(bucket: string, definitionId: BucketDefinitionId | null = null) {
    const bucketContext = await this.getBucketDataContext(bucket, definitionId);
    if (bucketContext == null) {
      return;
    }

    const resolvedDefinitionId = bucketContext.key.definitionId;

    const allOps = await this.loadBucketOps(bucket, resolvedDefinitionId);
    if (allOps == null) {
      return;
    }

    const retainedOps = this.filterSupersededOps(allOps);
    if (retainedOps.length == 0) {
      await this.prepareRetainedOps(bucketContext, bucket, resolvedDefinitionId);
      return;
    }

    this.signal?.throwIfAborted();

    const chunks = chunkBucketData(retainedOps);
    await this.writeCompactedChunks(bucketContext, chunks);

    const { checksum, opBytes } = this.computeChecksumAndBytes(chunks);
    await this.updateBucketStateAndFlush(bucket, resolvedDefinitionId, checksum, retainedOps.length, opBytes);

    logger.info(
      `Compacted bucket ${bucket}: ${allOps.length} ops → ${retainedOps.length} ops in ${chunks.length} documents`
    );
  }

  /**
   * Flushes the given bucket state updates to MongoDB.
   *
   * Accepts updates as an explicit parameter to avoid hidden state
   * coupling with {@link collectBucketStateUpdates}.
   */
  private async flushPendingBucketStateUpdates(updates: mongo.AnyBulkWriteOperation<BucketStateDocumentBase>[]) {
    if (updates.length > 0) {
      logger.info(`Updating ${updates.length} bucket states`);
      this.bucketStateUpdates = updates;
      await this.writeBucketStateUpdates();
      this.bucketStateUpdates = [];
    }
  }
}
