import { mongo } from '@powersync/lib-service-mongodb';
import { logger, ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, storage, utils } from '@powersync/service-core';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { BucketDataDocumentGeneric, SingleBucketStore } from '../common/SingleBucketStore.js';
import { BucketStateDocumentBase } from '../models.js';
import { DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import { cacheKey } from '../OperationBatch.js';
import { loadBucketDataDocument, serializeBucketData } from './bucket-format.js';
import { chunkBucketData } from './chunking.js';
import { BucketDataDocumentV3, BucketStateDocumentV3 } from './models.js';
import { MongoChecksumsV3 } from './MongoChecksumsV3.js';
import type { MongoSyncBucketStorageV3 } from './MongoSyncBucketStorageV3.js';
import { BucketDataObjectStorage } from './object-storage/BucketDataObjectStorage.js';
import { SingleBucketStoreV3 } from './SingleBucketStoreV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class MongoCompactorV3 extends MongoCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV3;
  declare protected readonly storage: MongoSyncBucketStorageV3;

  public async *dirtyBucketBatches(options: {
    minBucketChanges: number;
    minChangeRatio: number;
  }): AsyncGenerator<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    const collection = this.db.bucketState(this.group_id) as unknown as mongo.Collection<BucketStateDocumentBase>;
    yield* this.dirtyBucketBatchesForCollection(
      collection,
      { d: new mongo.MinKey(), b: new mongo.MinKey() } as unknown as BucketStateDocumentV3['_id'],
      { d: new mongo.MaxKey(), b: new mongo.MaxKey() } as unknown as BucketStateDocumentV3['_id'],
      options,
      (bucketState) => (bucketState as BucketStateDocumentV3)._id.d
    );
  }

  public async dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    if (options.minBucketChanges <= 0) {
      throw new ReplicationAssertionError('minBucketChanges must be >= 1');
    }
    return this.dirtyBucketBatchForChecksumsForCollection(
      this.db.bucketState(this.group_id) as unknown as mongo.Collection<BucketStateDocumentBase>,
      {
        'estimate_since_compact.count': { $gte: options.minBucketChanges }
      } as unknown as mongo.Filter<BucketStateDocumentBase>,
      (bucketState) => (bucketState as BucketStateDocumentV3)._id.d
    );
  }

  protected async writeBucketStateUpdates(): Promise<void> {
    await this.db
      .bucketState(this.group_id)
      .bulkWrite(this.bucketStateUpdates as mongo.AnyBulkWriteOperation<BucketStateDocumentV3>[], {
        ordered: false
      });
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return (this.storage.checksums as MongoChecksumsV3).computePartialChecksumsDirectByDefinition(
      buckets.map(({ bucket, definitionId }) => {
        if (definitionId == null) {
          throw new ServiceAssertionError(`Missing definitionId for bucket checksum update on bucket ${bucket}`);
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
      throw new ServiceAssertionError(`Missing definitionId for V3 bucket state filter on bucket ${bucket}`);
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
    let resolvedDefinitionId = definitionId;

    if (resolvedDefinitionId == null) {
      const allDefinitionIds = this.storage.mapping.allBucketDefinitionIds();
      if (allDefinitionIds.length > 0) {
        const potentialIds = allDefinitionIds.map((id) => ({ d: id, b: bucket }));
        const bucketState = await this.db.bucketState(this.group_id).findOne({
          _id: { $in: potentialIds }
        });
        if (bucketState != null) {
          resolvedDefinitionId = bucketState._id.d;
        }
      }
    }

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

    // Retry any S3 deletes left from a previous crash before starting this compaction run
    await this.retryPendingS3Deletes();

    const lowerBound = bucketContext.minId;
    let upperBound = bucketContext.maxId;

    let totalChecksum = 0;
    let totalOpCount = 0;
    let totalOpBytes = 0;

    let lastNotPut: bigint | null = null;
    let opsSincePut = 0;

    const seen = new Map<string, bigint>();
    let trackingSize = 0;

    // --- Read batch from MongoDB ---
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
            storage_ref: 1,
            bsonSize: { $bsonSize: '$$ROOT' }
          }
        }
      ];

      const rawBatch = await collection
        .aggregate<BucketDataDocumentV3 & { bsonSize: number | bigint }>(pipeline, {
          batchSize: this.moveBatchQueryLimit + 1
        })
        .toArray();

      if (rawBatch.length == 0) {
        // No more documents in this bucket — compaction complete.
        break;
      }

      // --- Cut batch to byte limit ---
      let cumulativeBytes = 0;
      let batchCutIndex = rawBatch.length;

      for (let i = 0; i < rawBatch.length; i++) {
        const doc = rawBatch[i];
        let docSize = Number(doc.bsonSize);
        if (doc.storage_ref) {
          docSize += doc.size ?? 0;
        }
        cumulativeBytes += docSize;
        if (cumulativeBytes > this.moveBatchByteLimit && i > 0) {
          // Byte limit exceeded; cut batch at current index. Always include
          // at least one document (i > 0 guard) to guarantee forward progress.
          batchCutIndex = i;
          break;
        }
      }

      const batchDocs = rawBatch.slice(0, batchCutIndex);

      // Pre-fetch S3 objects for all S3-backed docs in this batch
      if (this.storage.objectStorage) {
        const store = new BucketDataObjectStorage(this.storage.objectStorage);
        const s3Docs = batchDocs.filter((d: BucketDataDocumentV3) => d.storage_ref);
        if (s3Docs.length > 0) {
          await Promise.all(
            s3Docs.map(async (doc: any) => {
              doc.ops = await store.retrieve(doc.storage_ref.path);
            })
          );
        }
      }

      // --- Decode documents into individual ops ---
      // Processable: document has at least one op <= maxOpId.
      // Only processable docs are deleted and recreated; the rest survive untouched.
      const batchOps: BucketDataDoc[] = [];
      const processableDocs: (BucketDataDocumentV3 & { bsonSize: number | bigint })[] = [];

      for (const doc of batchDocs) {
        let hasRelevantOp = false;
        const candidateOps: BucketDataDoc[] = [];
        for (const op of loadBucketDataDocument(context, doc as unknown as BucketDataDocumentV3)) {
          candidateOps.push(op);
          if (op.o <= this.maxOpId) {
            hasRelevantOp = true;
          }
        }
        if (hasRelevantOp) {
          processableDocs.push(doc);
          batchOps.push(...candidateOps);
        } // else: candidateOps discarded — document has no ops <= maxOpId
      }

      if (processableDocs.length == 0) {
        // No documents with relevant ops in this batch; paginate to next batch
        // without performing any writes. This handles batches where all documents
        // contain only ops above maxOpId.
        upperBound = batchDocs[batchDocs.length - 1]._id as typeof upperBound;
        if (batchCutIndex >= rawBatch.length && rawBatch.length < this.moveBatchQueryLimit) {
          // Entire remaining bucket is non-processable — compaction complete.
          break;
        }
        // Skip dedup, rechunking, and transaction for this batch.
        continue;
      }

      // Scoped replace in a bounded transaction.
      // Delete by individual _id values instead of a continuous range.
      // A continuous range could catch non-processable documents (all ops > maxOpId)
      // that happen to fall between processable documents in _id.o sort order.
      const idsToDelete = processableDocs.map((d) => d._id);
      const expectedDocCount = processableDocs.length;
      const expectedChecksum = processableDocs.reduce((sum, doc) => sum + doc.checksum, 0n);
      const expectedOpCount = processableDocs.reduce((sum, doc) => sum + doc.count, 0);

      // Sort ops by o descending for newest-first dedup
      batchOps.sort((a, b) => (b.o > a.o ? 1 : b.o < a.o ? -1 : 0));

      // --- Dedup: newest-first, superseded → MOVE ---
      const surviving: BucketDataDoc[] = [];

      for (const op of batchOps) {
        if (op.op == 'PUT' || op.op == 'REMOVE') {
          if (op.o > this.maxOpId) {
            surviving.push(op);
            continue; // Do not dedup ops above compaction horizon
          }
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
            if (lastNotPut == null) {
              lastNotPut = op.o;
            }
            opsSincePut += 1;
          } else {
            if (trackingSize < this.idLimitBytes) {
              seen.set(utils.flatstr(key), op.o);
              trackingSize += key.length + 140;
            }
            surviving.push(op);
            if (op.op == 'PUT') {
              lastNotPut = null;
              opsSincePut = 0;
            } else {
              if (lastNotPut == null) {
                lastNotPut = op.o;
              }
              opsSincePut += 1;
            }
          }
        } else {
          surviving.push(op);
          if (op.op != 'CLEAR') {
            if (lastNotPut == null) {
              lastNotPut = op.o;
            }
            opsSincePut += 1;
          }
        }
      }

      // Reverse back to ascending order for rechunking
      surviving.reverse();

      // --- Rechunk survivors into new V3 documents ---
      const chunks = chunkBucketData(surviving);

      // Track old S3 refs for cleanup
      const oldStorageRefs: string[] = processableDocs
        .filter((d: BucketDataDocumentV3) => d.storage_ref)
        .map((d: BucketDataDocumentV3) => d.storage_ref!.path);

      const newStoragePaths = new Set<string>();
      let newDocs: BucketDataDocumentV3[] = [];

      if (!this.storage.objectStorage) {
        newDocs = chunks.map((chunk) => serializeBucketData(bucket, chunk));
      } else {
        const store = new BucketDataObjectStorage(this.storage.objectStorage);
        for (const chunk of chunks) {
          const minOp = chunk[0].o;
          const maxOp = chunk[chunk.length - 1].o;

          let totalChecksum = 0n;
          let totalSize = 0;
          let maxTargetOp: bigint | null = null;
          let hasClearOp = false;
          const bucketOps = chunk.map((op) => {
            totalChecksum += op.checksum;
            totalSize += op.data?.length ?? 0;
            if (op.target_op != null && (maxTargetOp == null || op.target_op > maxTargetOp)) {
              maxTargetOp = op.target_op;
            }
            if (op.op === 'CLEAR') {
              hasClearOp = true;
            }
            return {
              o: op.o,
              op: op.op,
              source_table: op.source_table,
              source_key: op.source_key,
              table: op.table,
              row_id: op.row_id,
              checksum: op.checksum,
              data: op.data
            };
          });

          const path = `bucket-data/${this.group_id}/${resolvedDefinitionId}/${bucket}/${minOp}-${maxOp}`;
          const { compressedSize } = await store.store(path, bucketOps);
          newStoragePaths.add(path);

          newDocs.push({
            _id: { b: bucket, o: maxOp },
            min_op: minOp,
            checksum: totalChecksum,
            count: chunk.length,
            size: totalSize,
            target_op: maxTargetOp,
            has_clear_op: hasClearOp || undefined,
            storage_ref: {
              path,
              compressed_size: compressedSize
            }
          });
        }
      }
      // --- Commit: scoped delete + insert in transaction ---
      const session = this.db.client.startSession();
      try {
        await session.withTransaction(
          async () => {
            // Verify documents haven't been modified since we read them.
            // This aggregate anchors the transaction snapshot and catches
            // concurrent compaction jobs that modified the same documents.
            const verification = await bucketContext.collection
              .aggregate<{ docCount: number; checksumSum: bigint | null; opCountSum: number | null }>(
                [
                  { $match: { _id: { $in: idsToDelete } as any } },
                  {
                    $group: {
                      _id: null,
                      docCount: { $sum: 1 },
                      checksumSum: { $sum: '$checksum' },
                      opCountSum: { $sum: '$count' }
                    }
                  }
                ],
                { session }
              )
              .next();

            if (
              verification == null || // all docs deleted
              verification.docCount !== expectedDocCount || // some docs deleted
              verification.checksumSum !== expectedChecksum || // docs modified in-place
              verification.opCountSum !== expectedOpCount // ops added/removed within docs
            ) {
              throw new Error(
                `Concurrent modification detected in bucket ${bucket}. Aborting compaction for this batch.`
              );
            }

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

      // After commit: clean up old S3 objects via the pending delete queue.
      // Persist pending deletes inside the transaction (so a crash after commit
      // but before S3 delete cannot orphan objects). Then delete from S3 and
      // remove from the queue outside the transaction.
      if (this.storage.objectStorage && oldStorageRefs.length > 0) {
        const toDelete = oldStorageRefs.filter((p) => !newStoragePaths.has(p));
        if (toDelete.length > 0) {
          await this.deleteWithRetryQueue(bucketContext.collection, toDelete);
        }
      }

      // --- Accumulate bucket state ---
      for (const chunk of chunks) {
        for (const op of chunk) {
          if (op.o <= this.maxOpId) {
            totalChecksum = addChecksums(totalChecksum, Number(op.checksum));
            totalOpBytes += op.data?.length ?? 0;
          }
        }
      }
      totalOpCount += surviving.filter((op) => op.o <= this.maxOpId).length;

      // --- Advance to next batch ---
      upperBound = (newDocs.length > 0 ? newDocs[0]._id : rawBatch[batchCutIndex - 1]._id) as typeof upperBound;

      if (batchCutIndex < rawBatch.length) {
        // We cut the batch short due to byte limit — don't advance past cut point
        // The upperBound is already set to the last doc we processed
      } else {
        // Processed all docs in the raw batch. If we got fewer than the query
        // limit, there are no more documents in this bucket — compaction complete.
        if (rawBatch.length < this.moveBatchQueryLimit) {
          break;
        }
      }

      this.logger.info(`Compacted batch of ${batchDocs.length} documents for bucket ${bucket}`);
    }

    // --- Clear: collapse leading MOVE/REMOVE/CLEAR sequence ---
    if (lastNotPut != null && opsSincePut >= 2) {
      const cleared = await this.clearBucketLeading(lastNotPut, bucketContext, collection, context);
      if (cleared > 0) {
        totalOpCount = totalOpCount - cleared + 1; // cleared ops replaced by one CLEAR op
      }
    }

    // --- Finalize: update bucket checksums and state ---
    this.updateBucketChecksums({
      bucket,
      definitionId: resolvedDefinitionId,
      seen: new Map(),
      trackingSize: 0,
      lastNotPut: lastNotPut,
      opsSincePut: opsSincePut,
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

  /**
   * Collapse the leading sequence of MOVE/REMOVE/CLEAR ops at the start
   * of the bucket into a single CLEAR op. Reads forward (ascending) from
   * minId up to lastNotPut, then replaces all cleared documents in one
   * atomic transaction.
   *
   * Returns the number of ops collapsed (for bucket state adjustment).
   */
  private async clearBucketLeading(
    lastNotPut: bigint,
    bucketContext: SingleBucketStore,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<number> {
    const bucket = bucketContext.key.bucket;
    let highestSeenOp = 0n;

    const idsToDelete: BucketDataDocumentV3['_id'][] = [];
    let combinedChecksum = 0;
    let clearedOpCount = 0;
    let maxTargetOp: bigint | null = null;
    const boundarySurvivors: BucketDataDoc[] = [];

    let expectedDocCount = 0;
    let expectedChecksum = 0;
    let expectedOpCount = 0;

    // --- Read: paginate ascending, collect docs to delete ---
    const oldRefs: string[] = [];
    while (true) {
      this.signal?.throwIfAborted();

      const pipeline: mongo.Document[] = [
        {
          $match: {
            '_id.b': bucket,
            _id: {
              $gt: { b: bucket, o: highestSeenOp }
            }
          }
        },
        { $sort: { _id: 1 } },
        { $limit: this.clearBatchLimit },
        {
          $project: {
            _id: 1,
            min_op: 1,
            checksum: 1,
            count: 1,
            target_op: 1,
            ops: 1
          }
        }
      ];

      const rawBatch = await collection.aggregate(pipeline).toArray();

      if (rawBatch.length == 0) {
        break;
      }

      let boundaryFound = false;

      for (const doc of rawBatch) {
        if (doc.min_op > lastNotPut) {
          boundaryFound = true;
          break;
        }

        if (doc._id.o > lastNotPut) {
          // Boundary inside this document: split
          boundaryFound = true;
          idsToDelete.push(doc._id);
          if (doc.storage_ref?.path) {
            oldRefs.push(doc.storage_ref.path);
          }
          expectedDocCount++;
          expectedChecksum = addChecksums(expectedChecksum, Number(doc.checksum));
          expectedOpCount += doc.count;

          for (const op of loadBucketDataDocument(context, doc as unknown as BucketDataDocumentV3)) {
            if (op.o <= lastNotPut) {
              if (op.op == 'PUT') {
                throw new ReplicationAssertionError(
                  `Unexpected PUT at op ${op.o} in CLEAR region for bucket ${bucket}`
                );
              }
              combinedChecksum = addChecksums(combinedChecksum, Number(op.checksum));
              clearedOpCount++;
              if (op.target_op != null && (maxTargetOp == null || op.target_op > maxTargetOp)) {
                maxTargetOp = op.target_op;
              }
            } else {
              boundarySurvivors.push(op);
            }
          }
          break;
        } else {
          // Entire doc is in CLEAR region
          idsToDelete.push(doc._id);
          if (doc.storage_ref?.path) {
            oldRefs.push(doc.storage_ref.path);
          }
          expectedDocCount++;
          expectedChecksum = addChecksums(expectedChecksum, Number(doc.checksum));
          expectedOpCount += doc.count;

          for (const op of loadBucketDataDocument(context, doc as unknown as BucketDataDocumentV3)) {
            if (op.op == 'PUT') {
              throw new ReplicationAssertionError(`Unexpected PUT at op ${op.o} in CLEAR region for bucket ${bucket}`);
            }
            combinedChecksum = addChecksums(combinedChecksum, Number(op.checksum));
            clearedOpCount++;
            if (op.target_op != null && (maxTargetOp == null || op.target_op > maxTargetOp)) {
              maxTargetOp = op.target_op;
            }
          }
        }
      }

      highestSeenOp = (rawBatch[rawBatch.length - 1]._id as BucketDataDocumentV3['_id']).o;

      if (boundaryFound) {
        break;
      }

      if (rawBatch.length < this.clearBatchLimit) {
        break;
      }
    }

    if (idsToDelete.length == 0) {
      return 0;
    }

    this.logger.info(`Clearing ${clearedOpCount} ops (${idsToDelete.length} docs) at ${bucket} up to ${lastNotPut}`);

    // --- Build new documents (S3 upload or inline) ---
    const newStoragePaths = new Set<string>();
    let clearDoc: BucketDataDocumentV3;
    let boundaryDocShells: BucketDataDocumentV3[];

    if (!this.storage.objectStorage) {
      const clearOp = {
        bucketKey: { ...context, bucket },
        o: lastNotPut,
        op: 'CLEAR' as const,
        checksum: BigInt(combinedChecksum),
        data: null,
        target_op: maxTargetOp
      } satisfies BucketDataDoc;
      clearDoc = serializeBucketData(bucket, [clearOp]);

      boundaryDocShells =
        boundarySurvivors.length > 0
          ? chunkBucketData(boundarySurvivors).map((chunk) => serializeBucketData(bucket, chunk))
          : [];
    } else {
      const store = new BucketDataObjectStorage(this.storage.objectStorage!);

      // --- CLEAR doc: upload to S3, build metadata shell ---
      {
        const clearOpData = [
          {
            o: lastNotPut,
            op: 'CLEAR' as const,
            checksum: BigInt(combinedChecksum),
            data: null
          }
        ];

        const path = `bucket-data/${this.group_id}/${context.definitionId}/${bucket}/${lastNotPut}-${lastNotPut}`;
        const { compressedSize } = await store.store(path, clearOpData);
        newStoragePaths.add(path);

        clearDoc = {
          _id: { b: bucket, o: lastNotPut },
          min_op: lastNotPut,
          checksum: BigInt(combinedChecksum),
          count: 1,
          size: 0,
          target_op: maxTargetOp,
          has_clear_op: true,
          storage_ref: {
            path,
            compressed_size: compressedSize
          }
        };
      }

      // --- Boundary survivors: upload to S3, build metadata shells ---
      boundaryDocShells = [];
      if (boundarySurvivors.length > 0) {
        const chunks = chunkBucketData(boundarySurvivors);
        for (const chunk of chunks) {
          const minOp = chunk[0].o;
          const maxOp = chunk[chunk.length - 1].o;

          let totalChecksum = 0n;
          let totalSize = 0;
          let chunkMaxTargetOp: bigint | null = null;
          let hasClearOp = false;
          const bucketOps = chunk.map((op) => {
            totalChecksum += op.checksum;
            totalSize += op.data?.length ?? 0;
            if (op.target_op != null && (chunkMaxTargetOp == null || op.target_op > chunkMaxTargetOp)) {
              chunkMaxTargetOp = op.target_op;
            }
            if (op.op === 'CLEAR') {
              hasClearOp = true;
            }
            return {
              o: op.o,
              op: op.op,
              source_table: op.source_table,
              source_key: op.source_key,
              table: op.table,
              row_id: op.row_id,
              checksum: op.checksum,
              data: op.data
            };
          });

          const path = `bucket-data/${this.group_id}/${context.definitionId}/${bucket}/${minOp}-${maxOp}`;
          const { compressedSize } = await store.store(path, bucketOps);
          newStoragePaths.add(path);

          boundaryDocShells.push({
            _id: { b: bucket, o: maxOp },
            min_op: minOp,
            checksum: totalChecksum,
            count: chunk.length,
            size: totalSize,
            target_op: chunkMaxTargetOp,
            has_clear_op: hasClearOp || undefined,
            storage_ref: {
              path,
              compressed_size: compressedSize
            }
          });
        }
      }
    }

    // --- Write: single atomic transaction ---
    const session = this.db.client.startSession();
    try {
      await session.withTransaction(
        async () => {
          // Verify documents haven't been modified since we read them
          const verification = await collection
            .aggregate<{ docCount: number; checksumSum: bigint | null; opCountSum: number | null }>(
              [
                { $match: { _id: { $in: idsToDelete } as any } },
                {
                  $group: {
                    _id: null,
                    docCount: { $sum: 1 },
                    checksumSum: { $sum: '$checksum' },
                    opCountSum: { $sum: '$count' }
                  }
                }
              ],
              { session }
            )
            .next();

          if (
            verification == null || // all docs deleted
            verification.docCount !== expectedDocCount || // some docs deleted
            (Number((verification.checksumSum ?? 0n) & 0xffffffffn) | 0) !== expectedChecksum || // docs modified
            verification.opCountSum !== expectedOpCount // ops changed within docs
          ) {
            throw new Error(`Concurrent modification detected during CLEAR for bucket ${bucket}. Aborting.`);
          }

          await collection.deleteMany({ _id: { $in: idsToDelete } } as any, { session });

          await collection.insertOne(clearDoc as any, { session });

          if (boundaryDocShells.length > 0) {
            await collection.insertMany(boundaryDocShells as any, { session });
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

    // After commit: clean up old S3 objects via the pending delete queue
    if (this.storage.objectStorage && oldRefs.length > 0) {
      const toDelete = oldRefs.filter((p) => !newStoragePaths.has(p));
      if (toDelete.length > 0) {
        await this.deleteWithRetryQueue(collection, toDelete);
      }
    }

    return clearedOpCount;
  }

  /**
   * Safe S3 delete with a MongoDB-persisted retry queue. Writes pending
   * delete entries so a crash after commit but before S3 deletion cannot
   * orphan objects. The next compaction run retries any leftover pending deletes.
   */
  private async deleteWithRetryQueue(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    _collection: mongo.Collection<any>,
    paths: string[]
  ): Promise<void> {
    const pendingCollection = this.db.pendingS3Deletes(this.storage.replicationStreamId);
    const entries = paths.map((p) => ({ _id: p }));

    // Use ordered: false so a duplicate path (leftover from previous crash)
    // doesn't abort the batch.
    await pendingCollection.insertMany(entries, { ordered: false }).catch(() => {
      // Duplicate _id expected from retry runs; ignore silently
    });

    try {
      await this.storage.objectStorage!.delete(paths);
      await pendingCollection.deleteMany({ _id: { $in: paths } });
    } catch (err) {
      logger.warn(`Failed to delete S3 objects; will retry on next compaction: ${err}`);
    }
  }

  /**
   * Retry any S3 deletes left from a previous compaction run that crashed
   * between the transaction commit and the S3 delete.
   */
  private async retryPendingS3Deletes(): Promise<void> {
    if (!this.storage.objectStorage) {
      return;
    }
    const pendingCollection = this.db.pendingS3Deletes(this.storage.replicationStreamId);
    const pending = await pendingCollection.find({}).toArray();
    if (pending.length == 0) {
      return;
    }
    const paths = pending.map((p) => p._id);
    logger.info(`Retrying ${paths.length} pending S3 deletes from previous compaction run`);
    try {
      await this.storage.objectStorage.delete(paths);
      await pendingCollection.deleteMany({ _id: { $in: paths } });
    } catch (err) {
      logger.warn(`Failed to retry pending S3 deletes; will retry on next compaction: ${err}`);
    }
  }
}
