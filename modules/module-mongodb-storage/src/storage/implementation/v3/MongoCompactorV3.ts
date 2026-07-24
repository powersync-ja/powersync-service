import { mongo } from '@powersync/lib-service-mongodb';
import { logger, ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { addChecksums, storage, utils } from '@powersync/service-core';
import { BucketDefinitionId } from '@powersync/service-sync-rules';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { BucketDataDocumentGeneric } from '../common/SingleBucketStore.js';
import { BucketDataKey, BucketStateDocumentBase } from '../models.js';
import { DirtyBucket, MongoCompactor } from '../MongoCompactor.js';
import { cacheKey } from '../OperationBatch.js';
import { loadBucketDataDocument, serializeBucketData } from './bucket-format.js';
import { chunkBucketData } from './chunking.js';
import { BucketDataDocumentV3, BucketStateDocumentV3 } from './models.js';
import { DefinitionChecksumOperations, MongoChecksumsV3 } from './MongoChecksumsV3.js';
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

  /**
   * The compactor operates on persisted definition ids only - never on parsed sources.
   * This narrowed view makes the source-resolving checksum methods unreachable here.
   */
  private get definitionChecksums(): DefinitionChecksumOperations {
    return this.storage.checksums as MongoChecksumsV3;
  }

  protected async computeChecksumsForBuckets(
    buckets: Pick<DirtyBucket, 'bucket' | 'definitionId'>[]
  ): Promise<storage.PartialChecksumMap> {
    return this.definitionChecksums.computePartialChecksumsDirectByDefinition(
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
  ): Promise<SingleBucketStoreV3 | null> {
    let resolvedDefinitionId = definitionId;

    if (resolvedDefinitionId == null) {
      const allDefinitionIds = this.storage.storageIds.bucketDefinitionIds;
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

    await this.retryPendingS3Deletes();

    const lowerBound = bucketContext.minId;
    let upperBound = bucketContext.maxId;

    let totalChecksum = 0;
    let totalOpCount = 0;
    let totalOpBytes = 0;

    let lastNotPut: bigint | null = null;
    let opsSincePut = 0;
    let clearBoundaryDocId: BucketDataKey | null = null;

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
            }
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
        cumulativeBytes += Number(rawBatch[i].bsonSize) + (rawBatch[i].storage_ref ? rawBatch[i].size : 0);
        if (cumulativeBytes > this.moveBatchByteLimit && i > 0) {
          // Byte limit exceeded; cut batch at current index. Always include
          // at least one document (i > 0 guard) to guarantee forward progress.
          batchCutIndex = i;
          break;
        }
      }

      const batchDocs = rawBatch.slice(0, batchCutIndex);

      await this.hydrateS3Documents(batchDocs);

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
      const oldStoragePaths = processableDocs.flatMap((doc) => (doc.storage_ref ? [doc.storage_ref.path] : []));
      const { documents: newDocs, storagePaths: newStoragePaths } = await this.persistBucketData(
        bucket,
        chunks,
        context
      );

      if (lastNotPut == null) {
        clearBoundaryDocId = null;
      } else {
        const boundaryOp = lastNotPut;
        const boundaryDoc = newDocs.find((doc) => doc.min_op <= boundaryOp && doc._id.o >= boundaryOp);
        if (boundaryDoc != null) {
          clearBoundaryDocId = boundaryDoc._id;
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
                  { $match: { _id: { $in: idsToDelete } } },
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

      await this.deleteS3Objects(oldStoragePaths.filter((path) => !newStoragePaths.has(path)));

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
      if (clearBoundaryDocId == null) {
        throw new ReplicationAssertionError(`Missing CLEAR boundary document for bucket ${bucket}`);
      }

      totalOpCount += await this.clearBucketLeading(lastNotPut, clearBoundaryDocId, bucketContext, collection, context);
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
   * of the bucket into a single CLEAR op. Reads whole clearable documents
   * before the known boundary document, then splits that boundary document
   * if it contains ops on both sides of lastNotPut.
   *
   * Returns the op count diff after replacing cleared ops with CLEAR ops.
   */
  private async clearBucketLeading(
    lastNotPut: bigint,
    boundaryDocId: BucketDataKey,
    bucketContext: SingleBucketStoreV3,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<number> {
    let opCountDiff = 0;
    const session = this.db.client.startSession();
    try {
      let done = false;
      // First step is to clear full chunks that contain only CLEAR/MOVE/REMOVE operations.
      // There can be many of them, so we do one batch at a time.
      while (!done) {
        const batch = await this.clearLeadingFullDocuments(
          session,
          lastNotPut,
          boundaryDocId,
          bucketContext,
          collection,
          context
        );
        done = batch.done;
        opCountDiff += batch.opCountDiff;
      }

      // The final step is to process the "boundary" document: It may contain some CLEAR/MOVE/REMOVE operations,
      // potentially followed by PUT operations. This is only a single document, so no need for batching.
      opCountDiff += await this.clearBoundaryDocument(
        session,
        lastNotPut,
        boundaryDocId,
        bucketContext,
        collection,
        context
      );
    } finally {
      await session.endSession();
    }

    return opCountDiff;
  }

  private async clearLeadingFullDocuments(
    session: mongo.ClientSession,
    lastNotPut: bigint,
    boundaryDocId: BucketDataKey,
    bucketContext: SingleBucketStoreV3,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<{ done: boolean; opCountDiff: number }> {
    const bucket = bucketContext.key.bucket;
    let done = false;
    let opCountDiff = 0;
    const oldStoragePaths: string[] = [];
    let newStoragePaths = new Set<string>();

    this.signal?.throwIfAborted();
    await session.withTransaction(
      async () => {
        const query = collection.find(
          {
            _id: {
              $gte: bucketContext.minId,
              $lt: boundaryDocId
            }
          },
          {
            session,
            sort: { _id: 1 },
            projection: {
              _id: 1,
              min_op: 1,
              checksum: 1,
              count: 1,
              target_op: 1,
              ops: 1,
              storage_ref: 1
            },
            limit: this.clearBatchLimit
          }
        );

        let combinedChecksum = 0;
        let clearedOpCount = 0;
        let maxTargetOp: bigint | null = null;
        let lastDocId: BucketDataKey | null = null;
        let clearOpCount = 0;
        let gotNonClearOp = false;

        for await (const doc of query.stream()) {
          if (doc.min_op > lastNotPut) {
            throw new ReplicationAssertionError(
              `Unexpected document before CLEAR boundary with min_op ${doc.min_op} > ${lastNotPut} in bucket ${bucket}`
            );
          }

          lastDocId = doc._id;
          if (doc.storage_ref) {
            oldStoragePaths.push(doc.storage_ref.path);
          }
          await this.hydrateS3Documents([doc]);
          for (const op of loadBucketDataDocument(context, doc)) {
            if (op.o > lastNotPut) {
              throw new ReplicationAssertionError(
                `Unexpected op ${op.o} after CLEAR boundary ${lastNotPut} in bucket ${bucket}`
              );
            }
            if (op.op == 'PUT') {
              throw new ReplicationAssertionError(`Unexpected PUT at op ${op.o} in CLEAR region for bucket ${bucket}`);
            }

            if (op.op == 'CLEAR') {
              clearOpCount++;
              if (clearOpCount > 1) {
                throw new ReplicationAssertionError(`Unexpected multiple CLEAR operations in bucket ${bucket}`);
              }
            } else {
              gotNonClearOp = true;
            }
            combinedChecksum = addChecksums(combinedChecksum, Number(op.checksum));
            clearedOpCount++;
            if (op.target_op != null && (maxTargetOp == null || op.target_op > maxTargetOp)) {
              maxTargetOp = op.target_op;
            }
          }
        }

        if (!gotNonClearOp) {
          done = true;
          return;
        }

        this.logger.info(`Flushing CLEAR for ${clearedOpCount} ops at ${lastDocId?.o}`);
        await collection.deleteMany(
          {
            _id: {
              $gte: bucketContext.minId,
              $lte: lastDocId!
            }
          },
          { session }
        );

        const clearOp = {
          bucketKey: { ...context, bucket },
          o: lastDocId!.o,
          op: 'CLEAR' as const,
          checksum: BigInt(combinedChecksum),
          data: null,
          target_op: maxTargetOp
        } satisfies BucketDataDoc;
        const persisted = await this.persistBucketData(bucket, [[clearOp]], context);
        newStoragePaths = persisted.storagePaths;
        await collection.insertOne(persisted.documents[0], { session });

        opCountDiff = -clearedOpCount + 1;
      },
      {
        writeConcern: { w: 'majority' },
        readConcern: { level: 'snapshot' }
      }
    );

    await this.deleteS3Objects(oldStoragePaths.filter((path) => !newStoragePaths.has(path)));

    return { done, opCountDiff };
  }

  private async clearBoundaryDocument(
    session: mongo.ClientSession,
    lastNotPut: bigint,
    boundaryDocId: BucketDataKey,
    bucketContext: SingleBucketStoreV3,
    collection: mongo.Collection<BucketDataDocumentV3 & { bsonSize?: number | bigint }>,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<number> {
    const bucket = bucketContext.key.bucket;
    let opCountDiff = 0;
    const oldStoragePaths: string[] = [];
    let newStoragePaths = new Set<string>();

    await session.withTransaction(
      async () => {
        const query = collection.find(
          {
            // This is a range query, but should only ever return two documents:
            // 1. The CLEAR op from the previous clearLeadingFullDocuments.
            // 2. The boundary document.
            _id: {
              $gte: bucketContext.minId,
              $lte: boundaryDocId
            }
          },
          {
            session,
            sort: { _id: 1 },
            projection: {
              _id: 1,
              min_op: 1,
              checksum: 1,
              count: 1,
              target_op: 1,
              ops: 1,
              storage_ref: 1
            },
            limit: 3
          }
        );

        let docsRead = 0;
        let combinedChecksum = 0;
        let clearedOpCount = 0;
        let maxTargetOp: bigint | null = null;
        const boundarySurvivors: BucketDataDoc[] = [];

        for await (const doc of query.stream()) {
          docsRead++;
          if (docsRead > 2) {
            throw new ReplicationAssertionError(`Unexpected extra document before CLEAR boundary in bucket ${bucket}`);
          }

          const isBoundaryDoc = doc._id.o == boundaryDocId.o;
          if (doc.storage_ref) {
            oldStoragePaths.push(doc.storage_ref.path);
          }
          await this.hydrateS3Documents([doc]);
          for (const op of loadBucketDataDocument(context, doc)) {
            if (!isBoundaryDoc && op.op != 'CLEAR') {
              throw new ReplicationAssertionError(
                `Unexpected ${op.op} operation before CLEAR boundary in bucket ${bucket}`
              );
            }

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
            } else if (isBoundaryDoc) {
              boundarySurvivors.push(op);
            } else {
              throw new ReplicationAssertionError(
                `Unexpected op ${op.o} after CLEAR boundary ${lastNotPut} in bucket ${bucket}`
              );
            }
          }
        }

        if (clearedOpCount == 0) {
          throw new Error(`CLEAR boundary document not found for bucket ${bucket}`);
        }

        this.logger.info(`Flushing CLEAR for ${clearedOpCount} ops at ${lastNotPut}`);
        await collection.deleteMany(
          {
            _id: {
              $gte: bucketContext.minId,
              $lte: boundaryDocId
            }
          },
          { session }
        );

        const clearOp = {
          bucketKey: { ...context, bucket },
          o: lastNotPut,
          op: 'CLEAR' as const,
          checksum: BigInt(combinedChecksum),
          data: null,
          target_op: maxTargetOp
        } satisfies BucketDataDoc;
        const persisted = await this.persistBucketData(
          bucket,
          [[clearOp], ...chunkBucketData(boundarySurvivors)],
          context
        );
        newStoragePaths = persisted.storagePaths;
        await collection.insertMany(persisted.documents, { session });

        opCountDiff = -clearedOpCount + 1;
      },
      {
        writeConcern: { w: 'majority' },
        readConcern: { level: 'snapshot' }
      }
    );

    await this.deleteS3Objects(oldStoragePaths.filter((path) => !newStoragePaths.has(path)));

    return opCountDiff;
  }

  private async hydrateS3Documents(documents: BucketDataDocumentV3[]): Promise<void> {
    if (!this.storage.objectStorage) {
      return;
    }

    const store = new BucketDataObjectStorage(this.storage.objectStorage);
    await Promise.all(
      documents
        .filter((document) => document.storage_ref)
        .map(async (document) => {
          document.ops = await store.retrieve(document.storage_ref!.path);
        })
    );
  }

  private async persistBucketData(
    bucket: string,
    chunks: BucketDataDoc[][],
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<{ documents: BucketDataDocumentV3[]; storagePaths: Set<string> }> {
    if (!this.storage.objectStorage) {
      return {
        documents: chunks.map((chunk) => serializeBucketData(bucket, chunk)),
        storagePaths: new Set()
      };
    }

    const store = new BucketDataObjectStorage(this.storage.objectStorage);
    const storagePaths = new Set<string>();
    const documents: BucketDataDocumentV3[] = [];

    for (const chunk of chunks) {
      const minOp = chunk[0].o;
      const maxOp = chunk[chunk.length - 1].o;
      const path = `bucket-data/${context.replicationStreamId}/${context.definitionId}/${bucket}/${minOp}-${maxOp}.bson.zstd`;
      const { compressedSize } = await store.store(path, chunk);
      storagePaths.add(path);
      documents.push({
        _id: { b: bucket, o: maxOp },
        min_op: minOp,
        checksum: chunk.reduce((checksum, op) => checksum + op.checksum, 0n),
        count: chunk.length,
        size: chunk.reduce((size, op) => size + (op.data?.length ?? 0), 0),
        target_op: chunk.reduce<bigint | null>(
          (targetOp, op) =>
            op.target_op != null && (targetOp == null || op.target_op > targetOp) ? op.target_op : targetOp,
          null
        ),
        has_clear_op: chunk.some((op) => op.op == 'CLEAR') || undefined,
        storage_ref: { path, compressed_size: compressedSize }
      });
    }

    return { documents, storagePaths };
  }

  private async deleteS3Objects(paths: string[]): Promise<void> {
    if (!this.storage.objectStorage || paths.length == 0) {
      return;
    }

    const pendingDeletes = this.db.pendingS3Deletes(this.storage.replicationStreamId);
    await pendingDeletes
      .insertMany(
        paths.map((_id) => ({ _id })),
        { ordered: false }
      )
      .catch(() => undefined);

    try {
      await this.storage.objectStorage.delete(paths);
      await pendingDeletes.deleteMany({ _id: { $in: paths } });
    } catch (error) {
      this.logger.warn(`Failed to delete S3 objects; will retry during the next compaction`, error);
    }
  }

  private async retryPendingS3Deletes(): Promise<void> {
    if (!this.storage.objectStorage) {
      return;
    }

    const pendingDeletes = this.db.pendingS3Deletes(this.storage.replicationStreamId);
    const paths = (await pendingDeletes.find({}).toArray()).map((entry) => entry._id);
    if (paths.length == 0) {
      return;
    }

    try {
      await this.storage.objectStorage.delete(paths);
      await pendingDeletes.deleteMany({ _id: { $in: paths } });
    } catch (error) {
      this.logger.warn(`Failed to retry pending S3 object deletes`, error);
    }
  }
}
