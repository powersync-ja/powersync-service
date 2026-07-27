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
import { chunkBucketData, DEFAULT_MAX_DOC_SIZE_BYTES } from './chunking.js';
import { BucketDataDocumentV3, BucketStateDocumentV3 } from './models.js';
import { DefinitionChecksumOperations, MongoChecksumsV3 } from './MongoChecksumsV3.js';
import type { MongoSyncBucketStorageV3 } from './MongoSyncBucketStorageV3.js';
import { BucketDataObjectStorage, hydrateBucketDataDocuments } from './object-storage/BucketDataObjectStorage.js';
import { ObjectStorageLifecycle, PreparedObjectStorageUpload } from './object-storage/ObjectStorageLifecycle.js';
import { SingleBucketStoreV3 } from './SingleBucketStoreV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

interface PendingCompactionGroup {
  /**
   * Input documents are ordered from oldest to newest, matching `ops`.
   * Keeping the inputs intact lets unchanged singletons retain their object.
   */
  inputs: BucketDataDocumentV3[];
  ops: BucketDataDoc[];
  changed: boolean;
}

/**
 * Read one bounded prefix from a descending compaction cursor.
 *
 * The document that would cross the byte limit is deliberately not returned:
 * pagination resumes below the last returned `_id`, so that document remains
 * eligible for the next query. The first document is always accepted to ensure
 * progress when a single document exceeds the configured byte limit.
 *
 * `hasMore` is conservative when the document limit is reached. An extra empty
 * query is preferable to exhausting the cursor just to determine whether the
 * limited MongoDB query contained another document.
 */
async function readCompactionBatch(
  cursor: mongo.AggregationCursor<BucketDataDocumentV3>,
  options: { byteLimit: number; documentLimit: number }
): Promise<{ documents: BucketDataDocumentV3[]; hasMore: boolean }> {
  const documents: BucketDataDocumentV3[] = [];
  let cumulativeBytes = 0;

  try {
    for await (const document of cursor) {
      if (documents.length > 0 && cumulativeBytes + document.size > options.byteLimit) {
        return { documents, hasMore: true };
      }

      documents.push(document);
      cumulativeBytes += document.size;

      if (documents.length >= options.documentLimit) {
        return { documents, hasMore: true };
      }
    }
    return { documents, hasMore: false };
  } finally {
    await cursor.close();
  }
}

export class MongoCompactorV3 extends MongoCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV3;
  declare protected readonly storage: MongoSyncBucketStorageV3;

  override async compact(): Promise<void> {
    await super.compact();
    if (this.storage.objectStorage) {
      await this.objectStorageLifecycle.cleanup(this.logger);
    }
  }

  private get objectStorageLifecycle(): ObjectStorageLifecycle {
    if (!this.storage.objectStorage) {
      throw new Error('Object storage is not configured');
    }
    return new ObjectStorageLifecycle(this.db, this.group_id, this.storage.objectStorage);
  }

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

    const lowerBound = bucketContext.minId;
    let upperBound = bucketContext.docId(this.maxOpId + 1n);

    let totalChecksum = 0;
    let totalOpCount = 0;
    let totalOpBytes = 0;

    let lastNotPut: bigint | null = null;
    let opsSincePut = 0;
    let compactedOpId: bigint | null = null;
    let clearBoundary: { opId: bigint; documentId: BucketDataKey } | null = null;
    const seen = new Map<string, bigint>();
    let trackingSize = 0;
    let pendingGroup: PendingCompactionGroup | null = null;

    // --- Read batch from MongoDB ---
    while (true) {
      this.signal?.throwIfAborted();

      const pipeline: mongo.Document[] = [
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
            min_op: 1,
            checksum: 1,
            count: 1,
            size: 1,
            target_op: 1,
            ops: 1,
            storage_ref: 1
          }
        }
      ];

      const batch = await readCompactionBatch(
        collection.aggregate<BucketDataDocumentV3>(pipeline, {
          batchSize: this.moveBatchQueryLimit + 1
        }),
        {
          byteLimit: this.moveBatchByteLimit,
          documentLimit: this.moveBatchQueryLimit
        }
      );
      const batchDocs = batch.documents;

      if (batchDocs.length == 0) {
        // No more documents in this bucket — compaction complete.
        break;
      }

      await hydrateBucketDataDocuments(batchDocs, this.storage.objectStorage, this.signal);

      // Compact each document independently, then greedily merge adjacent
      // post-compaction results. This preserves existing boundaries unless
      // merging is useful, and writes each final object at most once.
      for (const doc of batchDocs) {
        compactedOpId ??= doc._id.o;
        const originalOps = Array.from(loadBucketDataDocument(context, doc));

        let changed = false;
        const compactedOps: BucketDataDoc[] = [];
        for (let index = originalOps.length - 1; index >= 0; index--) {
          const op = originalOps[index];
          if (op.op == 'PUT' || op.op == 'REMOVE') {
            const key = `${op.table}/${op.row_id}/${cacheKey(op.source_table!, op.source_key!)}`;
            const targetOp = seen.get(key);
            if (targetOp != null) {
              compactedOps.push({
                ...op,
                op: 'MOVE',
                target_op: targetOp,
                table: undefined,
                row_id: undefined,
                source_table: undefined,
                source_key: undefined,
                data: null
              });
              changed = true;
              if (lastNotPut == null) {
                lastNotPut = op.o;
              }
              opsSincePut += 1;
            } else {
              if (trackingSize < this.idLimitBytes) {
                seen.set(utils.flatstr(key), op.o);
                trackingSize += key.length + 140;
              }
              compactedOps.push(op);
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
            compactedOps.push(op);
            if (op.op != 'CLEAR') {
              if (lastNotPut == null) {
                lastNotPut = op.o;
              }
              opsSincePut += 1;
            }
          }
        }
        compactedOps.reverse();

        for (const op of compactedOps) {
          totalChecksum = addChecksums(totalChecksum, Number(op.checksum));
          totalOpBytes += op.data?.length ?? 0;
        }
        totalOpCount += compactedOps.length;

        const candidate: PendingCompactionGroup = {
          inputs: [doc],
          ops: compactedOps,
          changed
        };

        if (pendingGroup == null) {
          pendingGroup = candidate;
        } else {
          const mergedOps: BucketDataDoc[] = [...candidate.ops, ...pendingGroup.ops];
          const mergedSize = serializeBucketData(bucket, mergedOps).size;
          if (mergedSize <= DEFAULT_MAX_DOC_SIZE_BYTES) {
            pendingGroup = {
              inputs: [...candidate.inputs, ...pendingGroup.inputs],
              ops: mergedOps,
              changed: candidate.changed || pendingGroup.changed
            };
          } else {
            const flushedGroup = pendingGroup;
            const documentId = await this.flushCompactionGroup(bucket, flushedGroup, bucketContext, context);
            if (
              lastNotPut != null &&
              flushedGroup.ops[0].o <= lastNotPut &&
              flushedGroup.ops[flushedGroup.ops.length - 1].o >= lastNotPut
            ) {
              clearBoundary = { opId: lastNotPut, documentId };
            }
            pendingGroup = candidate;
          }
        }
      }

      // --- Advance to next batch ---
      upperBound = batchDocs[batchDocs.length - 1]._id as typeof upperBound;

      if (!batch.hasMore) {
        break;
      }

      this.logger.info(`Compacted batch of ${batchDocs.length} documents for bucket ${bucket}`);
    }

    if (pendingGroup != null) {
      const documentId = await this.flushCompactionGroup(bucket, pendingGroup, bucketContext, context);
      if (
        lastNotPut != null &&
        pendingGroup.ops[0].o <= lastNotPut &&
        pendingGroup.ops[pendingGroup.ops.length - 1].o >= lastNotPut
      ) {
        clearBoundary = { opId: lastNotPut, documentId };
      }
    }
    if (compactedOpId == null) {
      return;
    }

    // --- Clear: collapse leading MOVE/REMOVE/CLEAR sequence ---
    if (lastNotPut != null && opsSincePut >= 2) {
      if (clearBoundary == null || clearBoundary.opId != lastNotPut) {
        throw new ReplicationAssertionError(`Missing CLEAR boundary document for bucket ${bucket}`);
      }

      totalOpCount += await this.clearBucketLeading(
        lastNotPut,
        clearBoundary.documentId,
        bucketContext,
        collection,
        context
      );
    }

    // --- Finalize: update bucket checksums and state ---
    this.updateBucketChecksums(
      {
        bucket,
        definitionId: resolvedDefinitionId,
        seen: new Map(),
        trackingSize: 0,
        lastNotPut: lastNotPut,
        opsSincePut: opsSincePut,
        checksum: totalChecksum,
        opCount: totalOpCount,
        opBytes: totalOpBytes
      },
      compactedOpId
    );
    if (this.bucketStateUpdates.length > 0) {
      await this.writeBucketStateUpdates();
      this.bucketStateUpdates = [];
    }

    logger.info(`Compacted bucket ${bucket}: ${totalOpCount} surviving ops`);
  }

  private async flushCompactionGroup(
    bucket: string,
    group: PendingCompactionGroup,
    bucketContext: SingleBucketStoreV3,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<BucketDataKey> {
    if (group.inputs.length == 1 && !group.changed) {
      return group.inputs[0]._id;
    }

    const [newDoc] = await this.replaceCompactionDocuments(bucket, group.inputs, [group.ops], bucketContext, context);
    return newDoc._id;
  }

  /**
   * Persist replacement objects before starting the transaction, then atomically
   * publish their lifecycle markers alongside the MongoDB document replacement.
   * If verification or the transaction fails, the prepared markers retain enough
   * information for the uploaded objects to be cleaned up later.
   */
  private async replaceCompactionDocuments(
    bucket: string,
    inputs: BucketDataDocumentV3[],
    chunks: BucketDataDoc[][],
    bucketContext: SingleBucketStoreV3,
    context: { replicationStreamId: number; definitionId: string }
  ): Promise<BucketDataDocumentV3[]> {
    const idsToDelete = inputs.map((doc) => doc._id);
    const expectedDocCount = inputs.length;
    const expectedChecksum = inputs.reduce((sum, doc) => sum + doc.checksum, 0n);
    const expectedOpCount = inputs.reduce((sum, doc) => sum + doc.count, 0);
    const oldStoragePaths = inputs.flatMap((doc) => (doc.storage_ref ? [doc.storage_ref.path] : []));
    const { documents, storagePaths: newStoragePaths, uploads } = await this.persistBucketData(bucket, chunks, context);
    const session = this.db.client.startSession();
    try {
      await session.withTransaction(
        async () => {
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
            verification == null ||
            verification.docCount !== expectedDocCount ||
            verification.checksumSum !== expectedChecksum ||
            verification.opCountSum !== expectedOpCount
          ) {
            throw new Error(
              `Concurrent modification detected in bucket ${bucket}. Aborting compaction for this group.`
            );
          }

          await bucketContext.collection.deleteMany({ _id: { $in: idsToDelete } } as any, { session });
          await bucketContext.collection.insertMany(documents as unknown as BucketDataDocumentGeneric[], { session });
          await this.finishObjectStorageReplacement(oldStoragePaths, newStoragePaths, uploads, session);
        },
        {
          writeConcern: { w: 'majority' },
          readConcern: { level: 'snapshot' }
        }
      );
    } finally {
      await session.endSession();
    }
    return documents;
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
    this.signal?.throwIfAborted();
    const prepared = await this.prepareCompactionUploads(bucket, context, 1, lastNotPut);
    let done = false;
    let opCountDiff = 0;

    await session.withTransaction(
      async () => {
        done = false;
        opCountDiff = 0;
        const oldStoragePaths: string[] = [];
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
              has_clear_op: 1,
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

          // The compaction scan established that every operation before the
          // boundary is MOVE/REMOVE/CLEAR. Root metadata is sufficient to fold
          // whole documents into one CLEAR, so avoid downloading their payloads.
          if (doc.has_clear_op) {
            clearOpCount++;
            if (clearOpCount > 1) {
              throw new ReplicationAssertionError(`Unexpected multiple CLEAR operations in bucket ${bucket}`);
            }
          }
          if (!doc.has_clear_op || doc.count > 1) {
            gotNonClearOp = true;
          }
          combinedChecksum = addChecksums(combinedChecksum, Number(doc.checksum));
          clearedOpCount += doc.count;
          if (doc.target_op != null && (maxTargetOp == null || doc.target_op > maxTargetOp)) {
            maxTargetOp = doc.target_op;
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
        const persisted = await this.persistBucketData(bucket, [[clearOp]], context, prepared);
        await collection.insertOne(persisted.documents[0], { session });
        await this.finishObjectStorageReplacement(oldStoragePaths, persisted.storagePaths, persisted.uploads, session);

        opCountDiff = -clearedOpCount + 1;
      },
      {
        writeConcern: { w: 'majority' },
        readConcern: { level: 'snapshot' }
      }
    );

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
    this.signal?.throwIfAborted();
    const prepared = await this.prepareCompactionUploads(bucket, context, 2, lastNotPut);
    let opCountDiff = 0;

    await session.withTransaction(
      async () => {
        opCountDiff = 0;
        const oldStoragePaths: string[] = [];
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
          await hydrateBucketDataDocuments([doc], this.storage.objectStorage, this.signal);
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
        const chunks = [[clearOp], ...chunkBucketData(boundarySurvivors)];
        const persisted = await this.persistBucketData(bucket, chunks, context, prepared);
        await collection.insertMany(persisted.documents, { session });
        await this.finishObjectStorageReplacement(oldStoragePaths, persisted.storagePaths, persisted.uploads, session);

        opCountDiff = -clearedOpCount + 1;
      },
      {
        writeConcern: { w: 'majority' },
        readConcern: { level: 'snapshot' }
      }
    );

    return opCountDiff;
  }

  /**
   * Reserve stable object paths before starting a retryable MongoDB transaction.
   * Each retry can safely overwrite the same paths, while the pre-existing
   * deletion markers remain visible to the transaction that publishes them.
   *
   * CLEAR compaction reserves its maximum output count. Unused markers remain
   * pending so they can clean up a path that an earlier transaction attempt may
   * have uploaded before retrying with fewer output documents.
   */
  private async prepareCompactionUploads(
    bucket: string,
    context: { replicationStreamId: number; definitionId: string },
    count: number,
    opIdHint: bigint
  ): Promise<PreparedObjectStorageUpload[]> {
    if (!this.storage.objectStorage) {
      return [];
    }

    const lifecycle = this.objectStorageLifecycle;
    const paths = Array.from({ length: count }, () =>
      lifecycle.allocatePath(context.definitionId, bucket, opIdHint, opIdHint)
    );
    return lifecycle.prepareUploads(paths);
  }

  /** Publish replacement uploads and retire superseded objects in the same transaction. */
  private async finishObjectStorageReplacement(
    oldStoragePaths: Iterable<string>,
    newStoragePaths: Set<string>,
    uploads: PreparedObjectStorageUpload[],
    session: mongo.ClientSession
  ): Promise<void> {
    if (!this.storage.objectStorage) {
      return;
    }
    await this.objectStorageLifecycle.publishUploads(uploads, session);
    await this.objectStorageLifecycle.retire(
      Array.from(oldStoragePaths).filter((path) => !newStoragePaths.has(path)),
      session
    );
  }

  private async persistBucketData(
    bucket: string,
    chunks: BucketDataDoc[][],
    context: { replicationStreamId: number; definitionId: string },
    preparedUploads?: PreparedObjectStorageUpload[]
  ): Promise<{ documents: BucketDataDocumentV3[]; storagePaths: Set<string>; uploads: PreparedObjectStorageUpload[] }> {
    const serializedChunks = chunks.map((chunk) => serializeBucketData(bucket, chunk));
    if (!this.storage.objectStorage) {
      return {
        documents: serializedChunks,
        storagePaths: new Set(),
        uploads: []
      };
    }

    const store = new BucketDataObjectStorage(this.storage.objectStorage);
    const storagePaths = new Set<string>();
    const documents: BucketDataDocumentV3[] = [];
    const lifecycle = this.objectStorageLifecycle;
    // Base placement on the final compacted size. Unchanged documents are not
    // rewritten, while small MOVE/merge results and CLEAR ops stay inline.
    const storedIndexes = serializedChunks.flatMap((document, index) =>
      document.size > this.storage.inlineThresholdBytes ? [index] : []
    );
    const uploadsByIndex = new Map<number, PreparedObjectStorageUpload>();

    if (preparedUploads) {
      for (const index of storedIndexes) {
        const upload = preparedUploads[index];
        if (!upload) {
          throw new ServiceAssertionError(
            `Missing prepared object storage path for compacted document at index ${index}`
          );
        }
        uploadsByIndex.set(index, upload);
      }
    } else {
      const paths = storedIndexes.map((index) => {
        const chunk = chunks[index];
        return lifecycle.allocatePath(context.definitionId, bucket, chunk[0].o, chunk[chunk.length - 1].o);
      });
      const prepared = await lifecycle.prepareUploads(paths);
      storedIndexes.forEach((index, preparedIndex) => uploadsByIndex.set(index, prepared[preparedIndex]));
    }

    // Keep object uploads bounded. Compaction may produce many chunks, and a
    // sequential upload stream avoids unbounded memory and S3 request pressure.
    for (const [index, serialized] of serializedChunks.entries()) {
      const upload = uploadsByIndex.get(index);
      if (!upload) {
        documents.push(serialized);
        continue;
      }

      const { ops, ...metadata } = serialized;
      const { fileSize } = await store.store(upload.path, ops!);
      storagePaths.add(upload.path);
      documents.push({
        ...metadata,
        storage_ref: { path: upload.path, file_size: fileSize }
      });
    }

    return { documents, storagePaths, uploads: Array.from(uploadsByIndex.values()) };
  }
}
