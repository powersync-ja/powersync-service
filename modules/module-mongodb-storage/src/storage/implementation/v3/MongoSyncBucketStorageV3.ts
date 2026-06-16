import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  InternalOpId,
  internalToExternalOpId,
  maxLsn,
  ParameterSetLimitExceededError,
  ProtocolOpId,
  storage,
  utils
} from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { ParameterLookupRows, ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mapOpEntry, readSingleBatch, setSessionSnapshotTime } from '../../../utils/util.js';
import { MongoBucketStorage } from '../../MongoBucketStorage.js';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import {
  MongoSyncBucketStorageCheckpoint,
  MongoSyncBucketStorageContext
} from '../common/MongoSyncBucketStorageContext.js';
import { MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { MongoChecksums } from '../MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from '../MongoCompactor.js';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { MongoPersistedReplicationStream } from '../MongoPersistedReplicationStream.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageOptions, WriterSyncState } from '../MongoSyncBucketStorage.js';
import { loadBucketDataDocument } from './bucket-format.js';
import {
  BucketDataDocumentV3,
  BucketParameterDocumentV3,
  deserializeParameterLookup,
  ReplicationStreamDocumentV3,
  serializeParameterLookup,
  SyncRuleConfigStateV3
} from './models.js';
import { MongoBucketBatchV3 } from './MongoBucketBatchV3.js';
import { MongoChecksumsV3 } from './MongoChecksumsV3.js';
import { MongoCompactorV3 } from './MongoCompactorV3.js';
import { BucketDataObjectStorage } from './object-storage/BucketDataObjectStorage.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

function* walkDocumentOps(
  data: BucketDataDoc[],
  documentOpCounts: number[],
  documentSizes: number[]
): Generator<{ row: BucketDataDoc; docIndex: number; isLastOpInDocument: boolean }> {
  let opIndex = 0;
  for (const [docIndex, opCount] of documentOpCounts.entries()) {
    for (let i = 0; i < opCount; i++) {
      yield { row: data[opIndex++], docIndex, isLastOpInDocument: i === opCount - 1 };
    }
  }
}

function extractRowsFromDocument(
  doc: BucketDataDocumentV3,
  context: { replicationStreamId: number; definitionId: string },
  bucketMap: Map<string, InternalOpId>,
  endOpId: InternalOpId,
  remainingLimit: number
): { rows: BucketDataDoc[]; remainingLimit: number; limitReached: boolean } {
  const rows: BucketDataDoc[] = [];
  for (const row of loadBucketDataDocument(context, doc)) {
    const bucket = row.bucketKey.bucket;
    const bucketStart = bucketMap.get(bucket);
    if (bucketStart == null) {
      throw new Error(`data for unexpected bucket: ${bucket}`);
    }
    if (row.o <= bucketStart) {
      continue;
    }
    if (row.o > endOpId) {
      continue;
    }

    rows.push(row);
    remainingLimit--;
    if (remainingLimit <= 0) {
      return { rows, remainingLimit, limitReached: true };
    }
  }
  return { rows, remainingLimit, limitReached: false };
}

export class MongoSyncBucketStorageV3 extends MongoSyncBucketStorage {
  declare readonly db: VersionedPowerSyncMongoV3;

  constructor(
    factory: MongoBucketStorage,
    replicationStreamId: number,
    replicationStream: MongoPersistedReplicationStream,
    replicationStreamName: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    super(factory, replicationStreamId, replicationStream, replicationStreamName, writeCheckpointMode, options);
    if (replicationStream.syncConfigIds.length != 1) {
      throw new ServiceAssertionError('Missing sync config id for storage v3');
    }
  }

  private get syncConfigId(): bson.ObjectId {
    return this.replicationStream.syncConfigIds[0];
  }

  private get syncRulesCollection(): mongo.Collection<ReplicationStreamDocumentV3> {
    return this.db.sync_rules as unknown as mongo.Collection<ReplicationStreamDocumentV3>;
  }

  private syncConfigMatch(extra: mongo.Document = {}): mongo.Filter<ReplicationStreamDocumentV3> {
    return {
      _id: this.replicationStreamId,
      sync_configs: {
        $elemMatch: {
          _id: this.syncConfigId,
          ...extra
        }
      }
    };
  }

  private syncConfigProjection(extra: mongo.Document = {}): mongo.Document {
    return {
      ...extra,
      sync_configs: 1
    };
  }

  private syncConfigArrayFilters(): mongo.UpdateOptions['arrayFilters'] {
    return [{ 'config._id': this.syncConfigId }];
  }

  /**
   * For now, we only support a single sync config per replication stream.
   *
   * In the future we'll add support for multiple.
   */
  private selectedSyncConfig(
    doc: Pick<ReplicationStreamDocumentV3, 'sync_configs'> | null
  ): SyncRuleConfigStateV3 | null {
    return doc?.sync_configs?.[0] ?? null;
  }

  createMongoCompactor(options: MongoCompactOptions): MongoCompactor {
    return new MongoCompactorV3(this, this.db, options);
  }

  protected createMongoChecksums(options: MongoSyncBucketStorageOptions): MongoChecksums {
    return new MongoChecksumsV3(this.db, this.replicationStreamId, {
      ...options.checksumOptions,
      storageConfig: options?.storageConfig,
      mapping: this.replicationStream.storageContent.mapping
    });
  }

  protected createMongoParameterCompactor(
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ): MongoParameterCompactor {
    return new MongoParameterCompactor(this.db, this.replicationStreamId, checkpoint, options, () =>
      this.db
        .listParameterIndexCollections(this.replicationStreamId)
        .then((collections) =>
          collections.map((c) => c.collection as unknown as lib_mongo.mongo.Collection<lib_mongo.mongo.Document>)
        )
    );
  }

  protected createWriterImpl(batchOptions: MongoBucketBatchOptions): storage.BucketStorageBatch {
    return new MongoBucketBatchV3(batchOptions);
  }

  protected async fetchCheckpointState(
    session: mongo.ClientSession
  ): Promise<{ checkpoint: bigint; lsn: string | null } | null> {
    const doc = await this.syncRulesCollection.findOne(
      this.syncConfigMatch({
        state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
      }),
      {
        session,
        projection: this.syncConfigProjection()
      }
    );
    const syncConfig = this.selectedSyncConfig(doc);
    if (!syncConfig?.snapshot_done) {
      return null;
    }
    return {
      checkpoint: syncConfig.last_checkpoint ?? 0n,
      lsn: syncConfig.last_checkpoint_lsn ?? null
    };
  }

  protected async getWriterSyncState(): Promise<WriterSyncState> {
    const doc = await this.syncRulesCollection.findOne(this.syncConfigMatch(), {
      projection: this.syncConfigProjection({ snapshot_lsn: 1 })
    });
    const syncConfig = this.selectedSyncConfig(doc);
    const checkpointLsn = syncConfig?.last_checkpoint_lsn ?? null;
    return {
      lastCheckpointLsn: checkpointLsn,
      resumeFromLsn: maxLsn(checkpointLsn, doc?.snapshot_lsn),
      keepaliveOp: syncConfig?.keepalive_op ?? null,
      syncConfigIds: [this.syncConfigId]
    };
  }

  protected async terminateSyncRuleState(): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          state: storage.SyncRuleState.TERMINATED,
          persisted_lsn: null,
          sync_configs: []
        }
      }
    );
  }

  protected async getStatusImpl(): Promise<storage.SyncRuleStatus> {
    const doc = await this.syncRulesCollection.findOne(this.syncConfigMatch(), {
      projection: this.syncConfigProjection({ state: 1, snapshot_lsn: 1, keepalive_op: 1 })
    });
    const syncConfig = this.selectedSyncConfig(doc);
    if (doc == null || syncConfig == null) {
      throw new ServiceAssertionError('Cannot find replication stream status');
    }

    return {
      snapshot_done: syncConfig.snapshot_done ?? false,
      snapshot_lsn: doc.snapshot_lsn ?? null,
      active: doc.state == storage.SyncRuleState.ACTIVE && syncConfig.state == storage.SyncRuleState.ACTIVE,
      checkpoint_lsn: syncConfig.last_checkpoint_lsn ?? null,
      keepalive_op: syncConfig.keepalive_op ?? null
    };
  }

  protected async clearSyncRuleState(): Promise<void> {
    await this.syncRulesCollection.updateOne(
      this.syncConfigMatch(),
      {
        $set: {
          persisted_lsn: null,
          'sync_configs.$[config].snapshot_done': false,
          'sync_configs.$[config].last_checkpoint_lsn': null,
          'sync_configs.$[config].last_checkpoint': null,
          'sync_configs.$[config].no_checkpoint_before': null,
          'sync_configs.$[config].keepalive_op': null
        },
        $unset: {
          snapshot_lsn: 1
        }
      },
      {
        maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS,
        arrayFilters: this.syncConfigArrayFilters()
      }
    );
  }

  protected async initializeVersionStorage(): Promise<void> {
    const mapping = this.mapping;
    for (let source of mapping.allBucketDefinitionIds()) {
      const collection = this.db.bucketData(this.replicationStreamId, source).collectionName;
      await this.db.db
        .createCollection(collection, { clusteredIndex: { name: '_id', unique: true, key: { _id: 1 } } })
        .catch((error) => {
          if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceExists') {
            return;
          }
          throw error;
        });
    }
    for (let indexId of mapping.allParameterIndexIds()) {
      await this.db.parameterIndex(this.replicationStreamId, indexId).createIndex(
        {
          lookup: 1,
          key: 1,
          _id: -1
        },
        {
          name: 'lookup_op_id'
        }
      );
    }
  }

  protected override get versionContext(): MongoSyncBucketStorageContext<VersionedPowerSyncMongoV3> {
    return {
      db: this.db,
      group_id: this.replicationStreamId,
      mapping: this.mapping
    };
  }

  protected async getParameterSetsImpl(
    checkpoint: MongoSyncBucketStorageCheckpoint,
    lookups: ScopedParameterLookup[],
    limit: number
  ): Promise<ParameterLookupRows[]> {
    return this.db.client.withSession({ snapshot: true }, async (session) => {
      setSessionSnapshotTime(session, checkpoint.snapshotTime);

      const buildLookupPipeline = (
        lookup: ScopedParameterLookup,
        index: number
      ): {
        collection: lib_mongo.mongo.Collection<BucketParameterDocumentV3>;
        pipeline: lib_mongo.mongo.Document[];
      } => {
        const indexId = lookup.indexId;
        const collection = this.db.parameterIndex(this.replicationStreamId, indexId);
        const lookupFilter = serializeParameterLookup(lookup);
        return {
          collection,
          pipeline: [
            {
              $match: {
                lookup: lookupFilter,
                _id: { $lte: checkpoint.checkpoint }
              }
            },
            {
              $sort: {
                key: 1,
                _id: -1
              }
            },
            {
              $group: {
                _id: {
                  key: '$key'
                },
                bucket_parameters: {
                  $first: '$bucket_parameters'
                }
              }
            },
            {
              $project: {
                _id: 0,
                bucket_parameters: 1,
                index: { $literal: index }
              }
            }
          ]
        };
      };

      const [firstLookup, ...remainingLookups] = lookups;
      const firstQuery = firstLookup == null ? null : buildLookupPipeline(firstLookup, 0);
      if (firstQuery == null) {
        return [];
      }

      const pipeline: lib_mongo.mongo.Document[] = [
        ...firstQuery.pipeline,
        ...remainingLookups.map((lookup, indexInRemaining) => {
          const query = buildLookupPipeline(lookup, indexInRemaining + 1);
          return {
            $unionWith: {
              coll: query.collection.collectionName,
              pipeline: query.pipeline
            }
          };
        }),
        { $unwind: '$bucket_parameters' },
        { $limit: limit + 1 }
      ];

      const rows = await firstQuery.collection
        .aggregate<{ index: number; bucket_parameters: SqliteJsonRow }>(pipeline, {
          session,
          readConcern: 'snapshot',
          maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
        })
        .toArray()
        .catch((e) => {
          throw lib_mongo.mapQueryError(e, 'while evaluating parameter queries');
        });

      if (rows.length > limit) {
        throw new ParameterSetLimitExceededError(limit);
      }

      const byLookup = Map.groupBy(rows, (row) => lookups[row.index]);

      const results: ParameterLookupRows[] = [];
      byLookup.forEach((value, lookup) => results.push({ lookup, rows: value.map((r) => r.bucket_parameters) }));
      return results;
    });
  }

  protected async *getBucketDataBatchImpl(
    checkpoint: utils.InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    if (dataBuckets.length == 0) {
      return;
    }

    if (checkpoint == null) {
      throw new Error('checkpoint is null');
    }

    const batchLimit = options?.limit ?? storage.DEFAULT_DOCUMENT_BATCH_LIMIT;
    const chunkSizeLimitBytes = options?.chunkLimitBytes ?? storage.DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES;
    const end = checkpoint;
    let remainingLimit = batchLimit;

    const requestsByDefinition = new Map<string, storage.BucketDataRequest[]>();
    for (const request of dataBuckets) {
      const definitionId = this.mapping.bucketSourceId(request.source);
      const requests = requestsByDefinition.get(definitionId) ?? [];
      requests.push(request);
      requestsByDefinition.set(definitionId, requests);
    }

    const definitionGroups = Array.from(requestsByDefinition.entries());
    for (const [groupIndex, [definitionId, requests]] of definitionGroups.entries()) {
      if (remainingLimit <= 0) {
        break;
      }
      const hasLaterDefinitionGroups = groupIndex < definitionGroups.length - 1;
      const bucketMap = new Map(requests.map((request) => [request.bucket, request.start]));
      const filters = Array.from(bucketMap.entries()).map(([bucket, start]) => ({
        _id: {
          $gt: { b: bucket, o: start },
          $lte: { b: bucket, o: new bson.MaxKey() }
        },
        min_op: { $lte: end }
        // MongoDB Filter<T> doesn't accept compound _id ranges or dotted field paths in its type.
      })) as unknown as lib_mongo.mongo.Filter<BucketDataDocumentV3>[];

      const collection = this.db.bucketData(
        this.replicationStreamId,
        definitionId
      ) as unknown as lib_mongo.mongo.Collection<BucketDataDocumentV3>;
      // MongoDB Filter<T> doesn't accept the $or operator in its type.
      const filter = { $or: filters } as unknown as lib_mongo.mongo.Filter<BucketDataDocumentV3>;
      const context = { replicationStreamId: this.replicationStreamId, definitionId };
      const limit = remainingLimit;

      const cursorOptions = { limit: remainingLimit, batchSize: remainingLimit + 1 };

      // raw: true returns Buffers, but the driver typing doesn't reflect that
      // without an explicit cast to FindCursor<Buffer>.
      const cursor = collection.find(filter, {
        session: undefined,
        sort: { _id: 1 },
        raw: true,
        maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS,
        ...cursorOptions
      }) as unknown as lib_mongo.mongo.FindCursor<Buffer>;

      let { data: rawData, hasMore } = await readSingleBatch(cursor).catch((e: unknown) => {
        throw lib_mongo.mapQueryError(e, 'while reading bucket data');
      });

      if (cursorOptions.limit != null && rawData.length >= cursorOptions.limit) {
        hasMore = true;
      }

      const data: BucketDataDoc[] = [];
      const documentOpCounts: number[] = [];
      const documentSizes: number[] = [];
      let sharedRemainingLimit = limit;
      let limitReached = false;
      // Buckets whose matched document contributed no rows after filtering.
      const completeEmptyBuckets = new Set<string>();

      // Deserialize all docs once
      const docs: any[] = rawData.map((raw) => bson.deserialize(raw, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS));

      // Pre-fetch S3 objects for all S3-backed docs in this batch
      if (this.objectStorage) {
        const store = new BucketDataObjectStorage(this.objectStorage);
        const s3Docs = docs.filter((d) => d.storage_ref);
        if (s3Docs.length > 0) {
          await Promise.all(
            s3Docs.map(async (doc) => {
              doc.ops = await store.retrieve(doc.storage_ref.path);
            })
          );
        }
      }

      // Track sizes: use doc.size (the total decompressed data size stored at write
      // time) for S3-backed docs; fall back to raw byteLength for inline docs.
      const docSizes: number[] = rawData.map((raw, i) => {
        const doc = docs[i];
        return doc.storage_ref ? doc.size : raw.byteLength;
      });

      for (const [i, doc] of docs.entries()) {
        const {
          rows,
          remainingLimit,
          limitReached: docLimitReached
        } = extractRowsFromDocument(doc, context, bucketMap, end, sharedRemainingLimit);
        if (rows.length == 0) {
          // The document straddles the requested (start, end] window: it matched the
          // query, but none of its ops are in range. Since its _id.o (max op) must be
          // > end (any op <= end would have been > start, and thus in range), and
          // document ranges per bucket are disjoint, no later document for this bucket
          // can match either. The bucket is complete through the checkpoint.
          completeEmptyBuckets.add(doc._id.b);
        }
        data.push(...rows);
        documentOpCounts.push(rows.length);
        documentSizes.push(docSizes[i]);
        sharedRemainingLimit = remainingLimit;
        if (docLimitReached) {
          limitReached = true;
          break;
        }
      }

      const batchHasMore = hasMore || limitReached;

      // Empty chunks are not forwarded to clients, but report progress to the caller:
      // the bucket's position advances to the checkpoint, so it is not re-requested.
      // If the batch produced no data at all, the last empty chunk also carries the
      // has_more signal, so the caller re-requests the remaining buckets instead of
      // treating an all-filtered batch as the end of the stream.
      const emptyBuckets = Array.from(completeEmptyBuckets);
      for (const [index, bucket] of emptyBuckets.entries()) {
        const startOpId = bucketMap.get(bucket);
        if (startOpId == null) {
          throw new ServiceAssertionError(`data for unexpected bucket: ${bucket}`);
        }
        const isLastChunkOfBatch = data.length == 0 && index == emptyBuckets.length - 1;
        yield {
          chunkData: {
            bucket,
            after: internalToExternalOpId(startOpId),
            has_more: isLastChunkOfBatch && batchHasMore,
            data: [],
            next_after: internalToExternalOpId(end)
          },
          targetOp: null
        };
      }

      if (data.length == 0) {
        if (batchHasMore) {
          // The remaining documents are read in the next round, after the caller has
          // advanced the positions of the empty buckets above.
          return;
        }
        continue;
      }

      remainingLimit -= data.length;

      let currentChunkSizeBytes = 0;
      let currentChunk: utils.SyncBucketData | null = null;
      let targetOp: InternalOpId | null = null;

      for (const { row, docIndex, isLastOpInDocument } of walkDocumentOps(data, documentOpCounts, documentSizes)) {
        const bucket = row.bucketKey.bucket;

        if (currentChunk == null || currentChunk.bucket != bucket || currentChunkSizeBytes >= chunkSizeLimitBytes) {
          let start: ProtocolOpId | undefined = undefined;
          if (currentChunk != null) {
            if (currentChunk.bucket == bucket) {
              currentChunk.has_more = true;
              start = currentChunk.next_after;
            }

            const yieldChunk = currentChunk;
            currentChunk = null;
            currentChunkSizeBytes = 0;
            yield { chunkData: yieldChunk, targetOp: targetOp };
            targetOp = null;
          }

          if (start == null) {
            const startOpId = bucketMap.get(bucket);
            if (startOpId == null) {
              throw new Error(`data for unexpected bucket: ${bucket}`);
            }
            start = internalToExternalOpId(startOpId);
          }
          currentChunk = {
            bucket,
            after: start,
            has_more: false,
            data: [],
            next_after: start
          };
        }

        const entry = mapOpEntry(row);
        if (row.target_op != null && (targetOp == null || row.target_op > targetOp)) {
          targetOp = row.target_op;
        }

        currentChunk.data.push(entry);
        currentChunk.next_after = entry.op_id;

        if (isLastOpInDocument) {
          currentChunkSizeBytes += documentSizes[docIndex];
        }
      }

      if (currentChunk != null) {
        const yieldChunk = currentChunk;
        yieldChunk.has_more = batchHasMore || (remainingLimit <= 0 && hasLaterDefinitionGroups);
        yield { chunkData: yieldChunk, targetOp: targetOp };
      }

      if (batchHasMore || remainingLimit <= 0) {
        return;
      }
    }
  }

  protected async clearBucketData(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listBucketDataCollections(this.replicationStreamId)) {
      await collection.drop();
    }
  }

  protected async clearParameterIndexes(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listParameterIndexCollections(this.replicationStreamId)) {
      await collection.collection.drop();
    }
  }

  protected async clearSourceRecords(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listSourceRecordCollections(this.replicationStreamId)) {
      await collection.drop();
    }
  }

  protected async clearBucketState(_signal?: AbortSignal): Promise<void> {
    await this.db
      .bucketState(this.replicationStreamId)
      .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return;
        }
        throw error;
      });
  }

  protected async clearSourceTables(_signal?: AbortSignal): Promise<void> {
    await this.db
      .sourceTables(this.replicationStreamId)
      .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return;
        }
        throw error;
      });
  }

  protected async getDataBucketChangesImpl(
    options: storage.GetCheckpointChangesOptions
  ): Promise<Pick<storage.CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
    const limit = 1000;
    const bucketStateUpdates: { _id: string; last_op: bigint }[] = (await this.db
      .bucketState(this.replicationStreamId)
      .aggregate(
        [
          {
            $match: {
              last_op: { $gt: options.lastCheckpoint.checkpoint }
            }
          },
          {
            $group: {
              _id: '$_id.b',
              last_op: { $max: '$last_op' }
            }
          },
          {
            $sort: {
              last_op: 1
            }
          },
          {
            $limit: limit + 1
          }
        ],
        { maxTimeMS: lib_mongo.MONGO_CHECKSUM_TIMEOUT_MS }
      )
      .toArray()) as { _id: string; last_op: bigint }[];

    const buckets = bucketStateUpdates.map((doc) => doc._id);
    const invalidateDataBuckets = buckets.length > limit;

    return {
      invalidateDataBuckets,
      updatedDataBuckets: invalidateDataBuckets ? new Set<string>() : new Set(buckets)
    };
  }

  protected async getParameterBucketChangesImpl(
    options: storage.GetCheckpointChangesOptions
  ): Promise<Pick<storage.CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    const limit = 1000;
    const indexIds = this.mapping.allParameterIndexIds();
    const collections = indexIds.map((indexId) => ({
      indexId,
      collection: this.db.parameterIndex(this.replicationStreamId, indexId)
    }));
    if (collections.length == 0) {
      return {
        invalidateParameterBuckets: false,
        updatedParameterLookups: new Set<string>()
      };
    }
    const checkpointFilter = {
      _id: { $gt: options.lastCheckpoint.checkpoint, $lte: options.nextCheckpoint.checkpoint }
    };
    const pipelineForCollection = (indexId: string) => [
      {
        $match: checkpointFilter
      },
      {
        $project: {
          _id: 0,
          lookup: 1,
          indexId: { $literal: indexId }
        }
      }
    ];
    const [firstCollection, ...remainingCollections] = collections;
    const parameterUpdates: { lookup: bson.Binary; indexId: string }[] = (await firstCollection.collection
      .aggregate(
        [
          ...pipelineForCollection(firstCollection.indexId),
          ...remainingCollections.map((collection) => {
            return {
              $unionWith: {
                coll: collection.collection.collectionName,
                pipeline: pipelineForCollection(collection.indexId)
              }
            };
          }),
          {
            $limit: limit + 1
          }
        ],
        {
          batchSize: limit + 2,
          maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
        }
      )
      .toArray()) as { lookup: bson.Binary; indexId: string }[];

    const invalidateParameterUpdates = parameterUpdates.length > limit;

    return {
      invalidateParameterBuckets: invalidateParameterUpdates,
      updatedParameterLookups: invalidateParameterUpdates
        ? new Set<string>()
        : new Set<string>(
            parameterUpdates.map((p) => JSONBig.stringify(deserializeParameterLookup(p.lookup, p.indexId)))
          )
    };
  }
}
