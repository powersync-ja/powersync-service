import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import {
  CheckpointChanges,
  GetCheckpointChangesOptions,
  InternalOpId,
  internalToExternalOpId,
  ParameterSetLimitExceededError,
  ProtocolOpId,
  storage,
  utils
} from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mapOpEntry, readSingleBatch, setSessionSnapshotTime } from '../../../utils/util.js';
import { MongoBucketStorage } from '../../MongoBucketStorage.js';
import {
  MongoSyncBucketStorageCheckpoint,
  MongoSyncBucketStorageContext
} from '../common/MongoSyncBucketStorageContext.js';
import { CommonSourceTableDocument } from '../models.js';
import { MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { MongoChecksums } from '../MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from '../MongoCompactor.js';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { MongoPersistedSyncRulesContent } from '../MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageOptions } from '../MongoSyncBucketStorage.js';
import { BucketDataDocumentV3, BucketParameterDocumentV3, loadBucketDataDocumentV3 } from './models.js';
import { MongoBucketBatchV3 } from './MongoBucketBatchV3.js';
import { MongoChecksumsV3 } from './MongoChecksumsV3.js';
import { MongoCompactorV3 } from './MongoCompactorV3.js';
import { MongoParameterCompactorV3 } from './MongoParameterCompactorV3.js';
import { deserializeParameterLookupV3, serializeParameterLookupV3 } from './MongoParameterLookupV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class MongoSyncBucketStorageV3 extends MongoSyncBucketStorage {
  // Declare types to be more specific
  declare readonly db: VersionedPowerSyncMongoV3;
  declare readonly checksums: MongoChecksumsV3;

  constructor(
    factory: MongoBucketStorage,
    group_id: number,
    sync_rules: MongoPersistedSyncRulesContent,
    slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    super(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
  }

  protected async initializeVersionStorage(): Promise<void> {
    const mapping = this.mapping;
    for (let source of mapping.allBucketDefinitionIds()) {
      const collection = this.db.bucketDataV3(this.group_id, source).collectionName;
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
      await this.db.parameterIndexV3(this.group_id, indexId).createIndex(
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

  protected createMongoChecksums(options: MongoSyncBucketStorageOptions): MongoChecksums {
    return new MongoChecksumsV3(this.db, this.group_id, {
      ...options.checksumOptions,
      storageConfig: options?.storageConfig,
      mapping: this.sync_rules.mapping
    });
  }

  createMongoCompactor(options: MongoCompactOptions): MongoCompactor {
    return new MongoCompactorV3(this, this.db, options);
  }

  protected createMongoParameterCompactor(
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ): MongoParameterCompactor {
    return new MongoParameterCompactorV3(this.db, this.group_id, checkpoint, options);
  }

  protected createWriterImpl(batchOptions: MongoBucketBatchOptions): storage.BucketStorageBatch {
    return new MongoBucketBatchV3(batchOptions);
  }

  protected sourceTableBaseId(): Partial<CommonSourceTableDocument> {
    return {};
  }

  protected augmentCreatedSourceTableDocument(
    createDoc: CommonSourceTableDocument,
    options: storage.ResolveTableOptions,
    candidateSourceTable: storage.SourceTable
  ): void {
    const bucketDataSourceIds = options.sync_rules.definition.bucketDataSources
      .filter((source) => source.tableSyncsData(candidateSourceTable))
      .map((source) => this.mapping.bucketSourceId(source));
    const parameterLookupSourceIds = options.sync_rules.definition.bucketParameterLookupSources
      .filter((source) => source.tableSyncsParameters(candidateSourceTable))
      .map((source) => this.mapping.parameterLookupId(source));

    Object.assign(createDoc, {
      bucket_data_source_ids: bucketDataSourceIds,
      parameter_lookup_source_ids: parameterLookupSourceIds
    });
  }

  protected async initializeResolvedSourceRecords(sourceTableId: bson.ObjectId): Promise<void> {
    await this.db.initializeSourceRecordsCollection(this.group_id, sourceTableId);
  }

  protected override get versionContext(): MongoSyncBucketStorageContext<VersionedPowerSyncMongoV3> {
    return {
      db: this.db,
      group_id: this.group_id,
      mapping: this.mapping
    };
  }

  protected getParameterSetsImpl(
    checkpoint: MongoSyncBucketStorageCheckpoint,
    lookups: ScopedParameterLookup[],
    limit: number
  ): Promise<SqliteJsonRow[]> {
    return getParameterSetsV3(this.versionContext, checkpoint, lookups, limit);
  }

  protected getBucketDataBatchImpl(
    checkpoint: utils.InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    return getBucketDataBatchV3(this.versionContext, checkpoint, dataBuckets, options);
  }

  protected async clearBucketData(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listBucketDataCollectionsV3(this.group_id)) {
      await collection.drop();
    }
  }

  protected async clearParameterIndexes(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listParameterIndexCollectionsV3(this.group_id)) {
      await collection.collection.drop();
    }
  }

  protected async clearSourceRecords(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listSourceRecordCollectionsV3(this.group_id)) {
      await collection.drop();
    }
  }

  protected async clearBucketState(_signal?: AbortSignal): Promise<void> {
    await this.db
      .bucketStateV3(this.group_id)
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
      .sourceTablesV3(this.group_id)
      .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return;
        }
        throw error;
      });
  }

  protected getDataBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
    return getDataBucketChangesV3(this.versionContext, options);
  }

  protected getParameterBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    return getParameterBucketChangesV3(this.versionContext, options);
  }
}

export async function getParameterSetsV3(
  ctx: MongoSyncBucketStorageContext<VersionedPowerSyncMongoV3>,
  checkpoint: MongoSyncBucketStorageCheckpoint,
  lookups: ScopedParameterLookup[],
  limit: number
): Promise<SqliteJsonRow[]> {
  return ctx.db.client.withSession({ snapshot: true }, async (session) => {
    setSessionSnapshotTime(session, checkpoint.snapshotTime);

    const buildLookupPipeline = (
      lookup: ScopedParameterLookup
    ): {
      collection: mongo.Collection<BucketParameterDocumentV3>;
      pipeline: mongo.Document[];
    } => {
      const indexId = lookup.indexId;
      const collection = ctx.db.parameterIndexV3(ctx.group_id, indexId);
      const lookupFilter = serializeParameterLookupV3(lookup);

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
              bucket_parameters: 1
            }
          },
          // This limit still allows returning too many rows because this filter might be put into a $unionWith, but
          // at least the amount of rows we'd return isn't unbounded.
          { $limit: limit + 1 }
        ]
      };
    };

    const [firstLookup, ...remainingLookups] = lookups;
    const firstQuery = firstLookup == null ? null : buildLookupPipeline(firstLookup);
    if (firstQuery == null) {
      return [];
    }

    const pipeline: mongo.Document[] = [
      ...firstQuery.pipeline,
      ...remainingLookups.map((lookup) => {
        const query = buildLookupPipeline(lookup);
        return {
          $unionWith: {
            coll: query.collection.collectionName,
            pipeline: query.pipeline
          }
        };
      })
    ];

    const rows = await firstQuery.collection
      .aggregate<{ bucket_parameters: SqliteJsonRow[] }>(pipeline, {
        session,
        readConcern: 'snapshot',
        maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
      })
      .toArray()
      .catch((e) => {
        throw lib_mongo.mapQueryError(e, 'while evaluating parameter queries');
      });

    const expandedRows = rows.flatMap((row) => row.bucket_parameters);
    if (limit != null && expandedRows.length > limit) {
      throw new ParameterSetLimitExceededError(limit);
    }

    return expandedRows;
  });
}

export async function* getBucketDataBatchV3(
  ctx: MongoSyncBucketStorageContext<VersionedPowerSyncMongoV3>,
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
    const definitionId = ctx.mapping.bucketSourceId(request.source);
    const requests = requestsByDefinition.get(definitionId) ?? [];
    requests.push(request);
    requestsByDefinition.set(definitionId, requests);
  }

  const definitionGroups = Array.from(requestsByDefinition.entries());
  for (let groupIndex = 0; groupIndex < definitionGroups.length && remainingLimit > 0; groupIndex++) {
    const [definitionId, requests] = definitionGroups[groupIndex];
    const hasLaterDefinitionGroups = groupIndex < definitionGroups.length - 1;
    const bucketMap = new Map(requests.map((request) => [request.bucket, request.start]));
    const filters: mongo.Filter<BucketDataDocumentV3>[] = Array.from(bucketMap.entries()).map(([bucket, start]) => ({
      _id: {
        $gt: {
          b: bucket,
          o: start
        },
        $lte: {
          b: bucket,
          o: end as any
        }
      }
    }));

    const cursor = ctx.db.bucketDataV3(ctx.group_id, definitionId).find(
      {
        $or: filters
      },
      {
        session: undefined,
        sort: { _id: 1 },
        limit: remainingLimit,
        batchSize: remainingLimit + 1,
        raw: true,
        maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
      }
    ) as unknown as mongo.FindCursor<Buffer>;

    let { data, hasMore: batchHasMore } = await readSingleBatch(cursor).catch((e) => {
      throw lib_mongo.mapQueryError(e, 'while reading bucket data');
    });
    if (data.length == remainingLimit) {
      batchHasMore = true;
    }
    if (data.length == 0) {
      continue;
    }

    remainingLimit -= data.length;

    let chunkSizeBytes = 0;
    let currentChunk: utils.SyncBucketData | null = null;
    let targetOp: InternalOpId | null = null;

    for (let rawData of data) {
      const row = loadBucketDataDocumentV3(
        { replicationStreamId: ctx.group_id, definitionId },
        bson.deserialize(rawData, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS) as BucketDataDocumentV3
      );
      const bucket = row.bucketKey.bucket;

      if (currentChunk == null || currentChunk.bucket != bucket || chunkSizeBytes >= chunkSizeLimitBytes) {
        let start: ProtocolOpId | undefined = undefined;
        if (currentChunk != null) {
          if (currentChunk.bucket == bucket) {
            currentChunk.has_more = true;
            start = currentChunk.next_after;
          }

          const yieldChunk = currentChunk;
          currentChunk = null;
          chunkSizeBytes = 0;
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
      chunkSizeBytes += rawData.byteLength;
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

export async function getDataBucketChangesV3(
  ctx: MongoSyncBucketStorageContext<VersionedPowerSyncMongoV3>,
  options: GetCheckpointChangesOptions
): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
  const limit = 1000;
  const bucketStateUpdates = await ctx.db
    .bucketStateV3(ctx.group_id)
    .aggregate<{ _id: string; last_op: bigint }>(
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
    .toArray();

  const buckets = bucketStateUpdates.map((doc) => doc._id);
  const invalidateDataBuckets = buckets.length > limit;

  return {
    invalidateDataBuckets,
    updatedDataBuckets: invalidateDataBuckets ? new Set<string>() : new Set(buckets)
  };
}

export async function getParameterBucketChangesV3(
  ctx: MongoSyncBucketStorageContext<VersionedPowerSyncMongoV3>,
  options: GetCheckpointChangesOptions
): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
  const limit = 1000;
  const indexIds = ctx.mapping.allParameterIndexIds();
  const collections = indexIds.map((indexId) => ({
    indexId,
    collection: ctx.db.parameterIndexV3(ctx.group_id, indexId)
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
  const parameterUpdates = await firstCollection.collection
    .aggregate<{ lookup: bson.Binary; indexId: string }>(
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
    .toArray();

  const invalidateParameterUpdates = parameterUpdates.length > limit;

  return {
    invalidateParameterBuckets: invalidateParameterUpdates,
    updatedParameterLookups: invalidateParameterUpdates
      ? new Set<string>()
      : new Set<string>(
          parameterUpdates.map((p) => JSONBig.stringify(deserializeParameterLookupV3(p.lookup, p.indexId)))
        )
  };
}
