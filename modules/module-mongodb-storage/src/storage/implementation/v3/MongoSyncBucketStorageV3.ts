import * as lib_mongo from '@powersync/lib-service-mongodb';
import {
  InternalOpId,
  internalToExternalOpId,
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
import { AbstractMongoSyncBucketStorage, MongoSyncBucketStorageOptions } from '../AbstractMongoSyncBucketStorage.js';
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { MongoSyncBucketStorageCheckpoint } from '../common/MongoSyncBucketStorageContext.js';
import { BucketDataDocument, BucketDocumentFormatAdapter } from '../document-formats/bucket-document-format.js';
import { deserializeParameterLookup, serializeParameterLookup } from '../document-formats/parameter-lookup.js';
import { CommonSourceTableDocument } from '../models.js';
import { MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { MongoChecksums } from '../MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from '../MongoCompactor.js';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { MongoPersistedSyncRulesContent } from '../MongoPersistedSyncRulesContent.js';
import { MongoBucketBatchV3 } from './MongoBucketBatchV3.js';
import { MongoChecksumsV3 } from './MongoChecksumsV3.js';
import { MongoCompactorV3 } from './MongoCompactorV3.js';

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
  doc: any,
  context: { replicationStreamId: number; definitionId: string },
  bucketMap: Map<string, InternalOpId>,
  endOpId: InternalOpId,
  remainingLimit: number,
  formatAdapter: BucketDocumentFormatAdapter
): { rows: BucketDataDoc[]; remainingLimit: number; limitReached: boolean } {
  const rows: BucketDataDoc[] = [];
  for (const row of formatAdapter.loadDocument(context, doc)) {
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

export class MongoSyncBucketStorageV3 extends AbstractMongoSyncBucketStorage {
  get db(): VersionedPowerSyncMongo {
    return super.db as VersionedPowerSyncMongo;
  }

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

  createMongoCompactor(options: MongoCompactOptions): MongoCompactor {
    return new MongoCompactorV3(this, this.db, options);
  }

  protected createMongoChecksums(options: MongoSyncBucketStorageOptions): MongoChecksums {
    return new MongoChecksumsV3(this.db, this.group_id, {
      ...options.checksumOptions,
      storageConfig: options?.storageConfig,
      mapping: this.sync_rules.mapping
    });
  }

  protected createMongoParameterCompactor(
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ): MongoParameterCompactor {
    return new MongoParameterCompactor(this.db, this.group_id, checkpoint, options, () =>
      this.db
        .listParameterIndexCollections(this.group_id)
        .then((collections) =>
          collections.map((c) => c.collection as unknown as lib_mongo.mongo.Collection<lib_mongo.mongo.Document>)
        )
    );
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

  protected async initializeVersionStorage(): Promise<void> {
    const mapping = this.mapping;
    for (let source of mapping.allBucketDefinitionIds()) {
      const collection = this.db.bucketData(this.group_id, source).collectionName;
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
      await this.db.parameterIndex(this.group_id, indexId).createIndex(
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
        collection: lib_mongo.mongo.Collection<any>;
        pipeline: lib_mongo.mongo.Document[];
      } => {
        const indexId = lookup.indexId;
        const collection = this.db.parameterIndex(this.group_id, indexId);
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
          $gt: {
            b: bucket,
            o: start
          },
          $lte: {
            b: bucket,
            o: end
          }
        }
        // MongoDB Filter<T> doesn't accept dotted field paths like '_id.o' in its type.
      })) as unknown as lib_mongo.mongo.Filter<BucketDataDocument>[];

      const minStart = Array.from(bucketMap.values()).reduce((min, val) => (val < min ? val : min));

      const collection = this.db.bucketData<BucketDataDocument>(this.group_id, definitionId);
      const formatAdapter = new BucketDocumentFormatAdapter();
      // MongoDB Filter<T> doesn't accept the $or operator in its type.
      const filter = { $or: filters } as unknown as lib_mongo.mongo.Filter<BucketDataDocument>;
      const context = { replicationStreamId: this.group_id, definitionId };
      const startOpId = minStart;
      const endOpId = end;
      const limit = remainingLimit;

      const { filter: rangeFilter, cursorOptions } = formatAdapter.buildBucketDataQuery({
        startOpId,
        endOpId,
        remainingLimit: limit
      });

      const combinedFilter = {
        // MongoDB Filter<T> doesn't accept the $and operator in its type.
        $and: [filter, rangeFilter]
      } as unknown as lib_mongo.mongo.Filter<BucketDataDocument>;

      // raw: true returns Buffers, but the driver typing doesn't reflect that
      // without an explicit cast to FindCursor<Buffer>.
      const cursor = collection.find(combinedFilter, {
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
      let chunkSizeBytes = 0;
      let sharedRemainingLimit = limit;
      let limitReached = false;

      for (const raw of rawData) {
        const doc = bson.deserialize(raw, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS);
        const {
          rows,
          remainingLimit,
          limitReached: docLimitReached
        } = extractRowsFromDocument(doc, context, bucketMap, endOpId, sharedRemainingLimit, formatAdapter);
        data.push(...rows);
        documentOpCounts.push(rows.length);
        documentSizes.push(raw.byteLength);
        chunkSizeBytes += raw.byteLength;
        sharedRemainingLimit = remainingLimit;
        if (docLimitReached) {
          limitReached = true;
          break;
        }
      }

      const batchHasMore = hasMore || limitReached;

      if (data.length == 0) {
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
    for (const collection of await this.db.listBucketDataCollections(this.group_id)) {
      await collection.drop();
    }
  }

  protected async clearParameterIndexes(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listParameterIndexCollections(this.group_id)) {
      await collection.collection.drop();
    }
  }

  protected async clearSourceRecords(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listSourceRecordCollections(this.group_id)) {
      await collection.drop();
    }
  }

  protected async clearBucketState(_signal?: AbortSignal): Promise<void> {
    await this.db
      .bucketState(this.group_id)
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
      .sourceTables(this.group_id)
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
      .bucketState(this.group_id)
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
      collection: this.db.parameterIndex(this.group_id, indexId)
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
