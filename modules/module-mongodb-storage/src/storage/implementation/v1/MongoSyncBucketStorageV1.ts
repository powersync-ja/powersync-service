import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import {
  CheckpointChanges,
  deserializeParameterLookup,
  GetCheckpointChangesOptions,
  InternalOpId,
  internalToExternalOpId,
  ProtocolOpId,
  storage,
  utils
} from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { idPrefixFilter, mapOpEntry, readSingleBatch, setSessionSnapshotTime } from '../../../utils/util.js';
import { MongoBucketStorage } from '../../MongoBucketStorage.js';
import {
  MongoSyncBucketStorageCheckpoint,
  MongoSyncBucketStorageContext
} from '../common/MongoSyncBucketStorageContext.js';
import { CommonSourceTableDocument, SourceKey } from '../models.js';
import { MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { MongoChecksums } from '../MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from '../MongoCompactor.js';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { MongoPersistedSyncRulesContent } from '../MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageOptions } from '../MongoSyncBucketStorage.js';
import { BucketDataDocumentV1, BucketDataKeyV1, BucketStateDocument, loadBucketDataDocumentV1 } from './models.js';
import { MongoBucketBatchV1 } from './MongoBucketBatchV1.js';
import { MongoChecksumsV1 } from './MongoChecksumsV1.js';
import { MongoCompactorV1 } from './MongoCompactorV1.js';
import { MongoParameterCompactorV1 } from './MongoParameterCompactorV1.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';

export class MongoSyncBucketStorageV1 extends MongoSyncBucketStorage {
  // Declare types to be more specific
  declare readonly db: VersionedPowerSyncMongoV1;
  declare readonly checksums: MongoChecksumsV1;

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

  protected async initializeVersionStorage(): Promise<void> {}

  protected createWriterImpl(batchOptions: MongoBucketBatchOptions): storage.BucketStorageBatch {
    return new MongoBucketBatchV1(batchOptions);
  }

  protected createMongoChecksums(options: MongoSyncBucketStorageOptions): MongoChecksums {
    return new MongoChecksumsV1(this.db, this.group_id, {
      ...options.checksumOptions,
      storageConfig: options?.storageConfig,
      mapping: this.sync_rules.mapping
    });
  }

  createMongoCompactor(options: MongoCompactOptions): MongoCompactor {
    return new MongoCompactorV1(this, this.db, options);
  }

  protected createMongoParameterCompactor(
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ): MongoParameterCompactor {
    return new MongoParameterCompactorV1(this.db, this.group_id, checkpoint, options);
  }

  protected sourceTableBaseId(): Partial<CommonSourceTableDocument> {
    return { group_id: this.group_id };
  }

  protected augmentCreatedSourceTableDocument(
    _createDoc: CommonSourceTableDocument,
    _options: storage.ResolveTableOptions,
    _candidateSourceTable: storage.SourceTable
  ): void {}

  protected async initializeResolvedSourceRecords(_sourceTableId: bson.ObjectId): Promise<void> {}

  protected override get versionContext(): MongoSyncBucketStorageContext<VersionedPowerSyncMongoV1> {
    return {
      db: this.db,
      group_id: this.group_id,
      mapping: this.mapping
    };
  }

  protected getParameterSetsImpl(
    checkpoint: MongoSyncBucketStorageCheckpoint,
    lookups: ScopedParameterLookup[]
  ): Promise<SqliteJsonRow[]> {
    return getParameterSetsV1(this.versionContext, checkpoint, lookups);
  }

  protected getBucketDataBatchImpl(
    checkpoint: utils.InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    return getBucketDataBatchV1(this.versionContext, checkpoint, dataBuckets, options);
  }

  protected async clearBucketData(signal?: AbortSignal): Promise<void> {
    await this.clearDeleteMany(
      'bucket data',
      () =>
        this.db.bucket_data.deleteMany(
          {
            _id: idPrefixFilter<BucketDataKeyV1>({ g: this.group_id }, ['b', 'o'])
          },
          { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
        ),
      signal
    );
  }

  protected async clearParameterIndexes(signal?: AbortSignal): Promise<void> {
    await this.clearDeleteMany(
      'parameter index',
      () =>
        this.db.parameterIndexV1.deleteMany(
          {
            'key.g': this.group_id
          },
          { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
        ),
      signal
    );
  }

  protected async clearSourceRecords(signal?: AbortSignal): Promise<void> {
    await this.clearDeleteMany(
      'source records',
      () =>
        this.db.sourceRecordsV1.deleteMany(
          {
            _id: idPrefixFilter<SourceKey>({ g: this.group_id }, ['t', 'k'])
          },
          { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
        ),
      signal
    );
  }

  protected async clearBucketState(signal?: AbortSignal): Promise<void> {
    await this.clearDeleteMany(
      'bucket state',
      () =>
        this.db.bucketStateV1.deleteMany(
          {
            _id: idPrefixFilter<BucketStateDocument['_id']>({ g: this.group_id }, ['b'])
          },
          { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
        ),
      signal
    );
  }

  protected async clearSourceTables(signal?: AbortSignal): Promise<void> {
    await this.clearDeleteMany(
      'source tables',
      () =>
        this.db.commonSourceTables(this.group_id).deleteMany(
          {
            group_id: this.group_id
          },
          { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
        ),
      signal
    );
  }

  protected getDataBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
    return getDataBucketChangesV1(this.versionContext, options);
  }

  protected getParameterBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    return getParameterBucketChangesV1(this.versionContext, options);
  }
}

export async function getParameterSetsV1(
  ctx: MongoSyncBucketStorageContext<VersionedPowerSyncMongoV1>,
  checkpoint: MongoSyncBucketStorageCheckpoint,
  lookups: ScopedParameterLookup[]
): Promise<SqliteJsonRow[]> {
  return ctx.db.client.withSession({ snapshot: true }, async (session) => {
    setSessionSnapshotTime(session, checkpoint.snapshotTime);
    const lookupFilter = lookups.map((lookup) => {
      return storage.serializeLookup(lookup);
    });
    const rows = await ctx.db.parameterIndexV1
      .aggregate(
        [
          {
            $match: {
              'key.g': ctx.group_id,
              lookup: { $in: lookupFilter },
              _id: { $lte: checkpoint.checkpoint }
            }
          },
          {
            $sort: {
              _id: -1
            }
          },
          {
            $group: {
              _id: { key: '$key', lookup: '$lookup' },
              bucket_parameters: {
                $first: '$bucket_parameters'
              }
            }
          }
        ],
        {
          session,
          readConcern: 'snapshot',
          maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
        }
      )
      .toArray()
      .catch((e) => {
        throw lib_mongo.mapQueryError(e, 'while evaluating parameter queries');
      });
    const groupedParameters = rows.map((row) => {
      return row.bucket_parameters;
    });
    return groupedParameters.flat();
  });
}

export async function* getBucketDataBatchV1(
  ctx: MongoSyncBucketStorageContext<VersionedPowerSyncMongoV1>,
  checkpoint: utils.InternalOpId,
  dataBuckets: storage.BucketDataRequest[],
  options?: storage.BucketDataBatchOptions
): AsyncIterable<storage.SyncBucketDataChunk> {
  if (dataBuckets.length == 0) {
    return;
  }
  let filters: mongo.Filter<BucketDataDocumentV1>[] = [];
  const bucketMap = new Map(dataBuckets.map((request) => [request.bucket, request.start]));

  if (checkpoint == null) {
    throw new Error('checkpoint is null');
  }
  const end = checkpoint;
  for (let { bucket: name, start } of dataBuckets) {
    filters.push({
      _id: {
        $gt: {
          g: ctx.group_id,
          b: name,
          o: start
        },
        $lte: {
          g: ctx.group_id,
          b: name,
          o: end as any
        }
      }
    });
  }

  const batchLimit = options?.limit ?? storage.DEFAULT_DOCUMENT_BATCH_LIMIT;
  const chunkSizeLimitBytes = options?.chunkLimitBytes ?? storage.DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES;

  const cursor = ctx.db.bucket_data.find(
    {
      $or: filters
    },
    {
      session: undefined,
      sort: { _id: 1 },
      limit: batchLimit,
      batchSize: batchLimit + 1,
      raw: true,
      maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
    }
  ) as unknown as mongo.FindCursor<Buffer>;

  let { data, hasMore: batchHasMore } = await readSingleBatch(cursor).catch((e) => {
    throw lib_mongo.mapQueryError(e, 'while reading bucket data');
  });
  if (data.length == batchLimit) {
    batchHasMore = true;
  }

  let chunkSizeBytes = 0;
  let currentChunk: utils.SyncBucketData | null = null;
  let targetOp: InternalOpId | null = null;

  for (let rawData of data) {
    const row = loadBucketDataDocumentV1(
      bson.deserialize(rawData, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS) as BucketDataDocumentV1
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
      targetOp = null;
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
    yieldChunk.has_more = batchHasMore;
    yield { chunkData: yieldChunk, targetOp: targetOp };
  }
}

export async function getDataBucketChangesV1(
  ctx: MongoSyncBucketStorageContext<VersionedPowerSyncMongoV1>,
  options: GetCheckpointChangesOptions
): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
  const limit = 1000;
  const bucketStateUpdates = await ctx.db.bucketStateV1
    .find(
      {
        '_id.g': ctx.group_id,
        last_op: { $gt: options.lastCheckpoint.checkpoint }
      },
      {
        projection: {
          '_id.b': 1
        },
        limit: limit + 1,
        batchSize: limit + 2,
        singleBatch: true
      }
    )
    .toArray();

  const buckets = bucketStateUpdates.map((doc) => doc._id.b);
  const invalidateDataBuckets = buckets.length > limit;

  return {
    invalidateDataBuckets,
    updatedDataBuckets: invalidateDataBuckets ? new Set<string>() : new Set(buckets)
  };
}

export async function getParameterBucketChangesV1(
  ctx: MongoSyncBucketStorageContext<VersionedPowerSyncMongoV1>,
  options: GetCheckpointChangesOptions
): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
  const limit = 1000;
  const parameterUpdates = await ctx.db.parameterIndexV1
    .find(
      {
        _id: { $gt: options.lastCheckpoint.checkpoint, $lte: options.nextCheckpoint.checkpoint },
        'key.g': ctx.group_id
      },
      {
        projection: {
          lookup: 1
        },
        limit: limit + 1,
        batchSize: limit + 2,
        singleBatch: true
      }
    )
    .toArray();
  const invalidateParameterUpdates = parameterUpdates.length > limit;

  return {
    invalidateParameterBuckets: invalidateParameterUpdates,
    updatedParameterLookups: invalidateParameterUpdates
      ? new Set<string>()
      : new Set<string>(parameterUpdates.map((p) => JSONBig.stringify(deserializeParameterLookup(p.lookup))))
  };
}
