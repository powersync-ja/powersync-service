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
import { ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { JSONBig } from '@powersync/service-jsonbig';
import { mapOpEntry, readSingleBatch, setSessionSnapshotTime } from '../../../utils/util.js';
import { BucketDataDocumentV1, LEGACY_BUCKET_DATA_DEFINITION_ID, bucketDataDocumentToTagged } from '../models.js';
import {
  MongoSyncBucketStorageCheckpoint,
  MongoSyncBucketStorageContext
} from '../common/MongoSyncBucketStorageContext.js';

export async function getParameterSetsV1(
  ctx: MongoSyncBucketStorageContext,
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
  ctx: MongoSyncBucketStorageContext,
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
    const row = bucketDataDocumentToTagged(
      bson.deserialize(rawData, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS) as BucketDataDocumentV1,
      LEGACY_BUCKET_DATA_DEFINITION_ID
    );
    const bucket = row._id.b;

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
  ctx: MongoSyncBucketStorageContext,
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
  ctx: MongoSyncBucketStorageContext,
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
