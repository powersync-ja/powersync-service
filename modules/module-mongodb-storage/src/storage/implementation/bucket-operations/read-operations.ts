import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId, storage } from '@powersync/service-core';
import * as bson from 'bson';
import { readSingleBatch } from '../../../utils/util.js';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import { BucketDataDocumentGeneric } from '../common/SingleBucketStore.js';
import { BucketDataFormatAdapter } from '../document-formats/format-interface.js';

export interface BucketDataBatchResult {
  data: BucketDataDoc[];
  hasMore: boolean;
  chunkSizeBytes: number;
  documentOpCounts: number[];
  documentSizes: number[];
}

export async function getBucketDataBatchShared(options: {
  collection: mongo.Collection<BucketDataDocumentGeneric>;
  formatAdapter: BucketDataFormatAdapter;
  filter: mongo.Filter<BucketDataDocumentGeneric>;
  context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>;
  bucketMap: Map<string, InternalOpId>;
  startOpId: InternalOpId;
  endOpId: InternalOpId;
  limit: number;
}): Promise<BucketDataBatchResult> {
  const { collection, formatAdapter, filter, context, bucketMap, startOpId, endOpId, limit } = options;

  const { filter: rangeFilter, cursorOptions } = formatAdapter.buildBucketDataQuery({
    startOpId,
    endOpId,
    remainingLimit: limit
  });

  const combinedFilter: mongo.Filter<BucketDataDocumentGeneric> = {
    $and: [filter, rangeFilter]
  } as any;

  const cursor = collection.find(combinedFilter, {
    session: undefined,
    sort: { _id: 1 },
    raw: true,
    maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS,
    ...cursorOptions
  }) as unknown as mongo.FindCursor<Buffer>;

  let { data, hasMore } = await readSingleBatch(cursor).catch((e: any) => {
    throw lib_mongo.mapQueryError(e, 'while reading bucket data');
  });

  if (cursorOptions.limit != null && data.length >= cursorOptions.limit) {
    hasMore = true;
  }

  const result: BucketDataDoc[] = [];
  const documentOpCounts: number[] = [];
  const documentSizes: number[] = [];
  let chunkSizeBytes = 0;
  let remainingLimit = limit;
  let limitReached = false;

  for (const rawData of data) {
    const doc = bson.deserialize(rawData, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS);
    let opCount = 0;
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

      result.push(row);
      opCount++;
      remainingLimit--;
      if (remainingLimit <= 0) {
        limitReached = true;
        break;
      }
    }
    documentOpCounts.push(opCount);
    documentSizes.push(rawData.byteLength);
    chunkSizeBytes += rawData.byteLength;
    if (limitReached) {
      break;
    }
  }

  return {
    data: result,
    hasMore: hasMore || limitReached,
    chunkSizeBytes,
    documentOpCounts,
    documentSizes
  };
}
