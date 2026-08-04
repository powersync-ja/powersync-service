import {
  BucketDataBatchOptions,
  BucketDataRequest,
  InternalOpId,
  isBatchEnd,
  OplogEntry,
  ParsedSyncConfigSet,
  PersistedSyncConfigContent,
  ReplicationCheckpoint,
  SyncBucketDataChunk,
  SyncRulesBucketStorage
} from '@powersync/service-core';
import { bucketRequest, getBatchArray } from './general-utils.js';
import { fromAsync } from './stream_utils.js';

export class StorageDataHelpers {
  storage: SyncRulesBucketStorage;
  syncRules: PersistedSyncConfigContent | ParsedSyncConfigSet;

  constructor(storage: SyncRulesBucketStorage, syncRules: PersistedSyncConfigContent | ParsedSyncConfigSet) {
    this.storage = storage;
    this.syncRules = syncRules;
  }

  async getBucketData(bucket: string, checkpoint: ReplicationCheckpoint, start?: InternalOpId | string | undefined) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    let map = [bucketRequest(this.syncRules, bucket, start)];
    let data: OplogEntry[] = [];
    while (true) {
      const batch = this.storage!.getBucketDataBatch(checkpoint, map);

      const batches = await getBatchArray(batch);
      data = data.concat(batches[0]?.chunkData.data ?? []);
      if (batches.length == 0 || !batches[0]!.chunkData.has_more) {
        break;
      }
      map = [bucketRequest(this.syncRules, bucket, BigInt(batches[0]!.chunkData.next_after))];
    }
    return data;
  }

  async getBucketsDataBatch(buckets: Record<string, InternalOpId>, checkpoint: ReplicationCheckpoint) {
    const map = Object.entries(buckets).map(([bucket, start]) => bucketRequest(this.syncRules, bucket, start));
    return fromAsync(this.storage!.getBucketDataBatch(checkpoint, map));
  }

  async getAllBucketData(
    requests: BucketDataRequest[],
    checkpoint: ReplicationCheckpoint,
    options?: BucketDataBatchOptions
  ): Promise<SyncBucketDataChunk[]> {
    let remainingBuckets = new Map(requests.map((r) => [r.bucket, r]));
    let chunks: SyncBucketDataChunk[] = [];
    while (true) {
      let hasMore = false;
      for await (let chunk of this.storage!.getBucketDataBatch(checkpoint, [...remainingBuckets.values()], options)) {
        if (isBatchEnd(chunk)) {
          if (chunk.hasMore) {
            hasMore = true;
            break;
          } else {
            return chunks;
          }
        } else {
          chunks.push(chunk);
          if (chunk.chunkData.has_more) {
            hasMore = true;
            const r = remainingBuckets.get(chunk.chunkData.bucket)!;
            remainingBuckets.set(chunk.chunkData.bucket, {
              start: BigInt(chunk.chunkData.next_after),
              bucket: chunk.chunkData.bucket,
              source: r.source
            });
          } else {
            remainingBuckets.delete(chunk.chunkData.bucket);
          }
        }
      }
      if (!hasMore) {
        break;
      }
    }
    return chunks;
  }
}
