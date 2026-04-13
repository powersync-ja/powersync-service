import {
  InternalOpId,
  OplogEntry,
  PersistedSyncRules,
  PersistedSyncRulesContent,
  SyncRulesBucketStorage
} from '@powersync/service-core';
import { bucketRequest } from './general-utils.js';
import { fromAsync } from './stream_utils.js';

export class StorageDataHelpers {
  storage: SyncRulesBucketStorage;
  syncRules: PersistedSyncRulesContent | PersistedSyncRules;

  constructor(storage: SyncRulesBucketStorage, syncRules: PersistedSyncRulesContent | PersistedSyncRules) {
    this.storage = storage;
    this.syncRules = syncRules;
  }

  async getBucketData(bucket: string, checkpoint: InternalOpId, start?: InternalOpId | string | undefined) {
    start ??= 0n;
    if (typeof start == 'string') {
      start = BigInt(start);
    }
    let map = [bucketRequest(this.syncRules, bucket, start)];
    let data: OplogEntry[] = [];
    while (true) {
      const batch = this.storage!.getBucketDataBatch(checkpoint, map);

      const batches = await fromAsync(batch);
      data = data.concat(batches[0]?.chunkData.data ?? []);
      if (batches.length == 0 || !batches[0]!.chunkData.has_more) {
        break;
      }
      map = [bucketRequest(this.syncRules, bucket, BigInt(batches[0]!.chunkData.next_after))];
    }
    return data;
  }

  async getBucketsDataBatch(buckets: Record<string, InternalOpId>, checkpoint: InternalOpId) {
    const map = Object.entries(buckets).map(([bucket, start]) => bucketRequest(this.syncRules, bucket, start));
    return fromAsync(this.storage!.getBucketDataBatch(checkpoint, map));
  }
}
