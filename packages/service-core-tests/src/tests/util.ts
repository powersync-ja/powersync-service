import { storage } from '@powersync/service-core';

export function bucketRequest(syncRules: storage.PersistedSyncRulesContent, bucketName: string): string {
  if (/^\d+#/.test(bucketName)) {
    return bucketName;
  }

  const versionedBuckets = storage.STORAGE_VERSION_CONFIG[syncRules.storageVersion]?.versionedBuckets ?? false;
  return versionedBuckets ? `${syncRules.id}#${bucketName}` : bucketName;
}

export function bucketRequests(syncRules: storage.PersistedSyncRulesContent, bucketNames: string[]): string[] {
  return bucketNames.map((bucketName) => bucketRequest(syncRules, bucketName));
}

export function bucketRequestMap(
  syncRules: storage.PersistedSyncRulesContent,
  buckets: Iterable<readonly [string, bigint]>
): Map<string, bigint> {
  return new Map(Array.from(buckets, ([bucketName, opId]) => [bucketRequest(syncRules, bucketName), opId]));
}
