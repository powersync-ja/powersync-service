import { mongo } from '@powersync/lib-service-mongodb';
import { BucketDefinitionId } from '../BucketDefinitionMapping.js';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import type { VersionedPowerSyncMongo } from '../db.js';
import { BucketDocumentFormatAdapter } from '../document-formats/bucket-document-format.js';
export async function flushBucketDataShared(
  options: {
    db: VersionedPowerSyncMongo;
    groupId: number;
    bucketData: BucketDataDoc[];
    formatAdapter: BucketDocumentFormatAdapter;
    getCollection: (groupId: number, definitionId: BucketDefinitionId) => mongo.Collection<any>;
  },
  session: mongo.ClientSession
): Promise<void> {
  const operationsByDefinition = new Map<BucketDefinitionId, BucketDataDoc[]>();
  for (const document of options.bucketData) {
    const existing = operationsByDefinition.get(document.bucketKey.definitionId) ?? [];
    existing.push(document);
    operationsByDefinition.set(document.bucketKey.definitionId, existing);
  }

  for (const [definitionId, documents] of operationsByDefinition.entries()) {
    const operationsByBucket = new Map<string, BucketDataDoc[]>();
    for (const document of documents) {
      const existing = operationsByBucket.get(document.bucketKey.bucket) ?? [];
      existing.push(document);
      operationsByBucket.set(document.bucketKey.bucket, existing);
    }

    const inserts: mongo.AnyBulkWriteOperation<any>[] = [];
    for (const [bucket, ops] of operationsByBucket.entries()) {
      const serialized = options.formatAdapter.serializeForBulkWrite(bucket, ops);
      inserts.push(...serialized);
    }

    if (inserts.length > 0) {
      await options.getCollection(options.groupId, definitionId).bulkWrite(inserts, {
        session,
        ordered: false
      });
    }
  }
}
