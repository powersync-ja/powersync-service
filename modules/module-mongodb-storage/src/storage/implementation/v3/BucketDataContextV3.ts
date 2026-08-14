import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { BucketKey } from '../common/BucketDataDoc.js';
import { BucketDataKey } from '../models.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';
import { BucketDataDocumentV3 } from './models.js';

export class BucketDataContextV3 {
  public readonly collection: mongo.Collection<BucketDataDocumentV3>;

  constructor(
    db: VersionedPowerSyncMongoV3,
    public readonly key: BucketKey
  ) {
    this.collection = db.bucketData(key.replicationStreamId, key.definitionId);
  }

  docId(o: InternalOpId): BucketDataKey {
    return {
      b: this.key.bucket,
      o
    };
  }

  get minId(): BucketDataKey {
    // MongoDB's MinKey sentinel does not match the bigint type for _id.o.
    return {
      b: this.key.bucket,
      o: new mongo.MinKey()
    } as unknown as BucketDataKey;
  }
}
