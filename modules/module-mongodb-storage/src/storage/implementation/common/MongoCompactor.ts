import { PopulateChecksumCacheResults } from '@powersync/service-core';
import type { VersionedPowerSyncMongo } from '../db.js';
import type { MongoSyncBucketStorage } from '../MongoSyncBucketStorage.js';
import { MongoCompactorV1 } from '../v1/MongoCompactorV1.js';
import { MongoCompactorV3 } from '../v3/MongoCompactorV3.js';
import { BaseMongoCompactor, DirtyBucket, MongoCompactOptions } from './MongoCompactorBase.js';

export { DirtyBucket, MongoCompactOptions } from './MongoCompactorBase.js';

export class MongoCompactor {
  private readonly impl: BaseMongoCompactor;

  constructor(storage: MongoSyncBucketStorage, db: VersionedPowerSyncMongo, options: MongoCompactOptions) {
    if (db.storageConfig.incrementalReprocessing) {
      this.impl = new MongoCompactorV3(storage, db, options);
    } else {
      this.impl = new MongoCompactorV1(storage, db, options);
    }
  }

  async compact() {
    return this.impl.compact();
  }

  async populateChecksums(options: { minBucketChanges: number }): Promise<PopulateChecksumCacheResults> {
    return this.impl.populateChecksums(options);
  }

  dirtyBucketBatches(options: { minBucketChanges: number; minChangeRatio: number }): AsyncGenerator<DirtyBucket[]> {
    return this.impl.dirtyBucketBatches(options);
  }

  dirtyBucketBatchForChecksums(options: { minBucketChanges: number }): Promise<DirtyBucket[]> {
    return this.impl.dirtyBucketBatchForChecksums(options);
  }
}
