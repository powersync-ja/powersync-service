import {
  BucketChecksumRequest,
  ChecksumMap,
  FetchPartialBucketChecksum,
  InternalOpId,
  PartialChecksumMap
} from '@powersync/service-core';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import type { VersionedPowerSyncMongo } from '../db.js';
import { MongoChecksumsV1Impl } from '../v1/MongoChecksumsV1.js';
import { MongoChecksumsV3Impl } from '../v3/MongoChecksumsV3.js';
import type { VersionedPowerSyncMongoV3 } from '../v3/VersionedPowerSyncMongoV3.js';
import {
  AbstractMongoChecksums,
  FetchPartialBucketChecksumByBucket,
  FetchPartialBucketChecksumV3,
  MongoChecksumOptions
} from './MongoChecksumsBase.js';

export {
  FetchPartialBucketChecksumByBucket,
  FetchPartialBucketChecksumV3,
  MongoChecksumOptions
} from './MongoChecksumsBase.js';

/**
 * Public checksum API. Delegates to a storage-version-specific implementation.
 */
export class MongoChecksums {
  private readonly impl: AbstractMongoChecksums;
  private readonly v3Impl: MongoChecksumsV3Impl | null;
  private readonly v1Impl: MongoChecksumsV1Impl | null;

  constructor(db: VersionedPowerSyncMongo, group_id: number, options: MongoChecksumOptions) {
    if (options.storageConfig.incrementalReprocessing) {
      this.v3Impl = new MongoChecksumsV3Impl(
        db as VersionedPowerSyncMongoV3,
        group_id,
        options,
        options.mapping ??
          (() => {
            throw new ServiceAssertionError('BucketDefinitionMapping is required for v3 MongoDB checksum queries');
          })()
      );
      this.v1Impl = null;
      this.impl = this.v3Impl;
    } else {
      this.v3Impl = null;
      this.v1Impl = new MongoChecksumsV1Impl(db, group_id, options);
      this.impl = this.v1Impl;
    }
  }

  async getChecksums(checkpoint: InternalOpId, buckets: BucketChecksumRequest[]): Promise<ChecksumMap> {
    return this.impl.getChecksums(checkpoint, buckets);
  }

  clearCache() {
    this.impl.clearCache();
  }

  async computePartialChecksumsDirect(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    return this.impl.computePartialChecksumsDirect(batch);
  }

  async computePartialChecksumsDirectV1(batch: FetchPartialBucketChecksumByBucket[]): Promise<PartialChecksumMap> {
    if (this.v1Impl == null) {
      throw new ServiceAssertionError('V1 checksum routing is only available when incrementalReprocessing is disabled');
    }
    return this.v1Impl.computePartialChecksumsDirectByBucket(batch);
  }

  async computePartialChecksumsDirectV3(batch: FetchPartialBucketChecksumV3[]): Promise<PartialChecksumMap> {
    if (this.v3Impl == null) {
      throw new ServiceAssertionError('V3 checksum routing is only available when incrementalReprocessing is enabled');
    }
    return this.v3Impl.computePartialChecksumsDirectByDefinition(batch);
  }
}
