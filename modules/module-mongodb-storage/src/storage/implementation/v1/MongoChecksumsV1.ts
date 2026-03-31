import { mongo } from '@powersync/lib-service-mongodb';
import {
  bson,
  BucketChecksum,
  FetchPartialBucketChecksum,
  InternalOpId,
  PartialChecksumMap
} from '@powersync/service-core';
import { AbstractMongoChecksums, FetchPartialBucketChecksumByBucket } from '../common/MongoChecksumsBase.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';
import { BucketDataDocumentBase } from '../models.js';

export class MongoChecksumsV1Impl extends AbstractMongoChecksums {
  declare protected readonly db: VersionedPowerSyncMongoV1;

  async computePartialChecksumsDirectByBucket(
    batch: FetchPartialBucketChecksumByBucket[]
  ): Promise<PartialChecksumMap> {
    return this.computePartialChecksumsForCollection(batch, this.db.v1_bucket_data, (request) => ({
      _id: {
        $gt: {
          g: this.group_id,
          b: request.bucket,
          o: request.start ?? new bson.MinKey()
        },
        $lte: {
          g: this.group_id,
          b: request.bucket,
          o: request.end
        }
      }
    }));
  }

  protected async fetchPreStates(
    batch: FetchPartialBucketChecksum[]
  ): Promise<Map<string, { opId: InternalOpId; checksum: BucketChecksum }>> {
    const preFilters = batch
      .filter((request) => request.start == null)
      .map((request) => ({
        _id: {
          g: this.group_id,
          b: request.bucket
        },
        'compacted_state.op_id': { $exists: true, $lte: request.end }
      }));

    const preStates = new Map<string, { opId: InternalOpId; checksum: BucketChecksum }>();
    if (preFilters.length == 0) {
      return preStates;
    }

    const states = await this.db.bucketStateV1
      .find({
        $or: preFilters
      })
      .toArray();

    for (const state of states) {
      const compactedState = state.compacted_state!;
      preStates.set(state._id.b, {
        opId: compactedState.op_id,
        checksum: {
          bucket: state._id.b,
          checksum: Number(compactedState.checksum),
          count: compactedState.count
        }
      });
    }

    return preStates;
  }

  protected async computePartialChecksumsInternal(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    return this.computePartialChecksumsDirectByBucket(batch);
  }
}
