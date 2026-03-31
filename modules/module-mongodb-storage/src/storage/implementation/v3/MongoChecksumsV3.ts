import { mongo } from '@powersync/lib-service-mongodb';
import {
  BucketChecksum,
  FetchPartialBucketChecksum,
  InternalOpId,
  PartialChecksumMap,
  PartialOrFullChecksum
} from '@powersync/service-core';
import { BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import {
  AbstractMongoChecksums,
  createV3BucketFilter,
  emptyChecksumForRequest,
  FetchPartialBucketChecksumV3,
  MongoChecksumOptions
} from '../common/MongoChecksumsBase.js';
import { VersionedPowerSyncMongo } from '../db.js';

export class MongoChecksumsV3Impl extends AbstractMongoChecksums {
  constructor(
    db: VersionedPowerSyncMongo,
    group_id: number,
    options: MongoChecksumOptions,
    private readonly mapping: BucketDefinitionMapping
  ) {
    super(db, group_id, options);
  }

  private normalizeBatch(batch: FetchPartialBucketChecksum[]): FetchPartialBucketChecksumV3[] {
    return batch.map((request) => ({
      bucket: request.bucket,
      definitionId: this.mapping.bucketSourceId(request.source),
      start: request.start,
      end: request.end
    }));
  }

  async computePartialChecksumsDirectByDefinition(batch: FetchPartialBucketChecksumV3[]): Promise<PartialChecksumMap> {
    const results = new Map<string, PartialOrFullChecksum>();
    const requestsByDefinition = new Map<string, FetchPartialBucketChecksumV3[]>();

    for (const request of batch) {
      const existing = requestsByDefinition.get(request.definitionId) ?? [];
      existing.push(request);
      requestsByDefinition.set(request.definitionId, existing);
    }

    for (const [definitionId, requests] of requestsByDefinition.entries()) {
      const groupResults = await this.computePartialChecksumsForCollection(
        requests,
        this.db.bucket_data_v3(this.group_id, definitionId) as unknown as mongo.Collection<mongo.Document>,
        createV3BucketFilter
      );
      for (const checksum of groupResults.values()) {
        results.set(checksum.bucket, checksum);
      }
    }

    return new Map<string, PartialOrFullChecksum>(
      batch.map((request) => [request.bucket, results.get(request.bucket) ?? emptyChecksumForRequest(request)])
    );
  }

  protected async fetchPreStates(
    batch: FetchPartialBucketChecksum[]
  ): Promise<Map<string, { opId: InternalOpId; checksum: BucketChecksum }>> {
    const preFilters = this.normalizeBatch(batch)
      .filter((request) => request.start == null)
      .map((request) => ({
        _id: {
          d: request.definitionId,
          b: request.bucket
        },
        'compacted_state.op_id': { $exists: true, $lte: request.end }
      }));

    const preStates = new Map<string, { opId: InternalOpId; checksum: BucketChecksum }>();
    if (preFilters.length == 0) {
      return preStates;
    }

    const states = await this.db
      .bucketStateV3(this.group_id)
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
    return this.computePartialChecksumsDirectByDefinition(this.normalizeBatch(batch));
  }
}
