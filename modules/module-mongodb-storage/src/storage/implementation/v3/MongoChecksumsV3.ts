import {
  bson,
  BucketChecksum,
  FetchPartialBucketChecksum,
  InternalOpId,
  PartialChecksumMap,
  PartialOrFullChecksum
} from '@powersync/service-core';
import { SingleSyncConfigBucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import {
  emptyChecksumForRequest,
  FetchPartialBucketChecksumV3,
  MongoChecksumOptions,
  MongoChecksums
} from '../MongoChecksums.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

/**
 * Checksum operations addressed by persisted definition id.
 *
 * Safe on any storage instance, including multi-config replication-side instances - no
 * source resolution is involved. This is the only checksum surface the compactor may use.
 */
export interface DefinitionChecksumOperations {
  computePartialChecksumsDirectByDefinition(batch: FetchPartialBucketChecksumV3[]): Promise<PartialChecksumMap>;
}

export interface MongoChecksumOptionsV3 extends MongoChecksumOptions {
  /**
   * The persisted mapping of the single sync config that reads are served from.
   *
   * A thunk rather than a plain value: checksums are constructed eagerly with the storage
   * instance, but the single-config requirement may only be asserted when a read happens.
   */
  syncConfigMapping: () => SingleSyncConfigBucketDefinitionMapping;
}

export class MongoChecksumsV3 extends MongoChecksums implements DefinitionChecksumOperations {
  declare protected readonly db: VersionedPowerSyncMongoV3;
  private readonly syncConfigMapping: () => SingleSyncConfigBucketDefinitionMapping;

  constructor(db: VersionedPowerSyncMongoV3, group_id: number, options: MongoChecksumOptionsV3) {
    super(db, group_id, options);
    this.syncConfigMapping = options.syncConfigMapping;
  }

  private normalizeBatch(batch: FetchPartialBucketChecksum[]): FetchPartialBucketChecksumV3[] {
    const mapping = this.syncConfigMapping();
    return batch.map((request) => ({
      bucket: request.bucket,
      definitionId: mapping.bucketSourceId(request.source),
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
        this.db.bucketDataV3(this.group_id, definitionId),
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

function createV3BucketFilter(request: Pick<FetchPartialBucketChecksumV3, 'bucket' | 'start' | 'end'>) {
  return {
    _id: {
      $gt: {
        b: request.bucket,
        o: request.start ?? new bson.MinKey()
      },
      $lte: {
        b: request.bucket,
        o: request.end
      }
    }
  };
}
