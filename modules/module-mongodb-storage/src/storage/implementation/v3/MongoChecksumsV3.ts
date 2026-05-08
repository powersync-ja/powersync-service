import {
  BucketChecksum,
  FetchPartialBucketChecksum,
  InternalOpId,
  PartialChecksumMap,
  PartialOrFullChecksum
} from '@powersync/service-core';
import { BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import {
  emptyChecksumForRequest,
  FetchPartialBucketChecksumByDefinition,
  MongoChecksumOptions,
  MongoChecksums
} from '../MongoChecksums.js';
import {
  computePartialChecksumsInternal,
  fetchPreStates,
  normalizeBatch
} from '../bucket-operations/checksum-aggregation.js';
import { createBucketFilter } from '../bucket-operations/query-builders.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class MongoChecksumsV3 extends MongoChecksums {
  get db(): VersionedPowerSyncMongoV3 {
    return super.db as VersionedPowerSyncMongoV3;
  }

  private readonly mapping: BucketDefinitionMapping;

  constructor(db: VersionedPowerSyncMongoV3, group_id: number, options: MongoChecksumOptions) {
    super(db, group_id, options);
    this.mapping = options.mapping!;
  }

  async computePartialChecksumsDirectByDefinition(
    batch: FetchPartialBucketChecksumByDefinition[]
  ): Promise<PartialChecksumMap> {
    const results = new Map<string, PartialOrFullChecksum>();
    const requestsByDefinition = new Map<string, FetchPartialBucketChecksumByDefinition[]>();

    for (const request of batch) {
      const existing = requestsByDefinition.get(request.definitionId) ?? [];
      existing.push(request);
      requestsByDefinition.set(request.definitionId, existing);
    }

    for (const [definitionId, requests] of requestsByDefinition.entries()) {
      const groupResults = await this.computePartialChecksumsForCollection(
        requests,
        this.db.bucketDataV3(this.group_id, definitionId),
        createBucketFilter
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
    return fetchPreStates(normalizeBatch(batch, this.mapping), this.db.bucketStateV3(this.group_id));
  }

  protected async computePartialChecksumsInternal(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    return computePartialChecksumsInternal(
      batch,
      this.mapping,
      (definitionId) => this.db.bucketDataV3(this.group_id, definitionId),
      (batch, collection, createFilter) => this.computePartialChecksumsForCollection(batch, collection, createFilter)
    );
  }
}
