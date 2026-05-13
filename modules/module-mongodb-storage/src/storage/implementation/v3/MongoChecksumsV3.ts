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
import { VersionedPowerSyncMongo } from '../collection-access/versioned-collections.js';
import { BucketDataDocumentBase } from '../models.js';

export class MongoChecksumsV3 extends MongoChecksums {
  get db(): VersionedPowerSyncMongo {
    return super.db as VersionedPowerSyncMongo;
  }

  private readonly mapping: BucketDefinitionMapping;

  constructor(db: VersionedPowerSyncMongo, group_id: number, options: MongoChecksumOptions) {
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
        this.db.bucketData<BucketDataDocumentBase>(this.group_id, definitionId),
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
    return fetchPreStates(normalizeBatch(batch, this.mapping), this.db.bucketState(this.group_id));
  }

  protected async computePartialChecksumsInternal(batch: FetchPartialBucketChecksum[]): Promise<PartialChecksumMap> {
    return computePartialChecksumsInternal(
      batch,
      this.mapping,
      (definitionId) => this.db.bucketData<BucketDataDocumentBase>(this.group_id, definitionId),
      (batch, collection, createFilter) => this.computePartialChecksumsForCollection(batch, collection, createFilter)
    );
  }
}
