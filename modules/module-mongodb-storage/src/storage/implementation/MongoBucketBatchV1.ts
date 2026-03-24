import { mongo } from '@powersync/lib-service-mongodb';
import * as bson from 'bson';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';

import { idPrefixFilter } from '../../utils/util.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { cacheKey } from './OperationBatch.js';
import { PersistedBatch } from './PersistedBatch.js';
import { PersistedBatchV1 } from './PersistedBatchV1.js';
import {
  CommonCurrentBucket,
  CommonCurrentLookup,
  CurrentDataDocumentId,
  CurrentDataDocument,
  SourceKey,
  isCurrentBucketV3,
  isRecordedLookupV3
} from './models.js';

export class MongoBucketBatchV1 extends MongoBucketBatch {
  constructor(options: MongoBucketBatchOptions) {
    super(options);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV1(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected mapEvaluatedBuckets(evaluated: EvaluatedRow[]): CommonCurrentBucket[] {
    return evaluated.map((entry) => ({
      bucket: entry.bucket,
      table: entry.table,
      id: entry.id
    }));
  }

  protected mapParameterLookups(paramEvaluated: EvaluatedParameters[]): CommonCurrentLookup[] {
    return paramEvaluated.map((entry) => storage.serializeLookup(entry.lookup));
  }

  protected createCurrentDataId(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): CurrentDataDocumentId {
    return { g: this.group_id, t: sourceTableId, k: replicaId } satisfies SourceKey;
  }

  protected createCurrentDataDocument(
    id: CurrentDataDocumentId,
    data: bson.Binary,
    buckets: CommonCurrentBucket[],
    lookups: CommonCurrentLookup[]
  ): CurrentDataDocument {
    const narrowedBuckets = buckets.map((bucket) => {
      if (isCurrentBucketV3(bucket)) {
        throw new ReplicationAssertionError('Unexpected v3 bucket when incrementalReprocessing is disabled');
      }
      return bucket;
    });
    const narrowedLookups = lookups.map((lookup) => {
      if (isRecordedLookupV3(lookup)) {
        throw new ReplicationAssertionError('Unexpected v3 lookup when incrementalReprocessing is disabled');
      }
      return lookup;
    });

    return {
      _id: id as SourceKey,
      data,
      buckets: narrowedBuckets,
      lookups: narrowedLookups
    };
  }

  protected createCurrentDataLookupFilter(sourceTableId: bson.ObjectId, replicaIds: storage.ReplicaId[]) {
    return {
      _id: {
        $in: replicaIds.map((replicaId) => this.createCurrentDataId(sourceTableId, replicaId) as SourceKey)
      }
    };
  }

  protected currentDataCacheKey(sourceTableId: bson.ObjectId, document: CurrentDataDocument): string {
    return cacheKey(sourceTableId, document._id.k);
  }

  protected currentDataReplicaId(document: CurrentDataDocument): storage.ReplicaId {
    return document._id.k;
  }

  protected activeCurrentDataFilter(
    sourceTableId: bson.ObjectId
  ): mongo.Filter<import('./models.js').CommonCurrentDataDocument> {
    return {
      _id: idPrefixFilter<SourceKey>({ g: this.group_id, t: sourceTableId }, ['k']),
      pending_delete: { $exists: false }
    } as mongo.Filter<import('./models.js').CommonCurrentDataDocument>;
  }

  protected async cleanupCurrentData(_lastCheckpoint: bigint): Promise<void> {}
}
