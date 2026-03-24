import * as bson from 'bson';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';

import { MongoBucketBatch, MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { cacheKey } from './OperationBatch.js';
import { PersistedBatch } from './PersistedBatch.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';
import {
  CommonCurrentBucket,
  CommonCurrentLookup,
  CurrentDataDocumentId,
  CurrentBucketV3,
  CurrentDataDocumentV3,
  RecordedLookupV3,
  isCurrentBucketV3,
  isRecordedLookupV3
} from './models.js';

export class MongoBucketBatchV3 extends MongoBucketBatch {
  constructor(options: MongoBucketBatchOptions) {
    super(options);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected mapEvaluatedBuckets(evaluated: EvaluatedRow[]): CommonCurrentBucket[] {
    return evaluated.map((entry) => {
      const def = this.mapping.bucketSourceId(entry.source);
      return {
        def,
        bucket: entry.bucket,
        table: entry.table,
        id: entry.id
      } satisfies CurrentBucketV3;
    });
  }

  protected mapParameterLookups(paramEvaluated: EvaluatedParameters[]): CommonCurrentLookup[] {
    return paramEvaluated.map((entry) => {
      const def = this.mapping.parameterLookupId(entry.lookup.source);
      return { i: def, l: storage.serializeLookup(entry.lookup) } satisfies RecordedLookupV3;
    });
  }

  protected createCurrentDataId(_sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): CurrentDataDocumentId {
    return replicaId;
  }

  protected createCurrentDataDocument(
    id: CurrentDataDocumentId,
    data: bson.Binary,
    buckets: CommonCurrentBucket[],
    lookups: CommonCurrentLookup[]
  ): CurrentDataDocumentV3 {
    const narrowedBuckets = buckets.map((bucket) => {
      if (!isCurrentBucketV3(bucket)) {
        throw new ReplicationAssertionError('Expected v3 bucket when incrementalReprocessing is enabled');
      }
      return bucket;
    });
    const narrowedLookups = lookups.map((lookup) => {
      if (!isRecordedLookupV3(lookup)) {
        throw new ReplicationAssertionError('Expected v3 lookup when incrementalReprocessing is enabled');
      }
      return lookup;
    });

    return {
      _id: id,
      data,
      buckets: narrowedBuckets,
      lookups: narrowedLookups
    };
  }

  protected createCurrentDataLookupFilter(_sourceTableId: bson.ObjectId, replicaIds: storage.ReplicaId[]) {
    return {
      _id: { $in: replicaIds }
    };
  }

  protected currentDataCacheKey(sourceTableId: bson.ObjectId, document: CurrentDataDocumentV3): string {
    return cacheKey(sourceTableId, document._id);
  }

  protected currentDataReplicaId(document: CurrentDataDocumentV3): storage.ReplicaId {
    return document._id;
  }

  protected activeCurrentDataFilter(_sourceTableId: bson.ObjectId) {
    return {
      pending_delete: { $exists: false }
    };
  }

  protected async cleanupCurrentData(lastCheckpoint: bigint): Promise<void> {
    let deletedCount = 0;
    for (const collection of await this.db.listCommonCurrentDataCollections(this.group_id)) {
      const result = await collection.deleteMany({
        pending_delete: { $exists: true, $lte: lastCheckpoint }
      });
      deletedCount += result.deletedCount;
    }
    if (deletedCount > 0) {
      this.logger.info(
        `Cleaned up ${deletedCount} pending delete current_data records for checkpoint ${lastCheckpoint}`
      );
    }
  }
}
