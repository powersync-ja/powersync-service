import { mongo } from '@powersync/lib-service-mongodb';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';

import { MongoBucketBatch, MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { CurrentDataStore } from './CurrentDataStore.js';
import { CurrentDataStoreV1 } from './CurrentDataStoreV1.js';
import { PersistedBatch } from './PersistedBatch.js';
import { PersistedBatchV1 } from './PersistedBatchV1.js';
import { CommonCurrentBucket, CommonCurrentLookup, isCurrentBucketV3, isRecordedLookupV3 } from './models.js';

export class MongoBucketBatchV1 extends MongoBucketBatch {
  private readonly store: CurrentDataStore;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.store = new CurrentDataStoreV1(this.db, this.group_id);
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

  protected get currentDataStore(): CurrentDataStore {
    return this.store;
  }
}
