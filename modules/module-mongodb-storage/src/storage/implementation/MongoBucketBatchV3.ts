import * as bson from 'bson';
import { EvaluatedParameters, EvaluatedRow } from '@powersync/service-sync-rules';
import { storage } from '@powersync/service-core';

import { MongoBucketBatch, MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { CurrentDataStore } from './CurrentDataStore.js';
import { CurrentDataStoreV3 } from './CurrentDataStoreV3.js';
import { PersistedBatch } from './PersistedBatch.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';
import { CommonCurrentBucket, CommonCurrentLookup, CurrentBucketV3, RecordedLookupV3 } from './models.js';

export class MongoBucketBatchV3 extends MongoBucketBatch {
  private readonly store: CurrentDataStore;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.store = new CurrentDataStoreV3(this.db, this.group_id);
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

  protected get currentDataStore(): CurrentDataStore {
    return this.store;
  }
}
