import * as lib_mongo from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../common/MongoBucketBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { SourceRecordStoreV3 } from './SourceRecordStoreV3.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';
import { mongoTableId } from '../../../utils/util.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export class MongoBucketBatchV3 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV3;

  private readonly store: SourceRecordStore;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.store = new SourceRecordStoreV3(this.db, this.group_id, this.mapping);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }

  protected async cleanupDroppedSourceTables(sourceTables: storage.SourceTable[]) {
    for (const table of sourceTables) {
      await this.db
        .sourceRecordsV3(this.group_id, mongoTableId(table.id))
        .drop()
        .catch((error) => {
          if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
            return;
          }
          throw error;
        });
    }
  }
}
