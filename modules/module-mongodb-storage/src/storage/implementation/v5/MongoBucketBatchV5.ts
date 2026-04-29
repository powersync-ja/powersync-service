import * as lib_mongo from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { mongoTableId } from '../../../utils/util.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV5 } from './PersistedBatchV5.js';
import { SourceRecordStoreV5 } from './SourceRecordStoreV5.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoBucketBatchV5 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV5;

  private readonly store: SourceRecordStore;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.store = new SourceRecordStoreV5(this.db, this.group_id, this.mapping);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV5(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }

  protected async cleanupDroppedSourceTables(sourceTables: storage.SourceTable[]) {
    for (const table of sourceTables) {
      await this.db
        .sourceRecordsV5(this.group_id, mongoTableId(table.id))
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
