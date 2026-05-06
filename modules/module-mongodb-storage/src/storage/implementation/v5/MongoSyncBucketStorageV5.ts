import * as lib_mongo from '@powersync/lib-service-mongodb';
import { InternalOpId, storage } from '@powersync/service-core';
import * as bson from 'bson';
import { MongoBucketStorage } from '../../MongoBucketStorage.js';
import {
  getBucketDataBatchSharedWrapper,
  getDataBucketChangesShared,
  getParameterBucketChangesShared,
  getParameterSetsShared
} from '../bucket-operations/storage-operations.js';
import {
  MongoSyncBucketStorageCheckpoint,
  MongoSyncBucketStorageContext
} from '../common/MongoSyncBucketStorageContext.js';
import { V5FormatAdapter } from '../document-formats/v5-format.js';
import { CommonSourceTableDocument } from '../models.js';
import { MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { MongoChecksums } from '../MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from '../MongoCompactor.js';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { MongoPersistedSyncRulesContent } from '../MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageOptions } from '../MongoSyncBucketStorage.js';
import { MongoBucketBatchV5 } from './MongoBucketBatchV5.js';
import { MongoChecksumsV5 } from './MongoChecksumsV5.js';
import { MongoCompactorV5 } from './MongoCompactorV5.js';
import { MongoParameterCompactorV5 } from './MongoParameterCompactorV5.js';
import { deserializeParameterLookupV5, serializeParameterLookupV5 } from './MongoParameterLookupV5.js';
import { VersionedPowerSyncMongoV5 } from './VersionedPowerSyncMongoV5.js';

export class MongoSyncBucketStorageV5 extends MongoSyncBucketStorage {
  declare readonly db: VersionedPowerSyncMongoV5;
  declare readonly checksums: MongoChecksumsV5;

  constructor(
    factory: MongoBucketStorage,
    group_id: number,
    sync_rules: MongoPersistedSyncRulesContent,
    slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    super(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
  }

  protected async initializeVersionStorage(): Promise<void> {
    const mapping = this.mapping;
    for (let source of mapping.allBucketDefinitionIds()) {
      const collection = this.db.bucketDataV5(this.group_id, source).collectionName;
      await this.db.db
        .createCollection(collection, { clusteredIndex: { name: '_id', unique: true, key: { _id: 1 } } })
        .catch((error) => {
          if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceExists') {
            return;
          }
          throw error;
        });
    }
    for (let indexId of mapping.allParameterIndexIds()) {
      await this.db.parameterIndexV5(this.group_id, indexId).createIndex(
        {
          lookup: 1,
          key: 1,
          _id: -1
        },
        {
          name: 'lookup_op_id'
        }
      );
    }
  }

  protected createMongoChecksums(options: MongoSyncBucketStorageOptions): MongoChecksums {
    return new MongoChecksumsV5(this.db, this.group_id, {
      ...options.checksumOptions,
      storageConfig: options?.storageConfig,
      mapping: this.sync_rules.mapping
    });
  }

  createMongoCompactor(options: MongoCompactOptions): MongoCompactor {
    return new MongoCompactorV5(this, this.db, options);
  }

  protected createMongoParameterCompactor(
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ): MongoParameterCompactor {
    return new MongoParameterCompactorV5(this.db, this.group_id, checkpoint, options);
  }

  protected createWriterImpl(batchOptions: MongoBucketBatchOptions): storage.BucketStorageBatch {
    return new MongoBucketBatchV5(batchOptions);
  }

  protected sourceTableBaseId(): Partial<CommonSourceTableDocument> {
    return {};
  }

  protected augmentCreatedSourceTableDocument(
    createDoc: CommonSourceTableDocument,
    options: storage.ResolveTableOptions,
    candidateSourceTable: storage.SourceTable
  ): void {
    const bucketDataSourceIds = options.sync_rules.definition.bucketDataSources
      .filter((source) => source.tableSyncsData(candidateSourceTable))
      .map((source) => this.mapping.bucketSourceId(source));
    const parameterLookupSourceIds = options.sync_rules.definition.bucketParameterLookupSources
      .filter((source) => source.tableSyncsParameters(candidateSourceTable))
      .map((source) => this.mapping.parameterLookupId(source));

    Object.assign(createDoc, {
      bucket_data_source_ids: bucketDataSourceIds,
      parameter_lookup_source_ids: parameterLookupSourceIds
    });
  }

  protected async initializeResolvedSourceRecords(sourceTableId: bson.ObjectId): Promise<void> {
    await this.db.initializeSourceRecordsCollection(this.group_id, sourceTableId);
  }

  protected override get versionContext(): MongoSyncBucketStorageContext<VersionedPowerSyncMongoV5> {
    return {
      db: this.db,
      group_id: this.group_id,
      mapping: this.mapping
    };
  }

  protected getParameterSetsImpl(
    checkpoint: MongoSyncBucketStorageCheckpoint,
    lookups: import('@powersync/service-sync-rules').ScopedParameterLookup[]
  ): Promise<import('@powersync/service-sync-rules').SqliteJsonRow[]> {
    return getParameterSetsShared(
      {
        db: {
          client: this.db.client,
          parameterIndex: (groupId, indexId) => this.db.parameterIndexV5(groupId, indexId)
        },
        group_id: this.group_id
      },
      checkpoint,
      lookups,
      serializeParameterLookupV5
    );
  }

  protected getBucketDataBatchImpl(
    checkpoint: import('@powersync/service-core').utils.InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    return getBucketDataBatchSharedWrapper(
      {
        db: {
          bucketData: (groupId, definitionId) => this.db.bucketDataV5(groupId, definitionId)
        },
        group_id: this.group_id,
        mapping: this.mapping
      },
      checkpoint,
      dataBuckets,
      new V5FormatAdapter(),
      options
    );
  }

  protected async clearBucketData(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listBucketDataCollectionsV5(this.group_id)) {
      await collection.drop();
    }
  }

  protected async clearParameterIndexes(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listParameterIndexCollectionsV5(this.group_id)) {
      await collection.collection.drop();
    }
  }

  protected async clearSourceRecords(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listSourceRecordCollectionsV5(this.group_id)) {
      await collection.drop();
    }
  }

  protected async clearBucketState(_signal?: AbortSignal): Promise<void> {
    await this.db
      .bucketStateV5(this.group_id)
      .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return;
        }
        throw error;
      });
  }

  protected async clearSourceTables(_signal?: AbortSignal): Promise<void> {
    await this.db
      .sourceTablesV5(this.group_id)
      .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return;
        }
        throw error;
      });
  }

  protected getDataBucketChangesImpl(
    options: import('@powersync/service-core').GetCheckpointChangesOptions
  ): Promise<
    Pick<import('@powersync/service-core').CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>
  > {
    return getDataBucketChangesShared(
      {
        db: {
          bucketState: (groupId) => this.db.bucketStateV5(groupId)
        },
        group_id: this.group_id
      },
      options
    );
  }

  protected getParameterBucketChangesImpl(
    options: import('@powersync/service-core').GetCheckpointChangesOptions
  ): Promise<
    Pick<import('@powersync/service-core').CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>
  > {
    return getParameterBucketChangesShared(
      {
        db: {
          parameterIndex: (groupId, indexId) => this.db.parameterIndexV5(groupId, indexId)
        },
        group_id: this.group_id,
        mapping: this.mapping
      },
      options,
      deserializeParameterLookupV5
    );
  }
}
