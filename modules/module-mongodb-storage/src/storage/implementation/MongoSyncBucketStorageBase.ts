import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId, storage, utils } from '@powersync/service-core';
import { ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import {
  getBucketDataBatchSharedWrapper,
  getDataBucketChangesShared,
  getParameterBucketChangesShared,
  getParameterSetsShared
} from './bucket-operations/storage-operations.js';
import { MongoSyncBucketStorageCheckpoint } from './common/MongoSyncBucketStorageContext.js';
import { BucketDataFormatAdapter } from './document-formats/format-interface.js';
import {
  deserializeParameterLookup,
  serializeParameterLookup
} from './document-formats/parameter-lookup.js';
import { BucketDefinitionId, BucketDefinitionMapping, ParameterIndexId } from './BucketDefinitionMapping.js';
import { CommonSourceTableDocument, StorageConfig } from './models.js';
import { MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { MongoChecksumOptions, MongoChecksums } from './MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from './MongoCompactor.js';
import { MongoParameterCompactor } from './MongoParameterCompactor.js';
import { MongoPersistedSyncRulesContent } from './MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageOptions } from './MongoSyncBucketStorage.js';
import { VersionedPowerSyncMongo } from './db.js';

export interface MongoSyncBucketStorageBaseCallbacks {
  bucketData: (groupId: number, definitionId: BucketDefinitionId) => mongo.Collection<any>;
  parameterIndex: (groupId: number, indexId: ParameterIndexId) => mongo.Collection<any>;
  bucketState: (groupId: number) => mongo.Collection<any>;
  sourceTables: (groupId: number) => mongo.Collection<any>;
  listBucketDataCollections: (groupId: number) => Promise<mongo.Collection<any>[]>;
  listParameterIndexCollections: (groupId: number) => Promise<{ collection: mongo.Collection<any>; indexId: ParameterIndexId }[]>;
  listSourceRecordCollections: (groupId: number) => Promise<mongo.Collection<any>[]>;
  createChecksums: (
    db: VersionedPowerSyncMongo,
    groupId: number,
    options: MongoChecksumOptions & { storageConfig?: StorageConfig; mapping: BucketDefinitionMapping }
  ) => MongoChecksums;
  createCompactor: (storage: MongoSyncBucketStorage, db: VersionedPowerSyncMongo, options: MongoCompactOptions) => MongoCompactor;
  createParameterCompactor: (
    db: VersionedPowerSyncMongo,
    groupId: number,
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ) => MongoParameterCompactor;
  createWriter: (batchOptions: MongoBucketBatchOptions) => storage.BucketStorageBatch;
  formatAdapter: BucketDataFormatAdapter;
}

export class MongoSyncBucketStorageBase extends MongoSyncBucketStorage {
  protected get callbacks(): MongoSyncBucketStorageBaseCallbacks {
    return this._versionCallbacks as MongoSyncBucketStorageBaseCallbacks;
  }

  constructor(
    factory: MongoBucketStorage,
    group_id: number,
    sync_rules: MongoPersistedSyncRulesContent,
    slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions,
    callbacks: MongoSyncBucketStorageBaseCallbacks
  ) {
    super(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options, callbacks);
  }

  createMongoCompactor(options: MongoCompactOptions): MongoCompactor {
    return this.callbacks.createCompactor(this, this.db, options);
  }

  protected createMongoChecksums(options: MongoSyncBucketStorageOptions): MongoChecksums {
    return this.callbacks.createChecksums(this.db, this.group_id, {
      ...options.checksumOptions,
      storageConfig: options?.storageConfig,
      mapping: this.sync_rules.mapping
    });
  }

  protected createMongoParameterCompactor(
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ): MongoParameterCompactor {
    return this.callbacks.createParameterCompactor(this.db, this.group_id, checkpoint, options);
  }

  protected createWriterImpl(batchOptions: MongoBucketBatchOptions): storage.BucketStorageBatch {
    return this.callbacks.createWriter(batchOptions);
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
    await (this.db as any).initializeSourceRecordsCollection(this.group_id, sourceTableId);
  }

  protected async initializeVersionStorage(): Promise<void> {
    const mapping = this.mapping;
    for (let source of mapping.allBucketDefinitionIds()) {
      const collection = this.callbacks.bucketData(this.group_id, source).collectionName;
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
      await this.callbacks.parameterIndex(this.group_id, indexId).createIndex(
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

  protected getParameterSetsImpl(
    checkpoint: MongoSyncBucketStorageCheckpoint,
    lookups: ScopedParameterLookup[]
  ): Promise<SqliteJsonRow[]> {
    return getParameterSetsShared(
      {
        db: {
          client: this.db.client,
          parameterIndex: (groupId, indexId) => this.callbacks.parameterIndex(groupId, indexId)
        },
        group_id: this.group_id
      },
      checkpoint,
      lookups,
      serializeParameterLookup
    );
  }

  protected getBucketDataBatchImpl(
    checkpoint: utils.InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    return getBucketDataBatchSharedWrapper(
      {
        db: {
          bucketData: (groupId, definitionId) => this.callbacks.bucketData(groupId, definitionId)
        },
        group_id: this.group_id,
        mapping: this.mapping
      },
      checkpoint,
      dataBuckets,
      this.callbacks.formatAdapter,
      options
    );
  }

  protected async clearBucketData(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.callbacks.listBucketDataCollections(this.group_id)) {
      await collection.drop();
    }
  }

  protected async clearParameterIndexes(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.callbacks.listParameterIndexCollections(this.group_id)) {
      await collection.collection.drop();
    }
  }

  protected async clearSourceRecords(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.callbacks.listSourceRecordCollections(this.group_id)) {
      await collection.drop();
    }
  }

  protected async clearBucketState(_signal?: AbortSignal): Promise<void> {
    await this.callbacks
      .bucketState(this.group_id)
      .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return;
        }
        throw error;
      });
  }

  protected async clearSourceTables(_signal?: AbortSignal): Promise<void> {
    await this.callbacks
      .sourceTables(this.group_id)
      .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return;
        }
        throw error;
      });
  }

  protected getDataBucketChangesImpl(
    options: storage.GetCheckpointChangesOptions
  ): Promise<Pick<storage.CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
    return getDataBucketChangesShared(
      {
        db: {
          bucketState: (groupId) => this.callbacks.bucketState(groupId)
        },
        group_id: this.group_id
      },
      options
    );
  }

  protected getParameterBucketChangesImpl(
    options: storage.GetCheckpointChangesOptions
  ): Promise<Pick<storage.CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    return getParameterBucketChangesShared(
      {
        db: {
          parameterIndex: (groupId, indexId) => this.callbacks.parameterIndex(groupId, indexId)
        },
        group_id: this.group_id,
        mapping: this.mapping
      },
      options,
      deserializeParameterLookup
    );
  }
}
