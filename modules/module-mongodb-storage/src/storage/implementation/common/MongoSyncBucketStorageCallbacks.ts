import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId, storage } from '@powersync/service-core';
import { BucketDefinitionId, BucketDefinitionMapping, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { VersionedPowerSyncMongo } from '../db.js';
import { BucketDataFormatAdapter } from '../document-formats/format-interface.js';
import { StorageConfig } from '../models.js';
import { MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { MongoChecksumOptions, MongoChecksums } from '../MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from '../MongoCompactor.js';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';

export interface MongoSyncBucketStorageCallbacks {
  bucketData: (groupId: number, definitionId: BucketDefinitionId) => mongo.Collection<any>;
  parameterIndex: (groupId: number, indexId: ParameterIndexId) => mongo.Collection<any>;
  bucketState: (groupId: number) => mongo.Collection<any>;
  sourceTables: (groupId: number) => mongo.Collection<any>;
  listBucketDataCollections: (groupId: number) => Promise<mongo.Collection<any>[]>;
  listParameterIndexCollections: (
    groupId: number
  ) => Promise<{ collection: mongo.Collection<any>; indexId: ParameterIndexId }[]>;
  listSourceRecordCollections: (groupId: number) => Promise<mongo.Collection<any>[]>;
  createChecksums: (
    db: VersionedPowerSyncMongo,
    groupId: number,
    options: MongoChecksumOptions & { storageConfig?: StorageConfig; mapping: BucketDefinitionMapping }
  ) => MongoChecksums;
  createCompactor: (storage: any, db: VersionedPowerSyncMongo, options: MongoCompactOptions) => MongoCompactor;
  createParameterCompactor: (
    db: VersionedPowerSyncMongo,
    groupId: number,
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ) => MongoParameterCompactor;
  createWriter: (batchOptions: MongoBucketBatchOptions) => storage.BucketStorageBatch;
  formatAdapter: BucketDataFormatAdapter;
}
