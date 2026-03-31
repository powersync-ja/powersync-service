import { InternalOpId } from '@powersync/service-core';
import { BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import type { VersionedPowerSyncMongo } from '../db.js';
import * as bson from 'bson';

export interface MongoSyncBucketStorageContext<TDb extends VersionedPowerSyncMongo = VersionedPowerSyncMongo> {
  db: TDb;
  group_id: number;
  mapping: BucketDefinitionMapping;
}

export interface MongoSyncBucketStorageCheckpoint {
  checkpoint: InternalOpId;
  snapshotTime: bson.Timestamp;
}
