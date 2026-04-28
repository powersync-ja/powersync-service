import { InternalOpId } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import type { VersionedPowerSyncMongo } from '../db.js';

export interface MongoSyncBucketStorageContext<TDb extends VersionedPowerSyncMongo = VersionedPowerSyncMongo> {
  db: TDb;
  group_id: number;
  mapping: BucketDefinitionMapping;
}

export interface MongoSyncBucketStorageCheckpoint {
  checkpoint: InternalOpId;
  snapshotTime: bson.Timestamp;
}
