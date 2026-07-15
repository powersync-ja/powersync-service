import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import * as bson from 'bson';

export interface MongoSyncBucketStorageCheckpoint {
  checkpoint: InternalOpId;
  snapshotTime: bson.Timestamp;
  clusterTime: mongo.ClusterTime;
}
