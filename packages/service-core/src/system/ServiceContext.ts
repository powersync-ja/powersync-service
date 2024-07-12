import { ReplicationEngine } from '../replication/core/ReplicationEngine.js';
import { SyncAPIProvider } from '../api/SyncAPIProvider.js';
import { ResolvedPowerSyncConfig } from '../util/config/types.js';
import { BucketStorageFactory } from '../storage/BucketStorage.js';

export type ServiceContext = {
  configuration: ResolvedPowerSyncConfig;
  storage: BucketStorageFactory;
  replicationEngine: ReplicationEngine;
  syncAPIProvider: SyncAPIProvider;
};
