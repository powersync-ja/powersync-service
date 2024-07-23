import { ReplicationEngine } from '../replication/core/ReplicationEngine.js';
import { SyncAPIProvider } from '../api/SyncAPIProvider.js';
import { ResolvedPowerSyncConfig } from '../util/config/types.js';
import { BucketStorageFactory } from '../storage/BucketStorage.js';
import { CorePowerSyncSystem } from './CorePowerSyncSystem.js';

export abstract class ServiceContext {
  abstract configuration: ResolvedPowerSyncConfig;
  abstract storage: BucketStorageFactory;
  abstract replicationEngine: ReplicationEngine;
  abstract syncAPIProvider: SyncAPIProvider;
  abstract system: CorePowerSyncSystem;
}
