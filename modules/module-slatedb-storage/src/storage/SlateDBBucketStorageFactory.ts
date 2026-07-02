import { framework, storage } from '@powersync/service-core';

import { SlateDBStorageConfigDecoded } from '../types/types.js';

export type SlateDBBucketStorageOptions = {
  config: SlateDBStorageConfigDecoded;
  replicationStreamNamePrefix: string;
};

export class SlateDBBucketStorageFactory extends storage.BucketStorageFactory {
  [framework.DO_NOT_LOG] = true;

  readonly replicationStreamNamePrefix: string;

  constructor(readonly options: SlateDBBucketStorageOptions) {
    super();
    this.replicationStreamNamePrefix = options.replicationStreamNamePrefix;
  }

  getInstance(): storage.SyncRulesBucketStorage {
    throw notImplemented('getInstance');
  }

  async updateSyncRules(): Promise<storage.PersistedReplicationStream> {
    throw notImplemented('updateSyncRules');
  }

  async restartReplication(): Promise<void> {
    throw notImplemented('restartReplication');
  }

  async getActiveSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    throw notImplemented('getActiveSyncConfig');
  }

  async getDeployingSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    throw notImplemented('getDeployingSyncConfig');
  }

  async getReplicatingReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    throw notImplemented('getReplicatingReplicationStreams');
  }

  async getStoppedReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    throw notImplemented('getStoppedReplicationStreams');
  }

  async getStorageMetrics(): Promise<storage.StorageMetrics> {
    throw notImplemented('getStorageMetrics');
  }

  async getPowerSyncInstanceId(): Promise<string> {
    throw notImplemented('getPowerSyncInstanceId');
  }

  async getSystemIdentifier(): Promise<storage.BucketStorageSystemIdentifier> {
    throw notImplemented('getSystemIdentifier');
  }

  async [Symbol.asyncDispose](): Promise<void> {
    // No SlateDB resources are opened by the scaffold.
  }
}

function notImplemented(method: string): Error {
  return new Error(`SlateDB bucket storage scaffold: ${method} is not implemented yet.`);
}
