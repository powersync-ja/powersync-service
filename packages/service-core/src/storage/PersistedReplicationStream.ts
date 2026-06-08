import { logger as defaultLogger, ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { Logger } from 'winston';
import { SyncRuleState } from './BucketStorage.js';
import { ReplicationLock } from './ReplicationLock.js';
import { STORAGE_VERSION_CONFIG, StorageVersionConfig } from './StorageVersionConfig.js';

export abstract class PersistedReplicationStream implements PersistedReplicationStreamData {
  readonly id: number;
  readonly replicationJobId: string;
  readonly slot_name: string;
  readonly state: SyncRuleState;
  readonly storageVersion: number;
  readonly logger: Logger;

  abstract readonly current_lock: ReplicationLock | null;

  constructor(data: PersistedReplicationStreamData) {
    this.id = data.id;
    this.replicationJobId = data.replicationJobId ?? String(data.id);
    this.slot_name = data.slot_name;
    this.state = data.state;
    this.storageVersion = data.storageVersion;
    this.logger = defaultLogger.child({ prefix: `[${this.slot_name}] ` });
  }

  getStorageConfig(): StorageVersionConfig {
    const storageConfig = STORAGE_VERSION_CONFIG[this.storageVersion];
    if (storageConfig == null) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1005,
        `Unsupported storage version ${this.storageVersion} for replication stream ${this.id}`
      );
    }
    return storageConfig;
  }

  abstract lock(): Promise<ReplicationLock>;
}
export interface PersistedReplicationStreamData {
  readonly id: number;
  readonly slot_name: string;
  readonly state: SyncRuleState;
  readonly storageVersion: number;
  readonly replicationJobId?: string;
}
