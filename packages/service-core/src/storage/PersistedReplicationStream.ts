import { logger as defaultLogger, ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { Logger } from 'winston';
import { SyncRuleState } from './BucketStorage.js';
import { ReplicationLock } from './ReplicationLock.js';
import { STORAGE_VERSION_CONFIG, StorageVersionConfig } from './StorageVersionConfig.js';

export abstract class PersistedReplicationStream implements PersistedReplicationStreamData {
  readonly replicationStreamId: number;
  readonly replicationJobId: string;
  readonly replicationStreamName: string;
  readonly state: SyncRuleState;
  readonly storageVersion: number;
  readonly logger: Logger;

  abstract readonly current_lock: ReplicationLock | null;

  constructor(data: PersistedReplicationStreamData) {
    this.replicationStreamId = data.replicationStreamId;
    this.replicationJobId = data.replicationJobId ?? String(data.replicationStreamId);
    this.replicationStreamName = data.replicationStreamName;
    this.state = data.state;
    this.storageVersion = data.storageVersion;
    this.logger = defaultLogger.child({ prefix: `[${this.replicationStreamName}] ` });
  }

  getStorageConfig(): StorageVersionConfig {
    const storageConfig = STORAGE_VERSION_CONFIG[this.storageVersion];
    if (storageConfig == null) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1005,
        `Unsupported storage version ${this.storageVersion} for replication stream ${this.replicationStreamId}`
      );
    }
    return storageConfig;
  }

  abstract lock(): Promise<ReplicationLock>;
}
export interface PersistedReplicationStreamData {
  readonly replicationStreamId: number;
  readonly replicationStreamName: string;
  readonly state: SyncRuleState;
  readonly storageVersion: number;
  /**
   * Uniquely identifies a job, as a combination of replication stream and processing sync configs.
   */
  readonly replicationJobId?: string;
}
