import { SyncRuleState } from './BucketStorage.js';
import { PersistedSyncConfigId } from './PersistedSyncConfigContent.js';

export interface PersistedSyncConfigStatus {
  readonly id: PersistedSyncConfigId;
  readonly replicationStreamId: number;
  readonly state: SyncRuleState;
  readonly snapshot_done?: boolean;
  readonly last_checkpoint_lsn: string | null;
  readonly last_fatal_error?: string | null;
  readonly last_fatal_error_ts?: Date | null;
  readonly last_keepalive_ts?: Date | null;
  readonly last_checkpoint_ts?: Date | null;
}
