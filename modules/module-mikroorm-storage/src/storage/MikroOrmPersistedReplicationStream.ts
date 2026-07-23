import { MikroORM } from '@mikro-orm/core';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import type { SyncRules } from '../entities/entities-index.js';
import { MikroOrmStorageDialect } from './MikroOrmStorageDialect.js';

export class MikroOrmPersistedSyncConfigContent extends storage.PersistedSyncConfigContent {
  constructor(
    private readonly orm: MikroORM,
    private readonly dialect: MikroOrmStorageDialect,
    row: SyncRules
  ) {
    super({
      replicationStreamId: Number(row.id),
      sync_rules_content: row.content,
      compiled_plan: row.syncPlan,
      replicationStreamName: row.slotName,
      storageVersion: row.storageVersion ?? storage.LEGACY_STORAGE_VERSION,
      syncConfigId: String(row.id),
      syncConfigState: row.state as storage.SyncRuleState
    });
  }

  async getSyncConfigStatus(): Promise<storage.PersistedSyncConfigStatus | null> {
    const em = this.orm.em.fork();
    const row = await em.findOne(this.dialect.syncRulesEntity, {
      id: this.replicationStreamId
    });

    return row == null ? null : syncConfigStatusFromRow(row);
  }
}

export class MikroOrmPersistedReplicationStream extends storage.PersistedReplicationStream {
  current_lock: storage.ReplicationLock | null = null;
  readonly syncConfigContent: readonly MikroOrmPersistedSyncConfigContent[];

  constructor(
    private readonly orm: MikroORM,
    private readonly dialect: MikroOrmStorageDialect,
    private readonly row: SyncRules
  ) {
    super({
      replicationStreamId: Number(row.id),
      replicationStreamName: row.slotName,
      state: row.state,
      storageVersion: row.storageVersion ?? storage.LEGACY_STORAGE_VERSION
    });
    this.syncConfigContent = [new MikroOrmPersistedSyncConfigContent(this.orm, this.dialect, this.row)];
  }

  parsed(options: storage.ParseSyncConfigOptions): storage.ParsedSyncConfigSet {
    return this.syncConfigContent[0].parsed(options);
  }

  async lock(): Promise<storage.ReplicationLock> {
    if (this.current_lock != null) {
      throw new ServiceError(ErrorCode.PSYNC_S1003, `Replication stream is locked by this process.`);
    }

    return (this.current_lock = {
      sync_rules_id: this.replicationStreamId,
      release: async () => {
        this.current_lock = null;
      }
    });
  }
}

export function syncConfigStatusFromRow(row: SyncRules): storage.PersistedSyncConfigStatus {
  return {
    id: String(row.id),
    replicationStreamId: Number(row.id),
    state: row.state,
    snapshot_done: row.snapshotDone,
    last_checkpoint_lsn: row.lastCheckpointLsn,
    last_fatal_error: row.lastFatalError,
    last_fatal_error_ts: row.lastFatalErrorTs,
    last_keepalive_ts: row.lastKeepaliveTs,
    last_checkpoint_ts: row.lastCheckpointTs
  };
}
