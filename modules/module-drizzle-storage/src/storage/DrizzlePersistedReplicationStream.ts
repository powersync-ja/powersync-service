import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { eq } from 'drizzle-orm';
import type { SyncRulesRow } from '../drivers/sqlite/schema.js';
import type { DrizzleStorageDialect } from './DrizzleStorageDialect.js';

export class DrizzlePersistedSyncConfigContent extends storage.PersistedSyncConfigContent {
  constructor(
    private readonly dialect: DrizzleStorageDialect,
    row: SyncRulesRow
  ) {
    super({
      replicationStreamId: row.id,
      sync_rules_content: row.content,
      compiled_plan: row.syncPlan,
      replicationStreamName: row.slotName,
      storageVersion: row.storageVersion ?? storage.LEGACY_STORAGE_VERSION,
      syncConfigId: String(row.id)
    });
  }

  async getSyncConfigStatus(): Promise<storage.PersistedSyncConfigStatus | null> {
    const { db, tables } = this.dialect;
    const row = db.select().from(tables.syncRules).where(eq(tables.syncRules.id, this.replicationStreamId)).get();
    return row == null ? null : syncConfigStatusFromRow(row);
  }
}

export class DrizzlePersistedReplicationStream extends storage.PersistedReplicationStream {
  current_lock: storage.ReplicationLock | null = null;
  readonly syncConfigContent: readonly DrizzlePersistedSyncConfigContent[];

  constructor(
    private readonly dialect: DrizzleStorageDialect,
    private readonly row: SyncRulesRow
  ) {
    super({
      replicationStreamId: row.id,
      replicationStreamName: row.slotName,
      state: row.state,
      storageVersion: row.storageVersion ?? storage.LEGACY_STORAGE_VERSION
    });
    this.syncConfigContent = [new DrizzlePersistedSyncConfigContent(this.dialect, this.row)];
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

export function syncConfigStatusFromRow(row: SyncRulesRow): storage.PersistedSyncConfigStatus {
  return {
    id: String(row.id),
    replicationStreamId: row.id,
    state: row.state,
    snapshot_done: row.snapshotDone,
    last_checkpoint_lsn: row.lastCheckpointLsn,
    last_fatal_error: row.lastFatalError,
    last_fatal_error_ts: row.lastFatalErrorTs,
    last_keepalive_ts: row.lastKeepaliveTs,
    last_checkpoint_ts: row.lastCheckpointTs
  };
}
