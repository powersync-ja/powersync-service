import * as lib_postgres from '@powersync/lib-service-postgres';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { models } from '../../types/types.js';

export class PostgresPersistedSyncRulesContent extends storage.PersistedSyncConfigContent {
  constructor(
    private db: lib_postgres.DatabaseClient,
    row: models.SyncRulesDecoded
  ) {
    super({
      replicationStreamId: Number(row.id),
      sync_rules_content: row.content,
      compiled_plan: row.sync_plan,
      replicationStreamName: row.slot_name,
      storageVersion: row.storage_version ?? storage.LEGACY_STORAGE_VERSION
    });
  }

  async getSyncConfigStatus(): Promise<storage.PersistedSyncConfigStatus | null> {
    const row = await this.db.sql`
      SELECT
        *
      FROM
        sync_rules
      WHERE
        id = ${{ value: this.replicationStreamId, type: 'int4' }}
    `
      .decoded(models.SyncRules)
      .first();

    return row == null ? null : syncConfigStatusFromRow(row);
  }
}

function syncConfigStatusFromRow(row: models.SyncRulesDecoded): storage.PersistedSyncConfigStatus {
  return {
    id: String(row.id),
    replicationStreamId: Number(row.id),
    state: row.state as storage.SyncRuleState,
    snapshot_done: row.snapshot_done,
    last_checkpoint_lsn: row.last_checkpoint_lsn,
    last_fatal_error: row.last_fatal_error,
    last_keepalive_ts: row.last_keepalive_ts ? new Date(row.last_keepalive_ts) : null,
    last_checkpoint_ts: row.last_checkpoint_ts ? new Date(row.last_checkpoint_ts) : null
  };
}

export class PostgresPersistedReplicationStream extends storage.PersistedReplicationStream {
  current_lock: storage.ReplicationLock | null = null;
  readonly syncConfigContent: readonly PostgresPersistedSyncRulesContent[];

  constructor(
    private db: lib_postgres.DatabaseClient,
    private readonly row: models.SyncRulesDecoded
  ) {
    super({
      replicationStreamId: Number(row.id),
      replicationStreamName: row.slot_name,
      state: row.state as storage.SyncRuleState,
      storageVersion: row.storage_version ?? storage.LEGACY_STORAGE_VERSION
    });
    this.syncConfigContent = [new PostgresPersistedSyncRulesContent(this.db, this.row)];
  }

  parsed(options: storage.ParseSyncConfigOptions): storage.ParsedSyncConfigSet {
    return this.syncConfigContent[0].parsed(options);
  }

  async lock(): Promise<storage.ReplicationLock> {
    const manager = new lib_postgres.PostgresLockManager({
      db: this.db,
      name: `sync_rules_${this.replicationStreamId}_${this.replicationStreamName}`
    });
    const lockHandle = await manager.acquire();
    if (!lockHandle) {
      throw new ServiceError(ErrorCode.PSYNC_S1003, `Replication stream is locked by another process, standing by.`);
    }

    const interval = setInterval(async () => {
      try {
        await lockHandle.refresh();
      } catch (e) {
        this.logger.error('Failed to refresh lock', e);
        clearInterval(interval);
      }
    }, 30_130);

    return (this.current_lock = {
      sync_rules_id: this.replicationStreamId,
      release: async () => {
        clearInterval(interval);
        return lockHandle.release();
      }
    });
  }
}
