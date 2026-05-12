import * as lib_postgres from '@powersync/lib-service-postgres';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { models } from '../../types/types.js';

export class PostgresPersistedSyncRulesContent extends storage.PersistedSyncRulesContent {
  current_lock: storage.ReplicationLock | null = null;

  constructor(
    private db: lib_postgres.DatabaseClient,
    row: models.SyncRulesDecoded
  ) {
    super({
      id: Number(row.id),
      sync_rules_content: row.content,
      compiled_plan: row.sync_plan,
      last_checkpoint_lsn: row.last_checkpoint_lsn,
      slot_name: row.slot_name,
      last_fatal_error: row.last_fatal_error,
      last_checkpoint_ts: row.last_checkpoint_ts ? new Date(row.last_checkpoint_ts) : null,
      last_keepalive_ts: row.last_keepalive_ts ? new Date(row.last_keepalive_ts) : null,
      active: row.state == 'ACTIVE',
      storageVersion: row.storage_version ?? storage.LEGACY_STORAGE_VERSION
    });
  }

  async lock(): Promise<storage.ReplicationLock> {
    const manager = new lib_postgres.PostgresLockManager({
      db: this.db,
      name: `sync_rules_${this.id}_${this.slot_name}`
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
      sync_rules_id: this.id,
      release: async () => {
        clearInterval(interval);
        return lockHandle.release();
      }
    });
  }
}
