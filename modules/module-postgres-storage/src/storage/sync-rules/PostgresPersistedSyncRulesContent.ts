import * as lib_postgres from '@powersync/lib-service-postgres';
import { ErrorCode, logger, ServiceError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import {
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  HydrationState,
  SqlSyncRules,
  versionedHydrationState
} from '@powersync/service-sync-rules';
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
      last_checkpoint_lsn: row.last_checkpoint_lsn,
      slot_name: row.slot_name,
      last_fatal_error: row.last_fatal_error,
      last_checkpoint_ts: row.last_checkpoint_ts ? new Date(row.last_checkpoint_ts) : null,
      last_keepalive_ts: row.last_keepalive_ts ? new Date(row.last_keepalive_ts) : null,
      active: row.state == 'ACTIVE',
      storageVersion: row.storage_version ?? storage.LEGACY_STORAGE_VERSION
    });
  }

  parsed(options: storage.ParseSyncRulesOptions): storage.PersistedSyncRules {
    let hydrationState: HydrationState;
    const syncRules = SqlSyncRules.fromYaml(this.sync_rules_content, options);
    const storageConfig = this.getStorageConfig();
    if (
      storageConfig.versionedBuckets ||
      syncRules.config.compatibility.isEnabled(CompatibilityOption.versionedBucketIds)
    ) {
      hydrationState = versionedHydrationState(this.id);
    } else {
      hydrationState = DEFAULT_HYDRATION_STATE;
    }
    return {
      id: this.id,
      slot_name: this.slot_name,
      sync_rules: syncRules,
      hydratedSyncRules() {
        return this.sync_rules.config.hydrate({
          hydrationState
        });
      }
    };
  }

  async lock(): Promise<storage.ReplicationLock> {
    const manager = new lib_postgres.PostgresLockManager({
      db: this.db,
      name: `sync_rules_${this.id}_${this.slot_name}`
    });
    const lockHandle = await manager.acquire();
    if (!lockHandle) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1003,
        `Sync rules: ${this.id} have been locked by another process for replication.`
      );
    }

    const interval = setInterval(async () => {
      try {
        await lockHandle.refresh();
      } catch (e) {
        logger.error('Failed to refresh lock', e);
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
