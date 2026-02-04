import * as lib_postgres from '@powersync/lib-service-postgres';
import { ErrorCode, logger, ServiceError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { CompatibilityOption, SqlSyncRules, versionedHydrationState } from '@powersync/service-sync-rules';

import { models } from '../../types/types.js';
import { DEFAULT_HYDRATION_STATE, HydrationState } from '@powersync/service-sync-rules';

export class PostgresPersistedSyncRulesContent implements storage.PersistedSyncRulesContent {
  public readonly slot_name: string;

  public readonly id: number;
  public readonly sync_rules_content: string;
  public readonly last_checkpoint_lsn: string | null;
  public readonly last_fatal_error: string | null;
  public readonly last_keepalive_ts: Date | null;
  public readonly last_checkpoint_ts: Date | null;
  public readonly active: boolean;
  current_lock: storage.ReplicationLock | null = null;

  constructor(
    private db: lib_postgres.DatabaseClient,
    row: models.SyncRulesDecoded
  ) {
    this.id = Number(row.id);
    this.sync_rules_content = row.content;
    this.last_checkpoint_lsn = row.last_checkpoint_lsn;
    this.slot_name = row.slot_name;
    this.last_fatal_error = row.last_fatal_error;
    this.last_checkpoint_ts = row.last_checkpoint_ts ? new Date(row.last_checkpoint_ts) : null;
    this.last_keepalive_ts = row.last_keepalive_ts ? new Date(row.last_keepalive_ts) : null;
    this.active = row.state == 'ACTIVE';
  }

  parsed(options: storage.ParseSyncRulesOptions): storage.PersistedSyncRules {
    let hydrationState: HydrationState;
    const syncRules = SqlSyncRules.fromYaml(this.sync_rules_content, options);
    if (syncRules.compatibility.isEnabled(CompatibilityOption.versionedBucketIds)) {
      hydrationState = versionedHydrationState(this.id);
    } else {
      hydrationState = DEFAULT_HYDRATION_STATE;
    }
    return {
      id: this.id,
      slot_name: this.slot_name,
      sync_rules: syncRules,
      hydratedSyncRules() {
        return this.sync_rules.hydrate({
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
