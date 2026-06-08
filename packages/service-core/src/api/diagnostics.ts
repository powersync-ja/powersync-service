import { logger, ServiceAssertionError } from '@powersync/lib-services-framework';
import { DEFAULT_TAG, SourceTableRef, SyncConfigWithErrors } from '@powersync/service-sync-rules';
import { ReplicationError, SyncRulesStatus, TableInfo } from '@powersync/service-types';

import * as storage from '../storage/storage-index.js';
import { syncConfigYamlErrorToReplicationError } from '../util/errors.js';
import { RouteAPI, SlotWalBudgetInfo } from './RouteAPI.js';

export interface DiagnosticsOptions {
  /**
   * Include sync config content in response.
   */
  include_content?: boolean;

  /**
   * Check against storage database.
   *
   * If false, uses placeholder values for e.g. initial_replication_done.
   */
  live_status: boolean;

  /**
   * Check against the source postgres connection.
   */
  check_connection: boolean;
}

export const DEFAULT_DATASOURCE_ID = 'default';

export async function getSyncRulesStatus(
  bucketStorage: storage.BucketStorageFactory,
  apiHandler: RouteAPI,
  sync_rules: storage.PersistedSyncConfigContent | null,
  options: DiagnosticsOptions,
  syncConfigStatus?: storage.PersistedSyncConfigStatus | null,
  /**
   * Storage instance for the replication stream of this config.
   *
   * Required to populate live status (snapshot/checkpoint info). The content object
   * itself is no longer a replication stream, so the caller must resolve this.
   */
  systemStorage?: storage.SyncRulesBucketStorage
): Promise<SyncRulesStatus | undefined> {
  if (sync_rules == null) {
    return undefined;
  }

  const include_content = options.include_content ?? false;
  const live_status = options.live_status ?? false;
  const check_connection = options.check_connection ?? false;
  const now = new Date().toISOString();

  let parsed: SyncConfigWithErrors;
  let persisted: storage.ParsedSyncConfigSet;
  try {
    persisted = sync_rules.parsed(apiHandler.getParseSyncRulesOptions());
    // A content object represents a single sync config, so its parsed result has exactly one entry.
    const [singleConfig] = persisted.syncConfigs;
    if (singleConfig == null) {
      throw new ServiceAssertionError('Expected one sync config');
    }
    parsed = singleConfig;
  } catch (e) {
    return {
      content: include_content ? sync_rules.sync_rules_content : undefined,
      connections: [],
      errors: [{ level: 'fatal', message: e.message, ts: now }]
    };
  }

  const { config: rules, errors: syncRuleErrors } = parsed;
  const sourceConfig = await apiHandler.getSourceConfig();
  // This method can run under some situations if no connection is configured yet.
  // It will return a default tag in such a case. This default tag is not module specific.
  const tag = sourceConfig.tag ?? DEFAULT_TAG;
  const status = live_status ? await systemStorage?.getStatus() : undefined;
  let replication_lag_bytes: number | undefined = undefined;
  let slot_wal_budget: SlotWalBudgetInfo | undefined = undefined;

  let tables_flat: TableInfo[] = [];

  if (check_connection) {
    const source_table_patterns = rules.getSourceTables();
    const resolved_tables = await apiHandler.getDebugTablesInfo(source_table_patterns, rules);
    tables_flat = resolved_tables.flatMap((info) => {
      if (info.table) {
        return [info.table];
      } else if (info.tables) {
        return info.tables;
      } else {
        return [];
      }
    });

    if (systemStorage) {
      try {
        replication_lag_bytes = await apiHandler.getReplicationLagBytes({
          bucketStorage: systemStorage
        });
      } catch (e) {
        // Ignore
        logger.warn(`Unable to get replication lag`, e);
      }

      if (apiHandler.getSlotWalBudget && sync_rules.replicationStreamName) {
        try {
          slot_wal_budget = await apiHandler.getSlotWalBudget({
            slotName: sync_rules.replicationStreamName
          });
        } catch (e) {
          logger.warn(`Unable to get WAL budget`, e);
        }
      }
    }
  } else {
    const source_table_patterns = rules.getSourceTables();

    tables_flat = source_table_patterns.map((pattern): TableInfo => {
      if (pattern.isWildcard) {
        return {
          schema: pattern.schema,
          name: pattern.tablePrefix,
          pattern: pattern.isWildcard ? pattern.tablePattern : undefined,

          data_queries: false,
          parameter_queries: false,
          replication_id: [],
          errors: [{ level: 'fatal', message: 'connection failed', ts: now }]
        };
      } else {
        const source: SourceTableRef = {
          connectionTag: tag,
          schema: pattern.schema,
          name: pattern.tablePattern
        };
        const syncData = rules.tableSyncsData(source);
        const syncParameters = rules.tableSyncsParameters(source);
        return {
          schema: pattern.schema,
          name: pattern.name,
          data_queries: syncData,
          parameter_queries: syncParameters,
          replication_id: [],
          errors: [{ level: 'fatal', message: 'connection failed', ts: now }]
        };
      }
    });
  }

  const errors = tables_flat.flatMap((info) => info.errors);
  const statusSource = syncConfigStatus ?? sync_rules;

  if (statusSource.last_fatal_error) {
    errors.push({
      level: 'fatal',
      message: statusSource.last_fatal_error,
      ts: statusSource.last_fatal_error_ts?.toISOString()
    });
  }
  errors.push(...syncRuleErrors.map((error) => syncConfigYamlErrorToReplicationError(error, now)));

  if (
    slot_wal_budget &&
    slot_wal_budget.wal_status !== 'lost' &&
    slot_wal_budget.safe_wal_size != null &&
    slot_wal_budget.max_slot_wal_keep_size != null &&
    slot_wal_budget.max_slot_wal_keep_size > 0
  ) {
    const budgetPct = Math.max(
      0,
      Math.round((slot_wal_budget.safe_wal_size / slot_wal_budget.max_slot_wal_keep_size) * 100)
    );
    if (budgetPct <= 50) {
      errors.push({
        level: 'warning',
        message:
          `WAL budget is low: ${budgetPct}% remaining. ` +
          `The replication slot may be invalidated if WAL consumption ` +
          `continues at this rate. Consider increasing max_slot_wal_keep_size.`,
        ts: now
      });
    }
  }

  if (live_status && status?.active) {
    // Check replication lag for active replication stream.
    // Right now we exclude mysql, since it we don't have consistent keepalives for it.
    if (statusSource.last_checkpoint_ts == null && statusSource.last_keepalive_ts == null) {
      errors.push({
        level: 'warning',
        message: 'No checkpoint found, cannot calculate replication lag',
        ts: now
      });
    } else {
      const lastTime = Math.max(
        statusSource.last_checkpoint_ts?.getTime() ?? 0,
        statusSource.last_keepalive_ts?.getTime() ?? 0
      );
      const lagSeconds = Math.round((Date.now() - lastTime) / 1000);
      // On idle instances, keepalive messages are only persisted every 60 seconds.
      // So we use 5 minutes as a threshold for warnings, and 15 minutes for critical.
      // The replication lag metric should give a more granular value, but that is not available directly
      // in the API containers used for diagnostics, and this should give a good enough indication.
      if (lagSeconds > 15 * 60) {
        errors.push({
          level: 'fatal',
          message: `No replicated commit in more than ${lagSeconds}s`,
          ts: now
        });
      } else if (lagSeconds > 5 * 60) {
        errors.push({
          level: 'warning',
          message: `No replicated commit in more than ${lagSeconds}s`,
          ts: now
        });
      }
    }
  }

  return {
    content: include_content ? sync_rules.sync_rules_content : undefined,
    connections: [
      {
        id: sourceConfig.id ?? DEFAULT_DATASOURCE_ID,
        tag: tag,
        slot_name: sync_rules.replicationStreamName,
        initial_replication_done: status?.snapshot_done ?? false,
        // TODO: Rename?
        last_lsn: status?.checkpoint_lsn ?? undefined,
        last_checkpoint_ts: statusSource.last_checkpoint_ts?.toISOString(),
        last_keepalive_ts: statusSource.last_keepalive_ts?.toISOString(),
        replication_lag_bytes: replication_lag_bytes,
        wal_status: slot_wal_budget?.wal_status,
        safe_wal_size: slot_wal_budget?.safe_wal_size,
        max_slot_wal_keep_size: slot_wal_budget?.max_slot_wal_keep_size,
        tables: tables_flat
      }
    ],
    errors: deduplicate(errors)
  };
}

function deduplicate(errors: ReplicationError[]): ReplicationError[] {
  let seen = new Set<string>();
  let result: ReplicationError[] = [];
  for (let error of errors) {
    const key = JSON.stringify(error);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(error);
  }
  return result;
}
