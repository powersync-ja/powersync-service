import { logger } from '@powersync/lib-services-framework';
import { DEFAULT_TAG, SourceTableInterface, SqlSyncRules } from '@powersync/service-sync-rules';
import { SyncRulesStatus, TableInfo } from '@powersync/service-types';

import * as storage from '../storage/storage-index.js';
import { RouteAPI } from './RouteAPI.js';

export interface DiagnosticsOptions {
  /**
   * Include sync rules content in response.
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
  sync_rules: storage.PersistedSyncRulesContent | null,
  options: DiagnosticsOptions
): Promise<SyncRulesStatus | undefined> {
  if (sync_rules == null) {
    return undefined;
  }

  const include_content = options.include_content ?? false;
  const live_status = options.live_status ?? false;
  const check_connection = options.check_connection ?? false;

  let rules: SqlSyncRules;
  let persisted: storage.PersistedSyncRules;
  try {
    persisted = sync_rules.parsed(apiHandler.getParseSyncRulesOptions());
    rules = persisted.sync_rules;
  } catch (e) {
    return {
      content: include_content ? sync_rules.sync_rules_content : undefined,
      connections: [],
      errors: [{ level: 'fatal', message: e.message }]
    };
  }

  const sourceConfig = await apiHandler.getSourceConfig();
  const tag = sourceConfig.tag ?? DEFAULT_TAG;
  const systemStorage = live_status ? bucketStorage.getInstance(sync_rules) : undefined;
  const status = await systemStorage?.getStatus();
  let replication_lag_bytes: number | undefined = undefined;

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
        replication_lag_bytes = await apiHandler.getReplicationLag(systemStorage.slot_name);
      } catch (e) {
        // Ignore
        logger.warn(`Unable to get replication lag`, e);
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
          errors: [{ level: 'fatal', message: 'connection failed' }]
        };
      } else {
        const source: SourceTableInterface = {
          connectionTag: tag,
          schema: pattern.schema,
          table: pattern.tablePattern
        };
        const syncData = rules.tableSyncsData(source);
        const syncParameters = rules.tableSyncsParameters(source);
        return {
          schema: pattern.schema,
          name: pattern.name,
          data_queries: syncData,
          parameter_queries: syncParameters,
          replication_id: [],
          errors: [{ level: 'fatal', message: 'connection failed' }]
        };
      }
    });
  }

  const errors = tables_flat.flatMap((info) => info.errors);
  if (sync_rules.last_fatal_error) {
    errors.push({ level: 'fatal', message: sync_rules.last_fatal_error });
  }
  errors.push(
    ...rules.errors.map((e) => {
      return {
        level: e.type,
        message: e.message
      };
    })
  );

  return {
    content: include_content ? sync_rules.sync_rules_content : undefined,
    connections: [
      {
        id: sourceConfig.id ?? DEFAULT_DATASOURCE_ID,
        tag: tag,
        slot_name: sync_rules.slot_name,
        initial_replication_done: status?.snapshot_done ?? false,
        // TODO: Rename?
        last_lsn: status?.checkpoint_lsn ?? undefined,
        last_checkpoint_ts: sync_rules.last_checkpoint_ts?.toISOString(),
        last_keepalive_ts: sync_rules.last_keepalive_ts?.toISOString(),
        replication_lag_bytes: replication_lag_bytes,
        tables: tables_flat
      }
    ],
    errors: deduplicate(errors)
  };
}

function deduplicate(errors: { level: 'warning' | 'fatal'; message: string }[]) {
  let seen = new Set<string>();
  let result: { level: 'warning' | 'fatal'; message: string }[] = [];
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
