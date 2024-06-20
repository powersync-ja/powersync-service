import { DEFAULT_TAG, SourceTableInterface, SqlSyncRules } from '@powersync/service-sync-rules';
import { pgwireRows } from '@powersync/service-jpgwire';
import { ConnectionStatus, SyncRulesStatus, TableInfo, baseUri } from '@powersync/service-types';

import * as replication from '../replication/replication-index.js';
import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import { CorePowerSyncSystem } from '../system/CorePowerSyncSystem.js';
import { container } from '@powersync/lib-services-framework';

export async function getConnectionStatus(system: CorePowerSyncSystem): Promise<ConnectionStatus | null> {
  if (system.pgwire_pool == null) {
    return null;
  }

  const pool = system.requirePgPool();

  const base = {
    id: system.config.connection!.id,
    postgres_uri: baseUri(system.config.connection!)
  };
  try {
    await util.retriedQuery(pool, `SELECT 'PowerSync connection test'`);
  } catch (e) {
    return {
      ...base,
      connected: false,
      errors: [{ level: 'fatal', message: e.message }]
    };
  }

  try {
    await replication.checkSourceConfiguration(pool);
  } catch (e) {
    return {
      ...base,
      connected: true,
      errors: [{ level: 'fatal', message: e.message }]
    };
  }

  return {
    ...base,
    connected: true,
    errors: []
  };
}

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

export async function getSyncRulesStatus(
  sync_rules: storage.PersistedSyncRulesContent | null,
  system: CorePowerSyncSystem,
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
    persisted = sync_rules.parsed();
    rules = persisted.sync_rules;
  } catch (e) {
    return {
      content: include_content ? sync_rules.sync_rules_content : undefined,
      connections: [],
      errors: [{ level: 'fatal', message: e.message }]
    };
  }

  const systemStorage = live_status ? await system.storage.getInstance(persisted) : undefined;
  const status = await systemStorage?.getStatus();
  let replication_lag_bytes: number | undefined = undefined;

  let tables_flat: TableInfo[] = [];

  if (check_connection) {
    const pool = system.requirePgPool();

    const source_table_patterns = rules.getSourceTables();
    const wc = new replication.WalConnection({
      db: pool,
      sync_rules: rules
    });
    const resolved_tables = await wc.getDebugTablesInfo(source_table_patterns);
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
        const results = await util.retriedQuery(pool, {
          statement: `SELECT
      slot_name,
      confirmed_flush_lsn, 
      pg_current_wal_lsn(), 
      (pg_current_wal_lsn() - confirmed_flush_lsn) AS lsn_distance
    FROM pg_replication_slots WHERE slot_name = $1 LIMIT 1;`,
          params: [{ type: 'varchar', value: systemStorage!.slot_name }]
        });
        const [row] = pgwireRows(results);
        if (row) {
          replication_lag_bytes = Number(row.lsn_distance);
        }
      } catch (e) {
        // Ignore
        container.logger.warn(`Unable to get replication lag`, e);
      }
    }
  } else {
    const source_table_patterns = rules.getSourceTables();
    const tag = system.config.connection!.tag ?? DEFAULT_TAG;

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
        id: system.config.connection!.id,
        tag: system.config.connection!.tag ?? DEFAULT_TAG,
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
