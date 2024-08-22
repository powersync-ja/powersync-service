import * as sync_rules from '@powersync/service-sync-rules';

export function toSQLiteRow(row: Record<string, any>): sync_rules.SqliteRow {
  for (let key in row) {
    if (row[key] instanceof Date) {
      row[key] = row[key].toISOString();
    }
  }
  return sync_rules.toSyncRulesRow(row);
}
