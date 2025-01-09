// Adapted from https://github.com/kagis/pgwire/blob/0dc927f9f8990a903f238737326e53ba1c8d094f/mod.js#L2218

import * as pgwire from '@powersync/service-jpgwire';
import { SqliteRow, toSyncRulesRow } from '@powersync/service-sync-rules';

/**
 * pgwire message -> SQLite row.
 * @param message
 */
export function constructAfterRecord(message: pgwire.PgoutputInsert | pgwire.PgoutputUpdate): SqliteRow {
  const rawData = (message as any).afterRaw;

  const record = pgwire.decodeTuple(message.relation, rawData);
  return toSyncRulesRow(record);
}

/**
 * pgwire message -> SQLite row.
 * @param message
 */
export function constructBeforeRecord(message: pgwire.PgoutputDelete | pgwire.PgoutputUpdate): SqliteRow | undefined {
  const rawData = (message as any).beforeRaw;
  if (rawData == null) {
    return undefined;
  }
  const record = pgwire.decodeTuple(message.relation, rawData);
  return toSyncRulesRow(record);
}
