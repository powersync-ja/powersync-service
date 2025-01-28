// Adapted from https://github.com/kagis/pgwire/blob/0dc927f9f8990a903f238737326e53ba1c8d094f/mod.js#L2218

import * as pgwire from '@powersync/service-jpgwire';
import { DatabaseInputRow, SqliteRow, toSyncRulesRow } from '@powersync/service-sync-rules';

/**
 * pgwire message -> SQLite row.
 * @param message
 */
export function constructAfterRecord(message: pgwire.PgoutputInsert | pgwire.PgoutputUpdate): SqliteRow {
  const rawData = (message as any).afterRaw;

  const record = decodeTuple(message.relation, rawData);
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
  const record = decodeTuple(message.relation, rawData);
  return toSyncRulesRow(record);
}

/**
 * We need a high level of control over how values are decoded, to make sure there is no loss
 * of precision in the process.
 */
export function decodeTuple(relation: pgwire.PgoutputRelation, tupleRaw: Record<string, any>): DatabaseInputRow {
  let result: Record<string, any> = {};
  for (let columnName in tupleRaw) {
    const rawval = tupleRaw[columnName];
    const typeOid = (relation as any)._tupleDecoder._typeOids.get(columnName);
    if (typeof rawval == 'string' && typeOid) {
      result[columnName] = pgwire.PgType.decode(rawval, typeOid);
    } else {
      result[columnName] = rawval;
    }
  }
  return result;
}
