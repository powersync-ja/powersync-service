// Adapted from https://github.com/kagis/pgwire/blob/0dc927f9f8990a903f238737326e53ba1c8d094f/mod.js#L2218

import * as bson from 'bson';
import * as uuid from 'uuid';
import * as pgwire from '@powersync/service-jpgwire';
import { SqliteJsonValue, SqliteRow, ToastableSqliteRow, toSyncRulesRow } from '@powersync/service-sync-rules';

import * as replication from '../replication/replication-index.js';
import { container } from '@powersync/service-framework';

/**
 * pgwire message -> SQLite row.
 * @param message
 */
export function constructAfterRecord(message: pgwire.PgoutputInsert | pgwire.PgoutputUpdate): SqliteRow {
  const rawData = (message as any).afterRaw;

  const record = pgwire.decodeTuple(message.relation, rawData);
  return toSyncRulesRow(record);
}

export function hasToastedValues(row: ToastableSqliteRow) {
  for (let key in row) {
    if (typeof row[key] == 'undefined') {
      return true;
    }
  }
  return false;
}

export function isCompleteRow(row: ToastableSqliteRow): row is SqliteRow {
  return !hasToastedValues(row);
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

function getRawReplicaIdentity(
  tuple: ToastableSqliteRow,
  columns: replication.ReplicationColumn[]
): Record<string, any> {
  let result: Record<string, any> = {};
  for (let column of columns) {
    const name = column.name;
    result[name] = tuple[name];
  }
  return result;
}
const ID_NAMESPACE = 'a396dd91-09fc-4017-a28d-3df722f651e9';

export function getUuidReplicaIdentityString(
  tuple: ToastableSqliteRow,
  columns: replication.ReplicationColumn[]
): string {
  const rawIdentity = getRawReplicaIdentity(tuple, columns);

  return uuidForRow(rawIdentity);
}

export function uuidForRow(row: SqliteRow): string {
  // Important: This must not change, since it will affect how ids are generated.
  // Use BSON so that it's a well-defined format without encoding ambiguities.
  const repr = bson.serialize(row);
  return uuid.v5(repr, ID_NAMESPACE);
}

export function getUuidReplicaIdentityBson(
  tuple: ToastableSqliteRow,
  columns: replication.ReplicationColumn[]
): bson.UUID {
  if (columns.length == 0) {
    // REPLICA IDENTITY NOTHING - generate random id
    return new bson.UUID(uuid.v4());
  }
  const rawIdentity = getRawReplicaIdentity(tuple, columns);

  return uuidForRowBson(rawIdentity);
}

export function uuidForRowBson(row: SqliteRow): bson.UUID {
  // Important: This must not change, since it will affect how ids are generated.
  // Use BSON so that it's a well-defined format without encoding ambiguities.
  const repr = bson.serialize(row);
  const buffer = Buffer.alloc(16);
  return new bson.UUID(uuid.v5(repr, ID_NAMESPACE, buffer));
}

export function escapeIdentifier(identifier: string) {
  return `"${identifier.replace(/"/g, '""').replace(/\./g, '"."')}"`;
}

export function autoParameter(arg: SqliteJsonValue | boolean): pgwire.StatementParam {
  if (arg == null) {
    return { type: 'varchar', value: null };
  } else if (typeof arg == 'string') {
    return { type: 'varchar', value: arg };
  } else if (typeof arg == 'number') {
    if (Number.isInteger(arg)) {
      return { type: 'int8', value: arg };
    } else {
      return { type: 'float8', value: arg };
    }
  } else if (typeof arg == 'boolean') {
    return { type: 'bool', value: arg };
  } else if (typeof arg == 'bigint') {
    return { type: 'int8', value: arg };
  } else {
    throw new Error(`Unsupported query parameter: ${typeof arg}`);
  }
}

export async function retriedQuery(db: pgwire.PgClient, ...statements: pgwire.Statement[]): Promise<pgwire.PgResult>;
export async function retriedQuery(db: pgwire.PgClient, query: string): Promise<pgwire.PgResult>;

/**
 * Retry a simple query - up to 2 attempts total.
 */
export async function retriedQuery(db: pgwire.PgClient, ...args: any[]) {
  for (let tries = 2; ; tries--) {
    try {
      return await db.query(...args);
    } catch (e) {
      if (tries == 1) {
        throw e;
      }
      container.logger.warn('Query error, retrying', e);
    }
  }
}
