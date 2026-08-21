import { SqliteValue } from './types.js';

export const SQLITE_TRUE = 1n;
export const SQLITE_FALSE = 0n;

export function sqliteBool(value: SqliteValue | boolean): 1n | 0n {
  if (value == null) {
    return SQLITE_FALSE;
  } else if (typeof value == 'boolean' || typeof value == 'number') {
    return value ? SQLITE_TRUE : SQLITE_FALSE;
  } else if (typeof value == 'bigint') {
    return value != 0n ? SQLITE_TRUE : SQLITE_FALSE;
  } else if (typeof value == 'string') {
    return parseInt(value) ? SQLITE_TRUE : SQLITE_FALSE;
  } else {
    return SQLITE_FALSE;
  }
}

export function sqliteNot(value: SqliteValue | boolean) {
  return sqliteBool(!sqliteBool(value));
}
