// Adapted from https://github.com/kagis/pgwire/blob/0dc927f9f8990a903f238737326e53ba1c8d094f/mod.js#L2218

import * as pgwire from '@powersync/service-jpgwire';
import { SqliteJsonValue } from '@powersync/service-sync-rules';

import { logger } from '@powersync/lib-services-framework';

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
      logger.warn('Query error, retrying', e);
    }
  }
}
