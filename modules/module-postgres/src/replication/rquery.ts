import { PgChunk, PgClient, PgResult, Statement } from '@powersync/service-jpgwire';
import { mapPostgresReplicationError } from '../utils/errors.js';

export function rquery(db: PgClient, query: string): Promise<PgResult>;
export function rquery(db: PgClient, ...statements: Statement[]): Promise<PgResult>;

/**
 * Run a replication query, mapping errors.
 */
export async function rquery(db: PgClient, ...args: any[]): Promise<PgResult> {
  try {
    return await db.query(...args);
  } catch (e) {
    let query: string | undefined;
    if (typeof args[0] == 'string') {
      query = args[0];
    } else if (typeof args[0]?.statement == 'string') {
      query = args[0].statement;
    }
    throw mapPostgresReplicationError(e, query);
  }
}

export function rstream(db: PgClient, query: string): AsyncIterableIterator<PgChunk>;
export function rstream(db: PgClient, ...statements: Statement[]): AsyncIterableIterator<PgChunk>;
export async function* rstream(db: PgClient, ...args: any[]): AsyncIterableIterator<PgChunk> {
  try {
    yield* db.stream(...args);
  } catch (e) {
    let query: string | undefined;
    if (typeof args[0] == 'string') {
      query = args[0];
    } else if (typeof args[0]?.statement == 'string') {
      query = args[0].statement;
    }
    throw mapPostgresReplicationError(e, query);
  }
}
