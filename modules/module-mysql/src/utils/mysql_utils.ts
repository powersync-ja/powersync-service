import { logger } from '@powersync/lib-services-framework';
import mysql from 'mysql2/promise';
import * as types from '../types/types.js';

export type RetiredMySQLQueryOptions = {
  db: mysql.Pool;
  query: string;
  params?: any[];
  retries?: number;
};

/**
 * Retry a simple query - up to 2 attempts total.
 */
export async function retriedQuery(options: RetiredMySQLQueryOptions) {
  const { db, query, params = [], retries = 2 } = options;
  for (let tries = retries; ; tries--) {
    try {
      logger.debug(`Executing query: ${query}`);
      return db.query<mysql.RowDataPacket[]>(query, params);
    } catch (e) {
      if (tries == 1) {
        throw e;
      }
      logger.warn('Query error, retrying', e);
    }
  }
}

export function createPool(config: types.ResolvedConnectionConfig) {
  const sslOptions = {
    ca: config.cacert,
    key: config.client_private_key,
    cert: config.client_certificate
  };
  const hasSSLOptions = Object.values(sslOptions).some((v) => !!v);
  return mysql.createPool({
    host: config.hostname,
    user: config.username,
    password: config.password,
    database: config.database,
    ssl: hasSSLOptions ? sslOptions : undefined
  });
}

export function createConnection(config: types.ResolvedConnectionConfig) {
  return mysql.createConnection(config);
}
