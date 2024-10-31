import { logger } from '@powersync/lib-services-framework';
import mysql from 'mysql2';
import mysqlPromise from 'mysql2/promise';
import * as types from '../types/types.js';

export type RetriedQueryOptions = {
  connection: mysqlPromise.Connection;
  query: string;
  params?: any[];
  retries?: number;
};

/**
 * Retry a simple query - up to 2 attempts total.
 */
export async function retriedQuery(options: RetriedQueryOptions) {
  const { connection, query, params = [], retries = 2 } = options;
  for (let tries = retries; ; tries--) {
    try {
      logger.debug(`Executing query: ${query}`);
      return connection.query<mysqlPromise.RowDataPacket[]>(query, params);
    } catch (e) {
      if (tries == 1) {
        throw e;
      }
      logger.warn('Query error, retrying', e);
    }
  }
}

export function createPool(config: types.NormalizedMySQLConnectionConfig, options?: mysql.PoolOptions): mysql.Pool {
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
    ssl: hasSSLOptions ? sslOptions : undefined,
    supportBigNumbers: true,
    decimalNumbers: true,
    timezone: 'Z', // Ensure no auto timezone manipulation of the dates occur
    jsonStrings: true, // Return JSON columns as strings
    ...(options || {})
  });
}

/**
 *  Return a random server id for a given sync rule id.
 *  Expected format is: <syncRuleId>00<random number>
 *  The max value for server id in MySQL is 2^32 - 1.
 *  We use the GTID format to keep track of our position in the binlog, no state is kept by the MySQL server, therefore
 *  it is ok to use a randomised server id every time.
 *  @param syncRuleId
 */
export function createRandomServerId(syncRuleId: number): number {
  return Number.parseInt(`${syncRuleId}00${Math.floor(Math.random() * 10000)}`);
}
