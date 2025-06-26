import { logger } from '@powersync/lib-services-framework';
import mysql from 'mysql2';
import mysqlPromise from 'mysql2/promise';
import * as types from '../types/types.js';
import { coerce, eq, gte, satisfies } from 'semver';
import { SourceTable } from '@powersync/service-core';

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
  // TODO: Use config.lookup for DNS resolution
  return mysql.createPool({
    host: config.hostname,
    user: config.username,
    port: config.port,
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

export async function getMySQLVersion(connection: mysqlPromise.Connection): Promise<string> {
  const [[versionResult]] = await retriedQuery({
    connection,
    query: `SELECT VERSION() as version`
  });

  return versionResult.version as string;
}

/**
 *  Check if the current MySQL version is newer or equal to the target version.
 *  @param version
 *  @param minimumVersion
 */
export function isVersionAtLeast(version: string, minimumVersion: string): boolean {
  const coercedVersion = coerce(version);
  const coercedMinimumVersion = coerce(minimumVersion);

  return gte(coercedVersion!, coercedMinimumVersion!, { loose: true });
}

export function satisfiesVersion(version: string, targetVersion: string): boolean {
  const coercedVersion = coerce(version);

  return satisfies(coercedVersion!, targetVersion!, { loose: true });
}

export function escapeMysqlTableName(table: SourceTable): string {
  return `\`${table.schema.replaceAll('`', '``')}\`.\`${table.name.replaceAll('`', '``')}\``;
}
