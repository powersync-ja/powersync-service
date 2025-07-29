import mysqlPromise from 'mysql2/promise';
import * as mysql_utils from '../utils/mysql-utils.js';

export const KEEP_ALIVE_TABLE = 'powersync_keepalive';

export async function ensureKeepAliveConfiguration(connection: mysqlPromise.Connection): Promise<void> {
  await mysql_utils.retriedQuery({
    connection,
    query: `
      CREATE TABLE IF NOT EXISTS
        ${KEEP_ALIVE_TABLE}(
          id INT PRIMARY KEY DEFAULT 1, 
          last_ping TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`
  });

  // Ensure initial entry
  await mysql_utils.retriedQuery({
    connection,
    query: `INSERT IGNORE INTO ${KEEP_ALIVE_TABLE} (id) VALUES (1)`
  });
}

export async function pingKeepAlive(connection: mysqlPromise.Connection): Promise<void> {
  await mysql_utils.retriedQuery({
    connection,
    // The last_ping timestamp is automatically updated
    query: `UPDATE ${KEEP_ALIVE_TABLE} SET last_ping = NOW() WHERE id = 1`
  });
}

export async function getLastKeepAlive(connection: mysqlPromise.Connection): Promise<Date | null> {
  const [[result]] = await mysql_utils.retriedQuery({
    connection,
    query: `SELECT last_ping FROM ${KEEP_ALIVE_TABLE} WHERE id = 1`
  });

  return result.last_ping ? new Date(result.last_ping) : null;
}

export async function tearDownKeepAlive(connection: mysqlPromise.Connection): Promise<void> {
  await mysql_utils.retriedQuery({
    connection,
    query: `DROP TABLE IF EXISTS ${KEEP_ALIVE_TABLE}`
  });
}
