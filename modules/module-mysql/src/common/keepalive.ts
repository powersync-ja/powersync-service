import mysqlPromise, { ResultSetHeader } from 'mysql2/promise';
import * as mysql_utils from '../utils/mysql-utils.js';

export const KEEP_ALIVE_TABLE = 'powersync_keepalive';

export async function ensureKeepAliveConfiguration(connection: mysqlPromise.Connection): Promise<void> {
  const [results] = await mysql_utils.retriedQuery({
    connection,
    query: `
        SELECT TABLE_NAME
        FROM information_schema.tables
        WHERE TABLE_NAME = '${KEEP_ALIVE_TABLE}'
    `
  });
  if (results.length > 0) {
    const lastKeepAlive = await getLastKeepAlive(connection);
    if (lastKeepAlive) {
      // If the table exists and has an entry, we can assume it's already set up
      return;
    } else {
      // If the table exists but has no entry, we need to ensure it has an initial entry
      await mysql_utils.retriedQuery({
        connection,
        query: `INSERT IGNORE INTO ${KEEP_ALIVE_TABLE} (id) VALUES (1)`
      });
      return;
    }
  }

  // Try to create the keep-alive table
  await createKeepAliveTable(connection);
}

async function createKeepAliveTable(connection: mysqlPromise.Connection): Promise<void> {
  // Try to create the keep-alive table
  await mysql_utils.retriedQuery({
    connection,
    query: `
      CREATE TABLE IF NOT EXISTS
        ${KEEP_ALIVE_TABLE}(
          id INT PRIMARY KEY DEFAULT 1, 
          last_ping TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
        )`
  });

  // Ensure initial entry
  await mysql_utils.retriedQuery({
    connection,
    query: `INSERT IGNORE INTO ${KEEP_ALIVE_TABLE} (id) VALUES (1)`
  });
}

export async function pingKeepAlive(connection: mysqlPromise.Connection): Promise<void> {
  const [result] = await connection.query<ResultSetHeader>(
    `UPDATE ${KEEP_ALIVE_TABLE} SET last_ping = NOW(3) WHERE id = 1` // The last_ping timestamp is automatically updated
  );

  if (result.affectedRows === 0) {
    // If no rows were affected, it means the table was not set up correctly
    throw new Error(`Failed to update ${KEEP_ALIVE_TABLE} table.`);
  }
}

export async function getLastKeepAlive(connection: mysqlPromise.Connection): Promise<Date | null> {
  const [[result]] = await mysql_utils.retriedQuery({
    connection,
    query: `SELECT last_ping FROM ${KEEP_ALIVE_TABLE} WHERE id = 1`
  });

  return result?.last_ping ? new Date(result.last_ping) : null;
}

export async function tearDownKeepAlive(connection: mysqlPromise.Connection): Promise<void> {
  await mysql_utils.retriedQuery({
    connection,
    query: `DROP TABLE IF EXISTS ${KEEP_ALIVE_TABLE}`
  });
}
