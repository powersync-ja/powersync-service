import { logger } from '@powersync/lib-services-framework';
import mysql from 'mysql2/promise';

export type RetiredMySQLQueryOptions = {
  db: mysql.Pool;
  query: string;
  params?: any[];
  retries?: number;
};

/**
 * Retry a simple query - up to 2 attempts total.
 */
export async function retriedMysqlQuery(options: RetiredMySQLQueryOptions) {
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

/**
 * Transforms a GTID into a comparable string format, ensuring lexicographical
 * order aligns with the GTID's relative age. This assumes that all GTIDs
 * have the same server ID.
 *
 * @param gtid - The GTID string in the format 'server_id:transaction_ranges'
 * @returns A comparable string in the format 'padded_end_transaction|original_gtid'
 */
export function gtidMakeComparable(gtid: string): string {
  const [serverId, transactionRanges] = gtid.split(':');

  let maxTransactionId = 0;

  for (const range of transactionRanges.split(',')) {
    const [start, end] = range.split('-');
    maxTransactionId = Math.max(maxTransactionId, parseInt(start, 10), parseInt(end || start, 10));
  }

  const paddedTransactionId = maxTransactionId.toString().padStart(16, '0');
  return `${paddedTransactionId}|${gtid}`;
}

export async function readMasterComparableGtid(db: mysql.Pool): Promise<string> {
  const results = await db.query<mysql.RowDataPacket[]>('SELECT @@GLOBAL.gtid_executed as GTID;');
  return gtidMakeComparable(results[0].length > 0 ? `${results[0][0]['GTID']}` : '0:0');
}
