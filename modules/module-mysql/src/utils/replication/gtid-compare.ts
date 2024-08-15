import mysql from 'mysql2/promise';
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

export function extractGTIDComparable(comparableGTID: string) {
  return comparableGTID.split('|')[1];
}

export async function readMasterComparableGtid(db: mysql.Pool): Promise<string> {
  const [[gtidResult]] = await db.query<mysql.RowDataPacket[]>('SELECT @@GLOBAL.gtid_executed as GTID;');
  return gtidMakeComparable(gtidResult?.GTID ?? '0:0');
}
