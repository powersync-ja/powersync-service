import mysqlPromise from 'mysql2/promise';
import * as mysql_utils from '../utils/mysql-utils.js';
import { ReplicatedGTID } from './ReplicatedGTID.js';

/**
 * Gets the `@@server_uuid` of the current connected server
 */
export async function readServerUuid(connection: mysqlPromise.Connection): Promise<string> {
  const [[result]] = await mysql_utils.retriedQuery({
    connection,
    query: `SELECT @@server_uuid AS server_uuid`
  });
  return result.server_uuid;
}

/**
 * Gets the current master HEAD GTID
 */
export async function readExecutedGtid(connection: mysqlPromise.Connection): Promise<ReplicatedGTID> {
  const version = await mysql_utils.getMySQLVersion(connection);

  let binlogStatus: mysqlPromise.RowDataPacket;
  if (mysql_utils.isVersionAtLeast(version, '8.4.0')) {
    // Syntax for the below query changed in 8.4.0
    const [[binLogResult]] = await mysql_utils.retriedQuery({
      connection,
      query: `SHOW BINARY LOG STATUS`
    });
    binlogStatus = binLogResult;
  } else {
    const [[binLogResult]] = await mysql_utils.retriedQuery({
      connection,
      query: `SHOW MASTER STATUS`
    });
    binlogStatus = binLogResult;
  }
  const position = {
    filename: binlogStatus.File,
    offset: parseInt(binlogStatus.Position)
  };

  const activeServerUuid = await readServerUuid(connection);
  const executedGtidSet = binlogStatus.Executed_Gtid_Set.trim();

  if (executedGtidSet.length === 0) {
    // New server with no transactions executed yet. Keep the current binlog
    // coordinate so this synthetic GTID can still be validated after a restart.
    return new ReplicatedGTID({
      rawGtid: `${activeServerUuid}:0`,
      position
    });
  }

  const gtidSets = executedGtidSet.split(',');
  const latestActiveGtid = await getLatestActiveGtid(gtidSets, activeServerUuid);
  return new ReplicatedGTID({
    rawGtid: latestActiveGtid,
    position
  });
}

export async function getLatestActiveGtid(gtidSets: string[], activeServerUuid: string): Promise<string> {
  for (const gtidSet of gtidSets) {
    const [serverUuid, ...intervals] = gtidSet.trim().split(':');
    if (serverUuid === activeServerUuid) {
      let maxTransactionId: number | null = null;
      for (const interval of intervals) {
        const [start, end] = interval.split('-');
        const startId = parseInt(start, 10);
        const endId = end !== undefined ? parseInt(end, 10) : startId;
        if (!Number.isNaN(startId)) {
          maxTransactionId = Math.max(maxTransactionId ?? 0, startId);
        }
        if (!Number.isNaN(endId)) {
          maxTransactionId = Math.max(maxTransactionId ?? 0, endId);
        }
      }
      return activeServerUuid + ':' + maxTransactionId;
    }
  }

  return `${activeServerUuid}:0`;
}

/**
 * Checks that a stored resume GTID is still part of the server's executed history and that its
 * binlog coordinate is still readable. This detects source rewinds where a restored server keeps
 * the same UUID or recreates a binlog with the same filename but a shorter length.
 */
export async function isGtidPositionStillAvailable(
  connection: mysqlPromise.Connection,
  gtid: ReplicatedGTID
): Promise<boolean> {
  const [logFiles] = await mysql_utils.retriedQuery({
    connection,
    query: `SHOW BINARY LOGS;`
  });
  const logFile = logFiles.find((file) => file['Log_name'] == gtid.position.filename);

  if (!logFile || Number(logFile['File_size']) < gtid.position.offset) {
    return false;
  }

  // Transaction zero is PowerSync's synthetic position before the first
  // transaction from this server UUID. It is not valid MySQL GTID_SET syntax,
  // so its availability is determined by the binlog coordinate above.
  if (gtid.raw.split(':')[1] === '0') {
    return true;
  }

  const [[result]] = await mysql_utils.retriedQuery({
    connection,
    query: `SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed) AS is_executed`,
    params: [gtid.raw]
  });

  return result.is_executed === 1;
}
