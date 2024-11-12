import mysqlPromise from 'mysql2/promise';
import * as mysql_utils from '../utils/mysql-utils.js';
import { ReplicatedGTID } from './ReplicatedGTID.js';

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

  return new ReplicatedGTID({
    // The head always points to the next position to start replication from
    position,
    raw_gtid: binlogStatus.Executed_Gtid_Set
  });
}

export async function isBinlogStillAvailable(
  connection: mysqlPromise.Connection,
  binlogFile: string
): Promise<boolean> {
  const [logFiles] = await mysql_utils.retriedQuery({
    connection,
    query: `SHOW BINARY LOGS;`
  });

  return logFiles.some((f) => f['Log_name'] == binlogFile);
}
