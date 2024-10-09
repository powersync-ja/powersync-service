import mysql from 'mysql2/promise';
import * as mysql_utils from '../utils/mysql_utils.js';
import { gte } from 'semver';

import { ReplicatedGTID } from './ReplicatedGTID.js';

/**
 * Gets the current master HEAD GTID
 */
export async function readExecutedGtid(connection: mysql.Connection): Promise<ReplicatedGTID> {
  const [[versionResult]] = await mysql_utils.retriedQuery({
    connection,
    query: `SELECT VERSION() as version`
  });

  const version = versionResult.version as string;
  let binlogStatus: mysql.RowDataPacket;
  if (gte(version, '8.4.0')) {
    // Get the BinLog status
    const [[binLogResult]] = await mysql_utils.retriedQuery({
      connection,
      query: `SHOW BINARY LOG STATUS`
    });
    binlogStatus = binLogResult;
  } else {
    // TODO Check if this works for version 5.7
    // Get the BinLog status
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
