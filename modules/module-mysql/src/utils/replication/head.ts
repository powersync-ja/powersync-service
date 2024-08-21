import mysql from 'mysql2/promise';
import { retriedQuery } from '../mysql_utils.js';
import { ReplicatedGTID } from './GTID.js';

/**
 * Gets the current master HEAD GTID
 */
export async function readMasterGtid(db: mysql.Pool): Promise<ReplicatedGTID> {
  // Get the GTID
  const [[gtidResult]] = await db.query<mysql.RowDataPacket[]>('SELECT @@GLOBAL.gtid_executed as GTID;');

  // Get the BinLog position if master
  const [[masterResult]] = await retriedQuery({
    db,
    query: `SHOW master STATUS`
  });
  const position = {
    filename: masterResult.File,
    offset: parseInt(masterResult.Position)
  };

  return new ReplicatedGTID({
    // The head always points to the next position to start replication from
    position,
    raw_gtid: gtidResult.GTID
  });
}
