import mysqlPromise from 'mysql2/promise';
import * as mysql_utils from '../utils/mysql_utils.js';
import { gte } from 'semver';

import { ReplicatedGTID } from './ReplicatedGTID.js';
import { getMySQLVersion } from './check-source-configuration.js';
import { logger } from '@powersync/lib-services-framework';

/**
 * Gets the current master HEAD GTID
 */
export async function readExecutedGtid(connection: mysqlPromise.Connection): Promise<ReplicatedGTID> {
  const version = await getMySQLVersion(connection);
  let binlogStatus: mysqlPromise.RowDataPacket;
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

  logger.info('Succesfully read executed GTID', { position });

  return new ReplicatedGTID({
    // The head always points to the next position to start replication from
    position,
    raw_gtid: binlogStatus.Executed_Gtid_Set
  });
}
