import mysql from 'mysql2/promise';
import { retriedQuery } from '../mysql_utils.js';
import { getGTIDPositions } from './gtid-position.js';

export type GetGTIDDistanceOptions = {
  db: mysql.Pool;
  start_gtid: string;
  end_gtid: string;
  page_size?: number;
};

/**
 * Get the distance in bytes between GTIDs
 */
export async function getGTIDDistance(options: GetGTIDDistanceOptions): Promise<number | null> {
  const { db, page_size = 1000, start_gtid, end_gtid } = options;
  const [logFiles] = await retriedQuery({
    db,
    query: `SHOW BINARY LOGS;`
  });

  const gtids = new Set<string>();
  gtids.add(start_gtid);
  gtids.add(end_gtid);

  const positions = await getGTIDPositions({
    db,
    gtids,
    page_size
  });

  const endResult = positions.get(end_gtid);
  const startResult = positions.get(start_gtid);

  if (!endResult || !startResult) {
    return null;
  }

  if (endResult.filename == startResult.filename) {
    return endResult.position - startResult.position;
  }

  // The entries are on different files
  const startFileIndex = logFiles.findIndex((f) => f['Log_name'] == startResult.filename);
  const startFileEntry = logFiles[startFileIndex];

  if (!startFileEntry) {
    return null;
  }

  const endFileIndex = logFiles.findIndex((f) => f['Log_name'] == endResult.filename);
  const endFileEntry = logFiles[endFileIndex];

  if (!endFileEntry) {
    return null;
  }

  return (
    startFileEntry['File_size'] -
    startResult.position +
    endResult.position +
    logFiles.slice(startFileIndex + 1, endFileIndex).reduce((sum, file) => sum + file['File_size'], 0)
  );
}
