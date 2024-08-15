import mysql from 'mysql2/promise';
import { retriedQuery } from '../mysql_utils.js';

export type GetGTIDPositionOptions = {
  db: mysql.Pool;
  gtids: Set<string>;
  page_size?: number;
};

export type GTIDPosition = {
  filename: string;
  position: number;
};

/**
 * Given a set of GTID it will find the binlog files and offset positions.
 */
export async function getGTIDPositions(options: GetGTIDPositionOptions): Promise<Map<string, GTIDPosition>> {
  const { db, gtids, page_size = 1000 } = options;
  const [binLogFileResults] = await retriedQuery({
    db,
    query: `SHOW BINARY LOGS`
  });

  const binLogFilenames: string[] = binLogFileResults.map((r) => r.Log_name);
  const output: Map<string, GTIDPosition> = new Map();

  for (const filename of binLogFilenames.reverse()) {
    let returnedCount = 0;
    let pageIndex = 0;
    do {
      // Unfortunately this cannot be sorted in descending order
      const [events] = await retriedQuery({
        db,
        query: `SHOW BINLOG EVENTS IN ? FROM ? LIMIT ?`,
        params: [filename, pageIndex++ * page_size, page_size]
      });

      returnedCount = events.length;
      const matchedEvent = events.find((e) => e['Event_type'] == 'Previous_gtids' && gtids.has(e['Info']));

      if (matchedEvent) {
        output.set(matchedEvent['Previous_gtids'], {
          filename,
          position: matchedEvent['Pos']
        });

        if (output.size == gtids.size) {
          return output;
        }
      }
    } while (returnedCount > 0);
  }
  return output;
}
