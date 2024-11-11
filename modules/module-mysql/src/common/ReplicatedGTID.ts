import mysql from 'mysql2/promise';
import * as uuid from 'uuid';
import * as mysql_utils from '../utils/mysql-utils.js';

export type BinLogPosition = {
  filename: string;
  offset: number;
};

export type ReplicatedGTIDSpecification = {
  raw_gtid: string;
  /**
   * The (end) position in a BinLog file where this transaction has been replicated in.
   */
  position: BinLogPosition;
};

export type BinLogGTIDFormat = {
  server_id: Buffer;
  transaction_range: number;
};

export type BinLogGTIDEvent = {
  raw_gtid: BinLogGTIDFormat;
  position: BinLogPosition;
};

/**
 * A wrapper around the MySQL GTID value.
 * This adds and tracks additional metadata such as the BinLog filename
 * and position where this GTID could be located.
 */
export class ReplicatedGTID {
  static fromSerialized(comparable: string): ReplicatedGTID {
    return new ReplicatedGTID(ReplicatedGTID.deserialize(comparable));
  }

  private static deserialize(comparable: string): ReplicatedGTIDSpecification {
    const components = comparable.split('|');
    if (components.length < 3) {
      throw new Error(`Invalid serialized GTID: ${comparable}`);
    }

    return {
      raw_gtid: components[1],
      position: {
        filename: components[2],
        offset: parseInt(components[3])
      } satisfies BinLogPosition
    };
  }

  static fromBinLogEvent(event: BinLogGTIDEvent) {
    const { raw_gtid, position } = event;
    const stringGTID = `${uuid.stringify(raw_gtid.server_id)}:${raw_gtid.transaction_range}`;
    return new ReplicatedGTID({
      raw_gtid: stringGTID,
      position
    });
  }

  /**
   * Special case for the zero GTID which means no transactions have been executed.
   */
  static ZERO = new ReplicatedGTID({ raw_gtid: '0:0', position: { filename: '', offset: 0 } });

  constructor(protected options: ReplicatedGTIDSpecification) {}

  /**
   * Get the BinLog position of this replicated GTID event
   */
  get position() {
    return this.options.position;
  }

  /**
   * Get the raw Global Transaction ID. This of the format `server_id:transaction_ranges`
   */
  get raw() {
    return this.options.raw_gtid;
  }

  get serverId() {
    return this.options.raw_gtid.split(':')[0];
  }

  /**
   * Transforms a GTID into a comparable string format, ensuring lexicographical
   * order aligns with the GTID's relative age. This assumes that all GTIDs
   * have the same server ID.
   *
   * @returns A comparable string in the format
   *   `padded_end_transaction|raw_gtid|binlog_filename|binlog_position`
   */
  get comparable() {
    const { raw, position } = this;
    const [, transactionRanges] = this.raw.split(':');

    let maxTransactionId = 0;

    for (const range of transactionRanges.split(',')) {
      const [start, end] = range.split('-');
      maxTransactionId = Math.max(maxTransactionId, parseInt(start, 10), parseInt(end || start, 10));
    }

    const paddedTransactionId = maxTransactionId.toString().padStart(16, '0');
    return [paddedTransactionId, raw, position.filename, position.offset].join('|');
  }

  toString() {
    return this.comparable;
  }

  /**
   * Calculates the distance in bytes from this GTID to the provided argument.
   */
  async distanceTo(connection: mysql.Connection, to: ReplicatedGTID): Promise<number | null> {
    const [logFiles] = await mysql_utils.retriedQuery({
      connection,
      query: `SHOW BINARY LOGS;`
    });

    // Default to the first file for the start to handle the zero GTID case.
    const startFileIndex = Math.max(
      logFiles.findIndex((f) => f['Log_name'] == this.position.filename),
      0
    );
    const startFileEntry = logFiles[startFileIndex];

    if (!startFileEntry) {
      return null;
    }

    /**
     * Fall back to the next position for comparison if the replicated position is not present
     */
    const endPosition = to.position;

    // Default to the past the last file to cater for the HEAD case
    const testEndFileIndex = logFiles.findIndex((f) => f['Log_name'] == endPosition?.filename);
    // If the endPosition is not defined and found. Fallback to the last file as the end
    const endFileIndex = testEndFileIndex < 0 && !endPosition ? logFiles.length : logFiles.length - 1;

    const endFileEntry = logFiles[endFileIndex];

    if (!endFileEntry) {
      return null;
    }

    return (
      startFileEntry['File_size'] -
      this.position.offset -
      endFileEntry['File_size'] +
      endPosition.offset +
      logFiles.slice(startFileIndex + 1, endFileIndex).reduce((sum, file) => sum + file['File_size'], 0)
    );
  }
}
