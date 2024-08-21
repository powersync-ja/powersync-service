import mysql from 'mysql2/promise';
import { retriedQuery } from '../mysql_utils.js';

export type BinLogPosition = {
  filename: string;
  offset: number;
};

export type ReplicatedGTIDSpecification = {
  raw_gtid: string;
  /**
   * The position in a BinLog file where this transaction has been replicated in.
   */
  replicated_position?: BinLogPosition;
  /**
   * The position to start from for the next replication event.
   */
  next_position?: BinLogPosition;
};

/**
 * A wrapper around the MySQL GTID value.
 * This adds and tracks additional metadata such as the BinLog filename
 * and position where this GTID could be located.
 */
export class ReplicatedGTID {
  static deserialize(comparable: string): ReplicatedGTIDSpecification {
    const components = comparable.split('|');
    const currentPosition: BinLogPosition | undefined = components[2]
      ? {
          filename: components[2],
          offset: parseInt(components[3])
        }
      : undefined;

    const nextPosition: BinLogPosition | undefined = components[4]
      ? {
          filename: components[4],
          offset: parseInt(components[5])
        }
      : undefined;
    return {
      raw_gtid: components[1],
      replicated_position: currentPosition,
      next_position: nextPosition
    };
  }

  static fromSerialized(comparable: string): ReplicatedGTID {
    return new ReplicatedGTID(ReplicatedGTID.deserialize(comparable));
  }

  /**
   * Special case for the zero GTID which means no transactions have been executed.
   */
  static ZERO = new ReplicatedGTID({ raw_gtid: '0:0' });

  constructor(protected options: ReplicatedGTIDSpecification) {}

  /**
   * Get the BinLog position of this GTID event if it has been replicated already.
   */
  get replicatedPosition() {
    return this.options.replicated_position;
  }

  /**
   * Get the BinLog position of the next replication event
   */
  get nextPosition() {
    return this.options.next_position;
  }

  /**
   * Get the raw Global Transaction ID. This of the format `server_id:transaction_ranges`
   */
  get raw() {
    return this.options.raw_gtid;
  }

  /**
   * Transforms a GTID into a comparable string format, ensuring lexicographical
   * order aligns with the GTID's relative age. This assumes that all GTIDs
   * have the same server ID.
   *
   * @param gtid - The GTID string in the format `server_id:transaction_ranges`
   * @returns A comparable string in the format
   *   `padded_end_transaction|raw_gtid|current_binlog_filename|current_binlog_position|next_binlog_filename|next_binlog_position`
   */
  get comparable() {
    const { raw, replicatedPosition: currentPosition, nextPosition } = this;
    const [, transactionRanges] = this.raw.split(':');

    let maxTransactionId = 0;

    for (const range of transactionRanges.split(',')) {
      const [start, end] = range.split('-');
      maxTransactionId = Math.max(maxTransactionId, parseInt(start, 10), parseInt(end || start, 10));
    }

    const paddedTransactionId = maxTransactionId.toString().padStart(16, '0');
    return [
      paddedTransactionId,
      raw,
      currentPosition?.filename ?? '',
      currentPosition?.offset ?? '',
      nextPosition?.filename ?? '',
      nextPosition?.offset ?? ''
    ].join('|');
  }

  /**
   * Calculates the distance in bytes from this GTID to the provided argument.
   */
  async distanceTo(db: mysql.Pool, to: ReplicatedGTID): Promise<number | null> {
    const [logFiles] = await retriedQuery({
      db,
      query: `SHOW BINARY LOGS;`
    });

    // Default to the first file for the start to handle the zero GTID case.
    const startFileIndex = Math.max(
      logFiles.findIndex((f) => f['Log_name'] == this.replicatedPosition?.filename),
      0
    );
    const startFileEntry = logFiles[startFileIndex];

    if (!startFileEntry) {
      return null;
    }

    /**
     * Fall back to the next position for comparison if the replicated position is not present
     */
    const endPosition = to.replicatedPosition ?? to.nextPosition;

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
      (this?.replicatedPosition?.offset ?? 0) -
      endFileEntry['File_size'] +
      (endPosition?.offset ?? 0) +
      logFiles.slice(startFileIndex + 1, endFileIndex).reduce((sum, file) => sum + file['File_size'], 0)
    );
  }
}
