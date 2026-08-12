import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import mysql from 'mysql2/promise';
import * as uuid from 'uuid';
import * as mysql_utils from '../utils/mysql-utils.js';

export type BinLogPosition = {
  filename: string;
  offset: number;
};

export type ReplicatedGTIDSpecification = {
  /**
   * The raw Global Transaction ID. This is of the format `server_uuid:transaction_id`.
   * Must be a single GTID — not a GTID set (multiple UUIDs) or interval range.
   */
  rawGtid: string;
  /**
   * The (end) position in a BinLog file where this transaction has been replicated in.
   */
  position: BinLogPosition;
};

export type BinLogGTIDFormat = {
  serverUuid: Buffer;
  transactionId: number;
};

export type BinLogGTIDEvent = {
  rawGtid: BinLogGTIDFormat;
  position: BinLogPosition;
};

/**
 * A wrapper around the MySQL GTID value.
 * This adds and tracks additional metadata such as the BinLog filename
 * and position where this GTID could be located.
 */
export class ReplicatedGTID {
  private options: ReplicatedGTIDSpecification;

  constructor(options: ReplicatedGTIDSpecification) {
    const rawGtid = options.rawGtid.trim();
    assertSingleGtid(rawGtid);
    this.options = { ...options, rawGtid };
  }

  static fromSerialized(comparable: string): ReplicatedGTID {
    return new ReplicatedGTID(ReplicatedGTID.deserialize(comparable));
  }

  private static deserialize(comparable: string): ReplicatedGTIDSpecification {
    const components = comparable.split('|');
    if (components.length < 4) {
      throw new ReplicationAssertionError(`Invalid serialized GTID: ${comparable}`);
    }

    const offset = parseInt(components[3], 10);
    if (Number.isNaN(offset)) {
      throw new ReplicationAssertionError(`Invalid BinLog offset in serialized GTID: ${comparable}`);
    }

    return {
      rawGtid: components[1],
      position: {
        filename: components[2],
        offset: offset
      } satisfies BinLogPosition
    };
  }

  static fromBinLogEvent(event: BinLogGTIDEvent) {
    const { rawGtid, position } = event;
    const stringGTID = `${uuid.stringify(rawGtid.serverUuid)}:${rawGtid.transactionId}`;
    return new ReplicatedGTID({
      rawGtid: stringGTID,
      position
    });
  }

  /**
   * Special case for the zero GTID which means no transactions have been executed.
   */
  static ZERO(serverUuid: string): ReplicatedGTID {
    return new ReplicatedGTID({
      rawGtid: `${serverUuid}:0`,
      position: { filename: '', offset: 0 }
    });
  }

  /**
   * Get the BinLog position of this replicated GTID event
   */
  get position() {
    return this.options.position;
  }

  /**
   * Get the raw Global Transaction ID. This is of the format `server_uuid:transaction_id`
   */
  get raw() {
    return this.options.rawGtid;
  }

  /**
   * The server UUID of the server this transaction originated from
   */
  get serverUuid() {
    return this.options.rawGtid.split(':')[0];
  }

  /**
   * Transforms a GTID into a comparable string format, ensuring lexicographical
   * order aligns with the GTID's relative age. This assumes that all GTIDs
   * have the same server ID.
   *
   * @returns A comparable string in the format
   *   `padded_end_transaction|raw_gtid|binlog_filename|binlog_position`
   */
  get comparable(): string {
    const { raw, position } = this;
    const [, transactionId] = this.raw.split(':');

    const paddedTransactionId = transactionId.toString().padStart(16, '0');
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

/**
 * Asserts that the given gtid string is a single GTID of the form `server_uuid:transaction_id`,
 * not a GTID set such as `uuid:1-17` or `uuid1:1,uuid2:2`.
 */
function assertSingleGtid(gtid: string): void {
  // GTID sets join UUID sets with commas (often with newlines: `,\n`).
  if (gtid.includes(',') || gtid.includes('\n')) {
    throw new ReplicationAssertionError(`Expected a single GTID (server_uuid:transaction_id), got a GTID set: ${gtid}`);
  }

  const parts = gtid.split(':');
  if (parts.length !== 2 || parts[0].length === 0 || parts[1].length === 0) {
    throw new ReplicationAssertionError(`Expected a single GTID (server_uuid:transaction_id), got: ${gtid}`);
  }

  // Intervals use `n-m`; a single transaction id must be a non-negative integer.
  if (!/^\d+$/.test(parts[1])) {
    throw new ReplicationAssertionError(`Expected a single transaction id, got: ${gtid}`);
  }
}
