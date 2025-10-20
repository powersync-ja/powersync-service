import { ReplicationAssertionError } from '@powersync/service-errors';

/**
 *  Helper class for interpreting and manipulating SQL Server Log Sequence Numbers (LSNs).
 *  In SQL Server, an LSN is stored as a 10-byte binary value.
 *  But it is commonly represented in a human-readable format as three hexadecimal parts separated by colons:
 *  `00000000:00000000:0000`.
 *
 *  The three parts represent different hierarchical levels of the transaction log:
 *  1. The first part identifies the Virtual Log File (VLF).
 *  2. The second part points to the log block within the VLF.
 *  3. The third part specifies the exact log record within the log block.
 */

export class LSN {
  /**
   *  The zero or null LSN value. All other LSN values are greater than this.
   */
  static ZERO = '00000000:00000000:0000';

  protected value: string;

  private constructor(lsn: string) {
    this.value = lsn;
  }

  /**
   *  Converts this LSN back into its raw 10-byte binary representation for use in SQL Server functions.
   */
  toBinary(): Buffer {
    let sanitized: string = this.value.replace(/:/g, '');
    return Buffer.from(sanitized, 'hex');
  }

  /**
   *  Converts a raw 10-byte binary LSN value into its string representation.
   *  An error is thrown if the binary value is not exactly 10 bytes.
   *  @param rawLSN
   */
  static fromBinary(rawLSN: Buffer): LSN {
    if (rawLSN.length !== 10) {
      throw new ReplicationAssertionError(`LSN must be 10 bytes, got ${rawLSN.length}`);
    }
    const hex = rawLSN.toString('hex').toUpperCase(); // 20 hex chars

    return new LSN(`${hex.slice(0, 8)}:${hex.slice(8, 16)}:${hex.slice(16, 20)}`);
  }

  /**
   *  Creates an LSN instance from the provided string representation. An error is thrown if the format is invalid.
   *  @param stringLSN
   */
  static fromString(stringLSN: string): LSN {
    if (!/^[0-9A-Fa-f]{8}:[0-9A-Fa-f]{8}:[0-9A-Fa-f]{4}$/.test(stringLSN)) {
      throw new ReplicationAssertionError(
        `Invalid LSN string. Expected format is [00000000:00000000:0000]. Got: ${stringLSN}`
      );
    }

    return new LSN(stringLSN);
  }

  compare(other: LSN): -1 | 0 | 1 {
    if (this.value === other.value) {
      return 0;
    }
    return this.value < other.value ? -1 : 1;
  }

  valueOf(): string {
    return this.value;
  }

  toString(): string {
    return this.value;
  }
}
