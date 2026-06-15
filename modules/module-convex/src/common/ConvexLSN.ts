// Convex replication cursors are fixed-width decimal timestamps.
const CONVEX_TIMESTAMP_DIGITS = 19;

export const ZERO_LSN = '0';

/**
 * Converts the decimal timestamp LSN to a JS Date.
 */
export function lsnCursorToDate(cursor: bigint): Date {
  return new Date(Number(cursor / 1_000_000n));
}

/**
 * Parse and validate an incoming LSN.
 * Sources for input are usually the `list-snapshot` response - which returns a bigint
 * or the current stored LSN - which is stored as a string.
 */
export function parseConvexLsn(input: string | bigint): string {
  return toConvexLsn(input);
}

export function toConvexLsn(input: string | bigint): string {
  const asString = `${input}`;
  assertValidConvexLsn(asString);
  return asString;
}

function assertValidConvexLsn(cursor: string) {
  if (cursor.length == 0) {
    throw new Error('Convex cursor cannot be empty');
  }

  if (!/^[0-9]+$/.test(cursor)) {
    throw new Error(`Convex cursor is not a valid numeric timestamp: ${cursor}`);
  }

  if (cursor == ZERO_LSN) {
    return;
  }

  if (cursor.startsWith('0')) {
    throw new Error(`Convex cursor is not a canonical numeric timestamp: ${cursor}`);
  }

  if (cursor.length != CONVEX_TIMESTAMP_DIGITS) {
    throw new Error(`Convex cursor is not a 19-digit numeric timestamp: ${cursor}`);
  }
}
