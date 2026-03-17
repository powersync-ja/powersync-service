export const ZERO_LSN = '0';
// Convex replication cursors are fixed-width decimal timestamps.
const CONVEX_TIMESTAMP_DIGITS = 19;

export function parseConvexLsn(lsn: string): string {
  return toConvexLsn(lsn);
}

export function toConvexLsn(cursor: string | bigint): string {
  const asString = `${cursor}`;
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
