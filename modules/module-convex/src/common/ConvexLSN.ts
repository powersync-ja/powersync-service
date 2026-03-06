export const ZERO_LSN = '0';

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

  if (cursor.length > 1 && cursor.startsWith('0')) {
    throw new Error(`Convex cursor is not a canonical numeric timestamp: ${cursor}`);
  }
}
