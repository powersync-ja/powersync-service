const CURSOR_WIDTH = 20;

export class ConvexLSN {
  static ZERO = ConvexLSN.fromCursor('0');

  static fromSerialized(comparable: string): ConvexLSN {
    return ConvexLSN.fromCursor(parseSerializedCursor(comparable));
  }

  static fromCursor(cursor: string | bigint): ConvexLSN {
    const asString = `${cursor}`;
    assertValidCursor(asString);
    return new ConvexLSN(normalizeCursor(asString));
  }

  private constructor(private readonly value: string) {}

  get cursor() {
    return this.value;
  }

  get comparable() {
    return this.value.padStart(CURSOR_WIDTH, '0');
  }

  toString() {
    return this.comparable;
  }

  toCursorString() {
    return this.value;
  }
}

function parseSerializedCursor(comparable: string): string {
  assertValidCursor(comparable);
  return normalizeCursor(comparable);
}

function assertValidCursor(cursor: string) {
  if (cursor.length == 0) {
    throw new Error('Convex cursor cannot be empty');
  }

  if (!/^[0-9]+$/.test(cursor)) {
    throw new Error(`Convex cursor is not a valid numeric timestamp: ${cursor}`);
  }
}

function normalizeCursor(cursor: string): string {
  const normalized = cursor.replace(/^0+/, '');
  return normalized == '' ? '0' : normalized;
}
