export class ConvexLSN {
  static ZERO = ConvexLSN.fromCursor('0');

  static fromSerialized(comparable: string): ConvexLSN {
    return ConvexLSN.fromCursor(comparable);
  }

  static fromCursor(cursor: string | bigint): ConvexLSN {
    const asString = `${cursor}`;
    assertValidCursor(asString);
    return new ConvexLSN(asString);
  }

  private constructor(private readonly value: string) {}

  get cursor() {
    return this.value;
  }

  get comparable() {
    return this.value;
  }

  toString() {
    return this.comparable;
  }

  toCursorString() {
    return this.value;
  }
}

function assertValidCursor(cursor: string) {
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
