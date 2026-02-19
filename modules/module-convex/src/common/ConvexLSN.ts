const CURSOR_WIDTH = 20;
const DELIMITER = '|';

export interface ConvexLSNSpecification {
  cursor: string;
  timestamp: bigint;
}

export class ConvexLSN {
  static ZERO = ConvexLSN.fromCursor('0');

  static fromSerialized(comparable: string): ConvexLSN {
    if (!comparable.includes(DELIMITER)) {
      return ConvexLSN.fromCursor(comparable);
    }

    const [first, second] = comparable.split(DELIMITER, 2);
    const prefixed = first.match(/^[no]([0-9]+)$/);
    if (prefixed && second != null) {
      const decoded = decodeCursor(second);
      return ConvexLSN.fromCursor(decoded);
    }

    return ConvexLSN.fromCursor(comparable);
  }

  static fromCursor(cursor: string | number | bigint): ConvexLSN {
    const asString = `${cursor}`;
    if (asString.length == 0) {
      throw new Error('Convex cursor cannot be empty');
    }

    const ts = parseTimestamp(asString);
    return new ConvexLSN({
      cursor: asString,
      timestamp: ts
    });
  }

  constructor(private readonly options: ConvexLSNSpecification) {}

  get timestamp() {
    return this.options.timestamp;
  }

  get cursor() {
    return this.options.cursor;
  }

  get comparable() {
    const sortPart = this.options.timestamp.toString().padStart(CURSOR_WIDTH, '0');
    return `n${sortPart}${DELIMITER}${encodeCursor(this.options.cursor)}`;
  }

  toString() {
    return this.comparable;
  }

  toCursorString() {
    return this.options.cursor;
  }
}

function parseTimestamp(cursor: string): bigint {
  if (/^[0-9]+$/.test(cursor)) {
    return BigInt(cursor);
  }
  throw new Error(`Convex cursor is not a valid numeric timestamp: ${cursor}`);
}

function encodeCursor(value: string): string {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function decodeCursor(value: string): string {
  return Buffer.from(value, 'base64url').toString('utf8');
}
