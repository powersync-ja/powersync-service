const CURSOR_WIDTH = 20;
const DELIMITER = '|';

type CursorMode = 'numeric' | 'opaque';

export interface ConvexLSNSpecification {
  cursor: string;
  mode: CursorMode;
  sortKey: bigint;
}

export class ConvexLSN {
  static ZERO = ConvexLSN.fromCursor('0');

  static fromSerialized(comparable: string): ConvexLSN {
    if (!comparable.includes(DELIMITER)) {
      // Bare cursor string (no encoded format). Treat as a raw cursor.
      return ConvexLSN.fromCursor(comparable);
    }

    const [first, second] = comparable.split(DELIMITER, 2);
    const prefixed = first.match(/^([no])([0-9]+)$/);
    if (prefixed && second != null) {
      const decoded = decodeCursor(second);
      return ConvexLSN.fromCursor(decoded);
    }

    // Unrecognized format: treat entire input as an opaque cursor.
    return ConvexLSN.fromCursor(comparable);
  }

  static fromCursor(cursor: string | number | bigint): ConvexLSN {
    const asString = `${cursor}`;
    if (asString.length == 0) {
      throw new Error('Convex cursor cannot be empty');
    }

    const numericSortKey = getNumericSortKey(asString);
    const mode: CursorMode = numericSortKey == null ? 'opaque' : 'numeric';

    return new ConvexLSN({
      cursor: asString,
      mode,
      sortKey: numericSortKey ?? 0n
    });
  }

  constructor(private readonly options: ConvexLSNSpecification) {}

  get timestamp() {
    return this.options.sortKey;
  }

  get cursor() {
    return this.options.cursor;
  }

  get comparable() {
    const prefix = this.options.mode == 'numeric' ? 'n' : 'o';
    const sortPart = this.options.sortKey.toString().padStart(CURSOR_WIDTH, '0');
    return `${prefix}${sortPart}${DELIMITER}${encodeCursor(this.options.cursor)}`;
  }

  toString() {
    return this.comparable;
  }

  toCursorString() {
    return this.options.cursor;
  }
}

function getNumericSortKey(cursor: string): bigint | null {
  if (/^[0-9]+$/.test(cursor)) {
    return BigInt(cursor);
  }

  return null;
}

function encodeCursor(value: string): string {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function decodeCursor(value: string): string {
  return Buffer.from(value, 'base64url').toString('utf8');
}
