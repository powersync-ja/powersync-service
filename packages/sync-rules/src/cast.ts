import type { SqliteJsonRow, SqliteValue } from './types.js';

/**
 * Extracts and normalizes the ID column from a row.
 */
export function idFromData(data: SqliteJsonRow): string {
  let id = data.id;
  if (typeof id != 'string') {
    // While an explicit cast would be better, this covers against very common
    // issues when initially testing out sync, for example when the id column is an
    // auto-incrementing integer.
    // If there is no id column, we use a blank id. This will result in the user syncing
    // a single arbitrary row for this table - better than just not being able to sync
    // anything.
    id = castAsText(id) ?? '';
  }
  return id;
}

export const CAST_TYPES = new Set<String>(['text', 'numeric', 'integer', 'real', 'blob']);

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function castAsText(value: SqliteValue): string | null {
  if (value == null) {
    return null;
  } else if (value instanceof Uint8Array) {
    return textDecoder.decode(value);
  } else {
    return value.toString();
  }
}

export function castAsBlob(value: SqliteValue): Uint8Array | null {
  if (value == null) {
    return null;
  } else if (value instanceof Uint8Array) {
    return value;
  }

  if (typeof value != 'string') {
    value = value.toString();
  }
  return textEncoder.encode(value);
}

export function cast(value: SqliteValue, to: string) {
  if (value == null) {
    return null;
  }
  if (to == 'text') {
    return castAsText(value);
  } else if (to == 'numeric') {
    if (value instanceof Uint8Array) {
      value = textDecoder.decode(value);
    }
    if (typeof value == 'string') {
      return parseNumeric(value);
    } else if (typeof value == 'number' || typeof value == 'bigint') {
      return value;
    } else {
      return 0n;
    }
  } else if (to == 'real') {
    if (value instanceof Uint8Array) {
      value = textDecoder.decode(value);
    }
    if (typeof value == 'string') {
      const nr = parseFloat(value);
      if (isNaN(nr)) {
        return 0.0;
      } else {
        return nr;
      }
    } else if (typeof value == 'number') {
      return value;
    } else if (typeof value == 'bigint') {
      return Number(value);
    } else {
      return 0.0;
    }
  } else if (to == 'integer') {
    if (value instanceof Uint8Array) {
      value = textDecoder.decode(value);
    }
    if (typeof value == 'string') {
      return parseBigInt(value);
    } else if (typeof value == 'number') {
      // SQLite truncates a REAL towards zero when casting to INTEGER (e.g. CAST(-3.7 AS INTEGER) is -3),
      // so use Math.trunc rather than Math.floor, which would round -3.7 down to -4.
      return Number.isInteger(value) ? BigInt(value) : BigInt(Math.trunc(value));
    } else if (typeof value == 'bigint') {
      return value;
    } else {
      return 0n;
    }
  } else if (to == 'blob') {
    return castAsBlob(value);
  } else {
    throw new Error(`Type not supported for cast: '${to}'`);
  }
}

function parseNumeric(text: string): bigint | number {
  const match = /^\s*([+\-]?)(?:(\d+)(\.\d*)?|\.(\d+))(e[+\-]?\d+)?/i.exec(text);
  if (!match) {
    return 0n;
  }

  if (match[3] != null || match[4] != null || match[5] != null) {
    const v = parseFloat(match[0]);
    return isNaN(v) ? 0n : v;
  } else {
    const sign = match[1] == '-' ? '-' : '';
    return BigInt(`${sign}${match[2]}`);
  }
}

function parseBigInt(text: string): bigint {
  const match = /^\s*([+\-]?)(\d+)/.exec(text);
  if (!match) {
    return 0n;
  }
  const sign = match[1] == '-' ? '-' : '';
  return BigInt(`${sign}${match[2]}`);
}
