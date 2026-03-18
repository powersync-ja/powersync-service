import { mongo } from '@powersync/lib-service-mongodb';
import { SqliteRow } from '@powersync/service-sync-rules';

const NESTED_DEPTH_LIMIT = 20;
const TWO_DIGITS = Array.from({ length: 100 }, (_value, index) => index.toString().padStart(2, '0'));
const THREE_DIGITS = Array.from({ length: 1000 }, (_value, index) => index.toString().padStart(3, '0'));

export function bufferToSqlite(bytes: Buffer): SqliteRow {
  const row: SqliteRow = {};
  const bodyEnd = readInt32LE(bytes, 0) - 1;
  let offset = 4;

  while (offset < bodyEnd) {
    const type = bytes[offset++];
    const { value: key, nextOffset: afterKey } = readCString(bytes, offset);
    offset = afterKey;

    switch (type) {
      case 0x07: {
        row[key] = hexLower(bytes, offset, 12);
        offset += 12;
        break;
      }
      case 0x02: {
        const length = readInt32LE(bytes, offset);
        const stringStart = offset + 4;
        row[key] = bytes.toString('utf8', stringStart, stringStart + length - 1);
        offset = stringStart + length;
        break;
      }
      case 0x04: {
        const result = serializeNestedArrayToJson(bytes, offset, 1, '');
        row[key] = result.out;
        offset = result.nextOffset;
        break;
      }
      case 0x03: {
        const result = serializeNestedObjectToJson(bytes, offset, 1, '');
        row[key] = result.out;
        offset = result.nextOffset;
        break;
      }
      case 0x08: {
        row[key] = bytes[offset++] ? 1n : 0n;
        break;
      }
      case 0x09: {
        row[key] = legacyDateTimeString(Number(bytes.readBigInt64LE(offset)));
        offset += 8;
        break;
      }
      case 0x10: {
        row[key] = BigInt(readInt32LE(bytes, offset));
        offset += 4;
        break;
      }
      case 0x11: {
        const increment = bytes.readUInt32LE(offset);
        const time = bytes.readUInt32LE(offset + 4);
        row[key] = `{"t":${time},"i":${increment}}`;
        offset += 8;
        break;
      }
      case 0x12: {
        row[key] = bytes.readBigInt64LE(offset);
        offset += 8;
        break;
      }
      case 0x13: {
        row[key] = decimal128ToString(bytes, offset);
        offset += 16;
        break;
      }
      case 0x05: {
        const { value, nextOffset } = parseTopLevelBinary(bytes, offset);
        row[key] = value;
        offset = nextOffset;
        break;
      }
      case 0x0b: {
        const { pattern, options, nextOffset } = parseRegex(bytes, offset);
        row[key] = `{"pattern":${quoteJsonFast(pattern)},"options":${quoteJsonFast(options)}}`;
        offset = nextOffset;
        break;
      }
      case 0x06:
      case 0x0a:
      case 0xff:
      case 0x7f:
        row[key] = null;
        break;
      case 0x01: {
        const value = bytes.readDoubleLE(offset);
        offset += 8;
        row[key] = Number.isInteger(value) ? BigInt(value) : value;
        break;
      }
      default: {
        row[key] = null;
        offset = skipBsonValue(bytes, offset, type);
        break;
      }
    }
  }

  return row;
}

function readInt32LE(bytes: Buffer, offset: number): number {
  return bytes.readInt32LE(offset);
}

function readCString(bytes: Buffer, offset: number): { value: string; nextOffset: number } {
  const end = bytes.indexOf(0, offset);
  if (end < 0) {
    throw new Error('Invalid BSON: missing cstring terminator');
  }
  return {
    value: bytes.toString('utf8', offset, end),
    nextOffset: end + 1
  };
}

function skipCString(bytes: Buffer, offset: number): number {
  const end = bytes.indexOf(0, offset);
  if (end < 0) {
    throw new Error('Invalid BSON: missing cstring terminator');
  }
  return end + 1;
}

function quoteJsonFast(text: string) {
  if (text.length > 64) {
    return JSON.stringify(text);
  }
  for (let i = 0; i < text.length; i++) {
    const ch = text.charCodeAt(i);
    if (ch < 0x20 || ch > 0x7e || ch === 0x22 || ch === 0x5c) {
      return JSON.stringify(text);
    }
  }
  return `"${text}"`;
}

function parseRegex(bytes: Buffer, offset: number): { pattern: string; options: string; nextOffset: number } {
  const patternEnd = bytes.indexOf(0, offset);
  const optionsEnd = bytes.indexOf(0, patternEnd + 1);
  if (patternEnd < 0 || optionsEnd < 0) {
    throw new Error('Invalid BSON regex');
  }
  const pattern = bytes.toString('utf8', offset, patternEnd);
  const optionsRaw = bytes.toString('utf8', patternEnd + 1, optionsEnd);
  return {
    pattern,
    options: regexOptionsToJsFlags(optionsRaw),
    nextOffset: optionsEnd + 1
  };
}

function decimal128ToString(bytes: Buffer, offset: number): string {
  return new mongo.Decimal128(bytes.subarray(offset, offset + 16)).toString();
}

function uuidOrHex(bytes: Buffer): string {
  const hex = bytes.toString('hex');
  if (bytes.length != 16) {
    return hex;
  }
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function parseTopLevelBinary(bytes: Buffer, offset: number): { value: Buffer | string; nextOffset: number } {
  const length = readInt32LE(bytes, offset);
  const subtype = bytes[offset + 4];
  const dataStart = offset + 5;
  const dataEnd = dataStart + length;
  const data = bytes.subarray(dataStart, dataEnd);

  if (subtype === mongo.Binary.SUBTYPE_UUID || subtype === mongo.Binary.SUBTYPE_UUID_OLD) {
    return { value: uuidOrHex(data), nextOffset: dataEnd };
  }

  return { value: data, nextOffset: dataEnd };
}

function skipBsonValue(bytes: Buffer, offset: number, type: number) {
  switch (type) {
    case 0x01:
      return offset + 8;
    case 0x02: {
      const length = readInt32LE(bytes, offset);
      return offset + 4 + length;
    }
    case 0x03:
    case 0x04:
      return offset + readInt32LE(bytes, offset);
    case 0x05: {
      const length = readInt32LE(bytes, offset);
      return offset + 4 + 1 + length;
    }
    case 0x06:
    case 0x0a:
    case 0xff:
    case 0x7f:
      return offset;
    case 0x07:
      return offset + 12;
    case 0x08:
      return offset + 1;
    case 0x09:
      return offset + 8;
    case 0x0b: {
      const patternEnd = bytes.indexOf(0, offset);
      const optionsEnd = bytes.indexOf(0, patternEnd + 1);
      if (patternEnd < 0 || optionsEnd < 0) {
        throw new Error('Invalid BSON regex');
      }
      return optionsEnd + 1;
    }
    case 0x10:
      return offset + 4;
    case 0x11:
      return offset + 8;
    case 0x12:
      return offset + 8;
    case 0x13:
      return offset + 16;
    default:
      throw new Error(`Unsupported BSON type for skip: 0x${type.toString(16)}`);
  }
}

function serializeNestedObjectToJson(
  bytes: Buffer,
  offset: number,
  depth: number,
  out: string
): { out: string; nextOffset: number } {
  if (depth > NESTED_DEPTH_LIMIT) {
    throw new Error(`json nested object depth exceeds the limit of ${NESTED_DEPTH_LIMIT}`);
  }

  const totalLength = readInt32LE(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  out += '{';
  let first = true;

  while (cursor < bodyEnd) {
    const type = bytes[cursor++];
    const { value: key, nextOffset: afterKey } = readCString(bytes, cursor);
    cursor = afterKey;

    let candidateOut = out;
    if (!first) {
      candidateOut += ',';
    }
    candidateOut += quoteJsonFast(key);
    candidateOut += ':';

    const {
      out: nextOut,
      nextOffset: afterValue,
      defined
    } = serializeNestedElementValue(bytes, cursor, type, depth, candidateOut);
    cursor = afterValue;

    if (!defined) {
      continue;
    }

    first = false;
    out = nextOut;
  }

  out += '}';
  return { out, nextOffset: offset + totalLength };
}

function serializeNestedArrayToJson(
  bytes: Buffer,
  offset: number,
  depth: number,
  out: string
): { out: string; nextOffset: number } {
  if (depth > NESTED_DEPTH_LIMIT) {
    throw new Error(`json nested object depth exceeds the limit of ${NESTED_DEPTH_LIMIT}`);
  }

  const totalLength = readInt32LE(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  out += '[';
  let first = true;

  while (cursor < bodyEnd) {
    const type = bytes[cursor++];
    cursor = skipCString(bytes, cursor);

    if (!first) {
      out += ',';
    }
    first = false;

    const {
      out: nextOut,
      nextOffset: afterValue,
      defined
    } = serializeNestedElementValue(bytes, cursor, type, depth, out);
    cursor = afterValue;

    out = defined ? nextOut : out + 'null';
  }

  out += ']';
  return { out, nextOffset: offset + totalLength };
}

function serializeNestedElementValue(
  bytes: Buffer,
  offset: number,
  type: number,
  depth: number,
  out: string
): { out: string; nextOffset: number; defined: boolean } {
  switch (type) {
    case 0x01: {
      const value = bytes.readDoubleLE(offset);
      out += Number.isInteger(value) ? Math.trunc(value).toString() : Number(value).toString();
      return { out, nextOffset: offset + 8, defined: true };
    }
    case 0x02: {
      const length = readInt32LE(bytes, offset);
      const stringStart = offset + 4;
      const text = bytes.toString('utf8', stringStart, stringStart + length - 1);
      out += quoteJsonFast(text);
      return { out, nextOffset: stringStart + length, defined: true };
    }
    case 0x03: {
      const result = serializeNestedObjectToJson(bytes, offset, depth + 1, out);
      return { out: result.out, nextOffset: result.nextOffset, defined: true };
    }
    case 0x04: {
      const result = serializeNestedArrayToJson(bytes, offset, depth + 1, out);
      return { out: result.out, nextOffset: result.nextOffset, defined: true };
    }
    case 0x05: {
      const next = skipBsonValue(bytes, offset, type);
      return { out, nextOffset: next, defined: false };
    }
    case 0x06:
      return { out, nextOffset: offset, defined: false };
    case 0x07: {
      out += quoteJsonFast(hexLower(bytes, offset, 12));
      return { out, nextOffset: offset + 12, defined: true };
    }
    case 0x08:
      out += bytes[offset] ? '1' : '0';
      return { out, nextOffset: offset + 1, defined: true };
    case 0x09: {
      const millis = Number(bytes.readBigInt64LE(offset));
      out += quoteJsonFast(legacyDateTimeString(millis));
      return { out, nextOffset: offset + 8, defined: true };
    }
    case 0x0a:
    case 0xff:
    case 0x7f:
      out += 'null';
      return { out, nextOffset: offset, defined: true };
    case 0x0b: {
      const { pattern, options, nextOffset } = parseRegex(bytes, offset);
      out += '{"pattern":';
      out += quoteJsonFast(pattern);
      out += ',"options":';
      out += quoteJsonFast(options);
      out += '}';
      return { out, nextOffset, defined: true };
    }
    case 0x10: {
      out += String(readInt32LE(bytes, offset));
      return { out, nextOffset: offset + 4, defined: true };
    }
    case 0x11: {
      const increment = bytes.readUInt32LE(offset);
      const time = bytes.readUInt32LE(offset + 4);
      out += '{"t":';
      out += String(time);
      out += ',"i":';
      out += String(increment);
      out += '}';
      return { out, nextOffset: offset + 8, defined: true };
    }
    case 0x12: {
      out += bytes.readBigInt64LE(offset).toString();
      return { out, nextOffset: offset + 8, defined: true };
    }
    case 0x13:
      out += quoteJsonFast(decimal128ToString(bytes, offset));
      return { out, nextOffset: offset + 16, defined: true };
    default:
      throw new Error(`Unsupported BSON nested type: 0x${type.toString(16)}`);
  }
}

function legacyDateTimeString(millis: number) {
  const date = new Date(millis);

  if (Number.isNaN(date.getTime())) {
    throw new RangeError('Invalid time value');
  }

  const year = date.getUTCFullYear();
  if (year < 0 || year > 9999) {
    const iso = date.toISOString();
    return `${iso.slice(0, 10)} ${iso.slice(11)}`;
  }

  return (
    String(year).padStart(4, '0') +
    '-' +
    TWO_DIGITS[date.getUTCMonth() + 1] +
    '-' +
    TWO_DIGITS[date.getUTCDate()] +
    ' ' +
    TWO_DIGITS[date.getUTCHours()] +
    ':' +
    TWO_DIGITS[date.getUTCMinutes()] +
    ':' +
    TWO_DIGITS[date.getUTCSeconds()] +
    '.' +
    THREE_DIGITS[date.getUTCMilliseconds()] +
    'Z'
  );
}

function regexOptionsToJsFlags(options: string) {
  let out = '';
  if (options.includes('s')) out += 'g';
  if (options.includes('i')) out += 'i';
  if (options.includes('m')) out += 'm';
  return out;
}

function hexLower(bytes: Buffer, offset: number, length: number): string {
  return bytes.toString('hex', offset, offset + length);
}
