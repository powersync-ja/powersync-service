import { mongo } from '@powersync/lib-service-mongodb';
import { SqliteRow } from '@powersync/service-sync-rules';

const NESTED_DEPTH_LIMIT = 20;
const JSON_BUFFER_INITIAL_CAPACITY = 1024 * 1024;
const TWO_DIGITS = Array.from({ length: 100 }, (_value, index) => index.toString().padStart(2, '0'));
const THREE_DIGITS = Array.from({ length: 1000 }, (_value, index) => index.toString().padStart(3, '0'));
const SHARED_UTC_DATE = new Date(0);
const HEX_LOWER_BYTES = Buffer.from('0123456789abcdef', 'ascii');

class JsonBufferWriter {
  private buffer: Buffer;
  private length = 0;

  constructor(capacity = JSON_BUFFER_INITIAL_CAPACITY) {
    this.buffer = Buffer.allocUnsafe(capacity);
  }

  reset() {
    this.length = 0;
  }

  toString() {
    return this.buffer.toString('utf8', 0, this.length);
  }

  writeByte(value: number) {
    this.ensureCapacity(1);
    this.buffer[this.length++] = value;
  }

  writeAscii(text: string) {
    const length = text.length;
    this.ensureCapacity(length);
    this.buffer.write(text, this.length, length, 'ascii');
    this.length += length;
  }

  writeUtf8(text: string) {
    const length = Buffer.byteLength(text);
    this.ensureCapacity(length);
    this.buffer.write(text, this.length, length, 'utf8');
    this.length += length;
  }

  writeBufferSlice(bytes: Buffer, start: number, end: number) {
    const length = end - start;
    this.ensureCapacity(length);
    bytes.copy(this.buffer, this.length, start, end);
    this.length += length;
  }

  writeQuotedJsonString(text: string) {
    this.writeByte(0x22);
    let start = 0;

    for (let i = 0; i < text.length; i++) {
      const ch = text.charCodeAt(i);
      let escaped: string | undefined;

      switch (ch) {
        case 0x22:
          escaped = '\\"';
          break;
        case 0x5c:
          escaped = '\\\\';
          break;
        case 0x08:
          escaped = '\\b';
          break;
        case 0x09:
          escaped = '\\t';
          break;
        case 0x0a:
          escaped = '\\n';
          break;
        case 0x0c:
          escaped = '\\f';
          break;
        case 0x0d:
          escaped = '\\r';
          break;
        default:
          if (ch < 0x20) {
            escaped = `\\u${ch.toString(16).padStart(4, '0')}`;
          }
      }

      if (escaped == null) {
        continue;
      }

      if (start < i) {
        this.writeUtf8(text.slice(start, i));
      }
      this.writeAscii(escaped);
      start = i + 1;
    }

    if (start < text.length) {
      this.writeUtf8(text.slice(start));
    }

    this.writeByte(0x22);
  }

  writeQuotedUtf8Slice(bytes: Buffer, start: number, end: number) {
    let firstEscape = -1;
    for (let index = start; index < end; index++) {
      const value = bytes[index];
      if (value < 0x20 || value === 0x22 || value === 0x5c) {
        firstEscape = index;
        break;
      }
    }

    const rawLength = end - start;
    this.ensureCapacity(rawLength + 2);
    let buffer = this.buffer;
    let length = this.length;

    buffer[length++] = 0x22;

    if (firstEscape < 0) {
      bytes.copy(buffer, length, start, end);
      length += rawLength;
      buffer[length++] = 0x22;
      this.length = length;
      return;
    }

    if (firstEscape > start) {
      bytes.copy(buffer, length, start, firstEscape);
      length += firstEscape - start;
    }

    let chunkStart = firstEscape;
    for (let index = firstEscape; index < end; index++) {
      const value = bytes[index];
      if (value >= 0x20 && value !== 0x22 && value !== 0x5c) {
        continue;
      }

      if (chunkStart < index) {
        const chunkLength = index - chunkStart;
        this.ensureCapacity(chunkLength + 6);
        buffer = this.buffer;
        bytes.copy(buffer, length, chunkStart, index);
        length += chunkLength;
      } else {
        this.ensureCapacity(6);
        buffer = this.buffer;
      }

      switch (value) {
        case 0x22:
          buffer[length++] = 0x5c;
          buffer[length++] = 0x22;
          break;
        case 0x5c:
          buffer[length++] = 0x5c;
          buffer[length++] = 0x5c;
          break;
        case 0x08:
          buffer[length++] = 0x5c;
          buffer[length++] = 0x62;
          break;
        case 0x09:
          buffer[length++] = 0x5c;
          buffer[length++] = 0x74;
          break;
        case 0x0a:
          buffer[length++] = 0x5c;
          buffer[length++] = 0x6e;
          break;
        case 0x0c:
          buffer[length++] = 0x5c;
          buffer[length++] = 0x66;
          break;
        case 0x0d:
          buffer[length++] = 0x5c;
          buffer[length++] = 0x72;
          break;
        default:
          buffer[length++] = 0x5c;
          buffer[length++] = 0x75;
          buffer[length++] = 0x30;
          buffer[length++] = 0x30;
          buffer[length++] = HEX_LOWER_BYTES[value >> 4];
          buffer[length++] = HEX_LOWER_BYTES[value & 0x0f];
          break;
      }

      chunkStart = index + 1;
    }

    if (chunkStart < end) {
      const chunkLength = end - chunkStart;
      this.ensureCapacity(chunkLength + 1);
      buffer = this.buffer;
      bytes.copy(buffer, length, chunkStart, end);
      length += chunkLength;
    } else {
      this.ensureCapacity(1);
      buffer = this.buffer;
    }

    buffer[length++] = 0x22;
    this.length = length;
  }

  writeQuotedHexLower(bytes: Buffer, start: number, length: number) {
    this.ensureCapacity(length * 2 + 2);
    this.buffer[this.length++] = 0x22;
    for (let index = start; index < start + length; index++) {
      const value = bytes[index];
      this.buffer[this.length++] = HEX_LOWER_BYTES[value >> 4];
      this.buffer[this.length++] = HEX_LOWER_BYTES[value & 0x0f];
    }
    this.buffer[this.length++] = 0x22;
  }

  private ensureCapacity(extra: number) {
    const required = this.length + extra;
    if (required <= this.buffer.length) {
      return;
    }

    let nextLength = this.buffer.length;
    while (nextLength < required) {
      nextLength *= 2;
    }

    const next = Buffer.allocUnsafe(nextLength);
    this.buffer.copy(next, 0, 0, this.length);
    this.buffer = next;
  }

  writeLegacyDateTime(
    year: number,
    month: number,
    day: number,
    hour: number,
    minute: number,
    second: number,
    millisecond: number
  ) {
    this.ensureCapacity(26);
    const buffer = this.buffer;
    let offset = this.length;

    buffer[offset++] = 0x22;
    buffer[offset++] = 0x30 + ((year / 1000) | 0);
    buffer[offset++] = 0x30 + (((year / 100) | 0) % 10);
    buffer[offset++] = 0x30 + (((year / 10) | 0) % 10);
    buffer[offset++] = 0x30 + (year % 10);
    buffer[offset++] = 0x2d;
    buffer[offset++] = 0x30 + ((month / 10) | 0);
    buffer[offset++] = 0x30 + (month % 10);
    buffer[offset++] = 0x2d;
    buffer[offset++] = 0x30 + ((day / 10) | 0);
    buffer[offset++] = 0x30 + (day % 10);
    buffer[offset++] = 0x20;
    buffer[offset++] = 0x30 + ((hour / 10) | 0);
    buffer[offset++] = 0x30 + (hour % 10);
    buffer[offset++] = 0x3a;
    buffer[offset++] = 0x30 + ((minute / 10) | 0);
    buffer[offset++] = 0x30 + (minute % 10);
    buffer[offset++] = 0x3a;
    buffer[offset++] = 0x30 + ((second / 10) | 0);
    buffer[offset++] = 0x30 + (second % 10);
    buffer[offset++] = 0x2e;
    buffer[offset++] = 0x30 + ((millisecond / 100) | 0);
    buffer[offset++] = 0x30 + (((millisecond / 10) | 0) % 10);
    buffer[offset++] = 0x30 + (millisecond % 10);
    buffer[offset++] = 0x5a;
    buffer[offset++] = 0x22;

    this.length = offset;
  }
}

const SHARED_WRITER = new JsonBufferWriter(1024 * 1024);

export function bufferToSqlite(bytes: Buffer): SqliteRow {
  const row: SqliteRow = {};
  const jsonWriter = SHARED_WRITER;
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
        jsonWriter.reset();
        const result = serializeNestedArrayToJson(bytes, offset, 1, jsonWriter);
        row[key] = jsonWriter.toString();
        offset = result.nextOffset;
        break;
      }
      case 0x03: {
        jsonWriter.reset();
        const result = serializeNestedObjectToJson(bytes, offset, 1, jsonWriter);
        row[key] = jsonWriter.toString();
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
        jsonWriter.reset();
        jsonWriter.writeAscii('{"pattern":');
        jsonWriter.writeQuotedJsonString(pattern);
        jsonWriter.writeAscii(',"options":');
        jsonWriter.writeQuotedJsonString(options);
        jsonWriter.writeByte(0x7d);
        row[key] = jsonWriter.toString();
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
  writer: JsonBufferWriter
): { nextOffset: number } {
  if (depth > NESTED_DEPTH_LIMIT) {
    throw new Error(`json nested object depth exceeds the limit of ${NESTED_DEPTH_LIMIT}`);
  }

  const totalLength = readInt32LE(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  writer.writeByte(0x7b);
  let first = true;

  while (cursor < bodyEnd) {
    const type = bytes[cursor++];
    const keyEnd = bytes.indexOf(0, cursor);
    if (keyEnd < 0) {
      throw new Error('Invalid BSON: missing cstring terminator');
    }
    if (!first) {
      writer.writeByte(0x2c);
    }
    writer.writeQuotedUtf8Slice(bytes, cursor, keyEnd);
    writer.writeByte(0x3a);
    cursor = keyEnd + 1;

    const { nextOffset: afterValue, defined } = serializeNestedElementValue(bytes, cursor, type, depth, writer);
    cursor = afterValue;

    if (!defined) {
      continue;
    }

    first = false;
  }

  writer.writeByte(0x7d);
  return { nextOffset: offset + totalLength };
}

function serializeNestedArrayToJson(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter
): { nextOffset: number } {
  if (depth > NESTED_DEPTH_LIMIT) {
    throw new Error(`json nested object depth exceeds the limit of ${NESTED_DEPTH_LIMIT}`);
  }

  const totalLength = readInt32LE(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  writer.writeByte(0x5b);
  let first = true;

  while (cursor < bodyEnd) {
    const type = bytes[cursor++];
    cursor = skipCString(bytes, cursor);

    if (!first) {
      writer.writeByte(0x2c);
    }
    first = false;

    const { nextOffset: afterValue, defined } = serializeNestedElementValue(bytes, cursor, type, depth, writer);
    cursor = afterValue;

    if (!defined) {
      writer.writeAscii('null');
    }
  }

  writer.writeByte(0x5d);
  return { nextOffset: offset + totalLength };
}

function serializeNestedElementValue(
  bytes: Buffer,
  offset: number,
  type: number,
  depth: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  switch (type) {
    case 0x01:
      return serializeNestedDoubleElement(bytes, offset, writer);
    case 0x02:
      return serializeNestedStringElement(bytes, offset, writer);
    case 0x03:
      return serializeNestedObjectElement(bytes, offset, depth, writer);
    case 0x04:
      return serializeNestedArrayElement(bytes, offset, depth, writer);
    case 0x05: {
      const next = skipBsonValue(bytes, offset, type);
      return { nextOffset: next, defined: false };
    }
    case 0x06:
      return { nextOffset: offset, defined: false };
    case 0x07: {
      writer.writeQuotedHexLower(bytes, offset, 12);
      return { nextOffset: offset + 12, defined: true };
    }
    case 0x08:
      writer.writeByte(bytes[offset] ? 0x31 : 0x30);
      return { nextOffset: offset + 1, defined: true };
    case 0x09:
      return serializeNestedDateTimeElement(bytes, offset, writer);
    case 0x0a:
    case 0xff:
    case 0x7f:
      writer.writeAscii('null');
      return { nextOffset: offset, defined: true };
    case 0x0b:
      return serializeNestedRegexElement(bytes, offset, writer);
    case 0x10: {
      writer.writeAscii(String(readInt32LE(bytes, offset)));
      return { nextOffset: offset + 4, defined: true };
    }
    case 0x11: {
      const increment = bytes.readUInt32LE(offset);
      const time = bytes.readUInt32LE(offset + 4);
      writer.writeAscii('{"t":');
      writer.writeAscii(String(time));
      writer.writeAscii(',"i":');
      writer.writeAscii(String(increment));
      writer.writeByte(0x7d);
      return { nextOffset: offset + 8, defined: true };
    }
    case 0x12: {
      writer.writeAscii(bytes.readBigInt64LE(offset).toString());
      return { nextOffset: offset + 8, defined: true };
    }
    case 0x13:
      writer.writeQuotedJsonString(decimal128ToString(bytes, offset));
      return { nextOffset: offset + 16, defined: true };
    default:
      throw new Error(`Unsupported BSON nested type: 0x${type.toString(16)}`);
  }
}

function serializeNestedDoubleElement(
  bytes: Buffer,
  offset: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  const value = bytes.readDoubleLE(offset);
  writer.writeAscii(value.toString());
  return { nextOffset: offset + 8, defined: true };
}

function serializeNestedStringElement(
  bytes: Buffer,
  offset: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  const length = readInt32LE(bytes, offset);
  const stringStart = offset + 4;
  writer.writeQuotedUtf8Slice(bytes, stringStart, stringStart + length - 1);
  return { nextOffset: stringStart + length, defined: true };
}

function serializeNestedObjectElement(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  const result = serializeNestedObjectToJson(bytes, offset, depth + 1, writer);
  return { nextOffset: result.nextOffset, defined: true };
}

function serializeNestedArrayElement(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  const result = serializeNestedArrayToJson(bytes, offset, depth + 1, writer);
  return { nextOffset: result.nextOffset, defined: true };
}

function serializeNestedDateTimeElement(
  bytes: Buffer,
  offset: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  appendLegacyDateTimeToWriter(writer, Number(bytes.readBigInt64LE(offset)));
  return { nextOffset: offset + 8, defined: true };
}

function serializeNestedRegexElement(
  bytes: Buffer,
  offset: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  const { pattern, options, nextOffset } = parseRegex(bytes, offset);
  writer.writeAscii('{"pattern":');
  writer.writeQuotedJsonString(pattern);
  writer.writeAscii(',"options":');
  writer.writeQuotedJsonString(options);
  writer.writeByte(0x7d);
  return { nextOffset, defined: true };
}

function legacyDateTimeString(millis: number) {
  SHARED_UTC_DATE.setTime(millis);
  const date = SHARED_UTC_DATE;

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

function appendLegacyDateTimeToWriter(writer: JsonBufferWriter, millis: number) {
  SHARED_UTC_DATE.setTime(millis);
  const date = SHARED_UTC_DATE;

  if (Number.isNaN(date.getTime())) {
    throw new RangeError('Invalid time value');
  }

  const year = date.getUTCFullYear();
  if (year < 0 || year > 9999) {
    writer.writeQuotedJsonString(legacyDateTimeString(millis));
    return;
  }

  writer.writeLegacyDateTime(
    year,
    date.getUTCMonth() + 1,
    date.getUTCDate(),
    date.getUTCHours(),
    date.getUTCMinutes(),
    date.getUTCSeconds(),
    date.getUTCMilliseconds()
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
