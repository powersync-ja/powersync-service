import { mongo } from '@powersync/lib-service-mongodb';
import { SqliteRow } from '@powersync/service-sync-rules';
import { JsonBufferWriter } from './JsonBufferWriter.js';

const NESTED_DEPTH_LIMIT = 20;
const TWO_DIGITS = Array.from({ length: 100 }, (_value, index) => index.toString().padStart(2, '0'));
const THREE_DIGITS = Array.from({ length: 1000 }, (_value, index) => index.toString().padStart(3, '0'));
const SHARED_UTC_DATE = new Date(0);

const SHARED_WRITER = new JsonBufferWriter(1024 * 1024);

export function bufferToSqlite(bytes: Buffer): SqliteRow {
  const row: SqliteRow = {};
  const jsonWriter = SHARED_WRITER;
  // BSON documents are length-prefixed and null-terminated. We parse directly
  // from raw bytes, so structural validation happens here rather than in the
  // upstream BSON decoder.
  const bodyEnd = readDocumentLength(bytes, 0) - 1;
  let offset = 4;

  while (offset < bodyEnd) {
    const previousOffset = offset;
    const type = bytes[offset++];
    const { value: key, nextOffset: afterKey } = readCString(bytes, offset);
    offset = afterKey;

    switch (type) {
      case 0x07: {
        // ObjectId
        row[key] = hexLower(bytes, offset, 12);
        offset += 12;
        break;
      }
      case 0x02: {
        // String
        const { value, nextOffset } = readBsonString(bytes, offset);
        row[key] = value;
        offset = nextOffset;
        break;
      }
      case 0x04: {
        // Array
        jsonWriter.reset();
        const result = serializeNestedArrayToJson(bytes, offset, 0, jsonWriter);
        row[key] = jsonWriter.toString();
        offset = result.nextOffset;
        break;
      }
      case 0x03: {
        // Embedded document
        jsonWriter.reset();
        const result = serializeNestedObjectToJson(bytes, offset, 0, jsonWriter);
        row[key] = jsonWriter.toString();
        offset = result.nextOffset;
        break;
      }
      case 0x08: {
        // Boolean
        row[key] = bytes[offset++] ? 1n : 0n;
        break;
      }
      case 0x09: {
        // UTC datetime
        row[key] = legacyDateTimeString(Number(bytes.readBigInt64LE(offset)));
        offset += 8;
        break;
      }
      case 0x10: {
        // Int32
        row[key] = BigInt(readInt32LE(bytes, offset));
        offset += 4;
        break;
      }
      case 0x11: {
        // Timestamp
        row[key] = timestampToBigInt(bytes, offset);
        offset += 8;
        break;
      }
      case 0x12: {
        // Int64
        row[key] = bytes.readBigInt64LE(offset);
        offset += 8;
        break;
      }
      case 0x13: {
        // Decimal128
        row[key] = decimal128ToString(bytes, offset);
        offset += 16;
        break;
      }
      case 0x05: {
        // Binary
        const { value, nextOffset } = parseTopLevelBinary(bytes, offset);
        row[key] = value;
        offset = nextOffset;
        break;
      }
      case 0x0b: {
        // Regular expression
        const { pattern, options, nextOffset } = parseRegex(bytes, offset);
        jsonWriter.reset();
        writeRegexJson(jsonWriter, pattern, options);
        row[key] = jsonWriter.toString();
        offset = nextOffset;
        break;
      }
      case 0x0c: {
        // DBPointer
        jsonWriter.reset();
        const nextOffset = writeDbPointerJson(bytes, offset, jsonWriter);
        row[key] = jsonWriter.toString();
        offset = nextOffset;
        break;
      }
      case 0x0d: {
        // JavaScript code
        jsonWriter.reset();
        const nextOffset = writeCodeJson(bytes, offset, 0, jsonWriter);
        row[key] = jsonWriter.toString();
        offset = nextOffset;
        break;
      }
      case 0x0e: {
        // Symbol
        const { value, nextOffset } = readBsonString(bytes, offset);
        row[key] = value;
        offset = nextOffset;
        break;
      }
      case 0x0f: {
        // JavaScript code with scope
        jsonWriter.reset();
        const nextOffset = writeCodeWithScopeJson(bytes, offset, 0, jsonWriter);
        row[key] = jsonWriter.toString();
        offset = nextOffset;
        break;
      }
      case 0x06: // Undefined
      case 0x0a: // Null
      case 0xff: // MinKey
      case 0x7f: // MaxKey
        row[key] = null;
        break;
      case 0x01: {
        // Double
        const value = bytes.readDoubleLE(offset);
        offset += 8;
        // Match the default path: integral doubles are widened to bigint.
        row[key] = Number.isInteger(value) ? BigInt(value) : value;
        break;
      }
      default: {
        // Unknown top-level types are treated as null for parity with the
        // default converter, but we still advance through the raw bytes.
        row[key] = null;
        offset = skipBsonValue(bytes, offset, type);
        break;
      }
    }

    assertAdvanced(previousOffset, offset);
  }

  return row;
}

function readInt32LE(bytes: Buffer, offset: number): number {
  return bytes.readInt32LE(offset);
}

function readDocumentLength(bytes: Buffer, offset: number): number {
  const length = readInt32LE(bytes, offset);
  if (length < 5 || offset + length > bytes.length) {
    throw new Error('Invalid BSON document length');
  }
  if (bytes[offset + length - 1] !== 0) {
    throw new Error('Invalid BSON document terminator');
  }
  return length;
}

function readBsonString(bytes: Buffer, offset: number): { value: string; nextOffset: number } {
  const length = readInt32LE(bytes, offset);
  const stringStart = offset + 4;
  const stringEnd = stringStart + length;
  if (length < 1 || stringEnd > bytes.length) {
    throw new Error('Invalid BSON string length');
  }
  if (bytes[stringEnd - 1] !== 0) {
    throw new Error('Invalid BSON string terminator');
  }
  return {
    value: bytes.toString('utf8', stringStart, stringEnd - 1),
    nextOffset: stringEnd
  };
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
  return {
    pattern,
    // Preserve the raw BSON option string exactly as encoded. The default path
    // normalizes via JS RegExp semantics, but the custom path intentionally
    // keeps the BSON flags verbatim.
    options: bytes.toString('utf8', patternEnd + 1, optionsEnd),
    nextOffset: optionsEnd + 1
  };
}

function decimal128ToString(bytes: Buffer, offset: number): string {
  return new mongo.Decimal128(bytes.subarray(offset, offset + 16)).toString();
}

function timestampToBigInt(bytes: Buffer, offset: number): bigint {
  return (BigInt(bytes.readUInt32LE(offset + 4)) << 32n) | BigInt(bytes.readUInt32LE(offset));
}

/**
 *
 * @param bytes must be exactly 16 bytes in length - check before calling this.
 * @returns
 */
function uuidToString(bytes: Buffer): string {
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function parseTopLevelBinary(bytes: Buffer, offset: number): { value: Buffer | string; nextOffset: number } {
  const length = readInt32LE(bytes, offset);
  if (length < 0) {
    throw new Error('Invalid BSON binary length');
  }
  const subtype = bytes[offset + 4];
  const dataStart = offset + 5;
  const dataEnd = dataStart + length;
  if (dataEnd > bytes.length) {
    throw new Error('Invalid BSON binary length');
  }
  const data = binaryDataSlice(bytes, dataStart, dataEnd, subtype);

  // Only subtype 4 UUIDs are surfaced as strings. All other binary subtypes
  // stay as raw bytes at the top level.
  if (subtype === mongo.Binary.SUBTYPE_UUID && data.length === 16) {
    return { value: uuidToString(data), nextOffset: dataEnd };
  }

  return { value: data, nextOffset: dataEnd };
}

function binaryDataSlice(bytes: Buffer, dataStart: number, dataEnd: number, subtype: number) {
  if (subtype !== mongo.Binary.SUBTYPE_BYTE_ARRAY) {
    return bytes.subarray(dataStart, dataEnd);
  }

  // Legacy subtype 2 embeds its own nested length before the actual bytes.
  const legacyLength = readInt32LE(bytes, dataStart);
  const legacyStart = dataStart + 4;
  if (legacyLength < 0 || legacyStart + legacyLength > dataEnd) {
    throw new Error('Invalid BSON legacy binary length');
  }
  return bytes.subarray(legacyStart, legacyStart + legacyLength);
}

function skipBsonValue(bytes: Buffer, offset: number, type: number) {
  switch (type) {
    case 0x01: // Double
      return offset + 8;
    case 0x02: {
      // String
      const length = readInt32LE(bytes, offset);
      return offset + 4 + length;
    }
    case 0x03: // Embedded document
    case 0x04: // Array
      return offset + readInt32LE(bytes, offset);
    case 0x05: {
      // Binary
      const length = readInt32LE(bytes, offset);
      return offset + 4 + 1 + length;
    }
    case 0x06: // Undefined
    case 0x0a: // Null
    case 0xff: // MinKey
    case 0x7f: // MaxKey
      return offset;
    case 0x07: // ObjectId
      return offset + 12;
    case 0x08: // Boolean
      return offset + 1;
    case 0x09: // UTC datetime
      return offset + 8;
    case 0x0b: {
      // Regular expression
      const patternEnd = bytes.indexOf(0, offset);
      const optionsEnd = bytes.indexOf(0, patternEnd + 1);
      if (patternEnd < 0 || optionsEnd < 0) {
        throw new Error('Invalid BSON regex');
      }
      return optionsEnd + 1;
    }
    case 0x0c: {
      // DBPointer
      const { nextOffset } = readBsonString(bytes, offset);
      return nextOffset + 12;
    }
    case 0x0d: {
      // JavaScript code
      return readBsonString(bytes, offset).nextOffset;
    }
    case 0x0e: {
      // Symbol
      return readBsonString(bytes, offset).nextOffset;
    }
    case 0x0f: {
      // JavaScript code with scope
      const length = readInt32LE(bytes, offset);
      return offset + length;
    }
    case 0x10: // Int32
      return offset + 4;
    case 0x11: // Timestamp
      return offset + 8;
    case 0x12: // Int64
      return offset + 8;
    case 0x13: // Decimal128
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

  const totalLength = readDocumentLength(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  writer.writeByte(0x7b);
  let first = true;

  while (cursor < bodyEnd) {
    const previousCursor = cursor;
    const type = bytes[cursor++];
    const keyEnd = bytes.indexOf(0, cursor);
    if (keyEnd < 0) {
      throw new Error('Invalid BSON: missing cstring terminator');
    }
    const writerOffset = writer.getLength();
    if (!first) {
      writer.writeByte(0x2c);
    }
    writer.writeQuotedUtf8Slice(bytes, cursor, keyEnd);
    writer.writeByte(0x3a);
    cursor = keyEnd + 1;

    const { nextOffset: afterValue, defined } = serializeNestedElementValue(bytes, cursor, type, depth, writer);
    cursor = afterValue;
    // Malformed BSON must fail fast instead of getting the parser stuck on the
    // same element forever.
    assertAdvanced(previousCursor, cursor);

    if (!defined) {
      writer.truncate(writerOffset);
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

  const totalLength = readDocumentLength(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  writer.writeByte(0x5b);
  let first = true;

  while (cursor < bodyEnd) {
    const previousCursor = cursor;
    const type = bytes[cursor++];
    cursor = skipCString(bytes, cursor);

    if (!first) {
      writer.writeByte(0x2c);
    }
    first = false;

    const { nextOffset: afterValue, defined } = serializeNestedElementValue(bytes, cursor, type, depth, writer);
    cursor = afterValue;
    assertAdvanced(previousCursor, cursor);

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
    case 0x01: // Double
      return serializeNestedDoubleElement(bytes, offset, writer);
    case 0x02: // String
      return serializeNestedStringElement(bytes, offset, writer);
    case 0x03: // Embedded document
      return serializeNestedObjectElement(bytes, offset, depth, writer);
    case 0x04: // Array
      return serializeNestedArrayElement(bytes, offset, depth, writer);
    case 0x05: // Binary
      return serializeNestedBinaryElement(bytes, offset, writer);
    case 0x06: // Undefined
      return { nextOffset: offset, defined: false };
    case 0x07: {
      // ObjectId
      writer.writeQuotedHexLower(bytes, offset, 12);
      return { nextOffset: offset + 12, defined: true };
    }
    case 0x08: // Boolean
      writer.writeByte(bytes[offset] ? 0x31 : 0x30);
      return { nextOffset: offset + 1, defined: true };
    case 0x09: // UTC datetime
      return serializeNestedDateTimeElement(bytes, offset, writer);
    case 0x0a: // Null
    case 0xff: // MinKey
    case 0x7f: // MaxKey
      writer.writeAscii('null');
      return { nextOffset: offset, defined: true };
    case 0x0b: // Regular expression
      return serializeNestedRegexElement(bytes, offset, writer);
    case 0x0c: // DBPointer
      return serializeNestedDbPointerElement(bytes, offset, writer);
    case 0x0d: // JavaScript code
      return serializeNestedCodeElement(bytes, offset, depth, writer);
    case 0x0e: // Symbol
      return serializeNestedSymbolElement(bytes, offset, writer);
    case 0x0f: // JavaScript code with scope
      return serializeNestedCodeWithScopeElement(bytes, offset, depth, writer);
    case 0x10: {
      // Int32
      writer.writeAscii(String(readInt32LE(bytes, offset)));
      return { nextOffset: offset + 4, defined: true };
    }
    case 0x11: {
      // Timestamp
      writer.writeAscii(timestampToBigInt(bytes, offset).toString());
      return { nextOffset: offset + 8, defined: true };
    }
    case 0x12: {
      // Int64
      writer.writeAscii(bytes.readBigInt64LE(offset).toString());
      return { nextOffset: offset + 8, defined: true };
    }
    case 0x13: // Decimal128
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
  if (!Number.isFinite(value)) {
    writer.writeAscii('null');
  } else {
    writer.writeAscii(value.toString());
  }
  return { nextOffset: offset + 8, defined: true };
}

function serializeNestedStringElement(
  bytes: Buffer,
  offset: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  const { nextOffset } = readBsonString(bytes, offset);
  const stringStart = offset + 4;
  const length = nextOffset - stringStart;
  writer.writeQuotedUtf8Slice(bytes, stringStart, stringStart + length - 1);
  return { nextOffset, defined: true };
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
  writeRegexJson(writer, pattern, options);
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
    return date.toISOString().replace('T', ' ');
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

function hexLower(bytes: Buffer, offset: number, length: number): string {
  return bytes.toString('hex', offset, offset + length);
}

function writeRegexJson(writer: JsonBufferWriter, pattern: string, options: string) {
  writer.writeAscii('{"pattern":');
  writer.writeQuotedJsonString(pattern);
  writer.writeAscii(',"options":');
  writer.writeQuotedJsonString(options);
  writer.writeByte(0x7d);
}

function writeCodeJson(bytes: Buffer, offset: number, depth: number, writer: JsonBufferWriter) {
  const { value: code, nextOffset } = readBsonString(bytes, offset);
  writer.writeAscii('{"code":');
  writer.writeQuotedJsonString(code);
  writer.writeAscii(',"scope":null}');
  return nextOffset;
}

function writeCodeWithScopeJson(bytes: Buffer, offset: number, depth: number, writer: JsonBufferWriter) {
  const totalLength = readInt32LE(bytes, offset);
  const { value: code, nextOffset: afterCode } = readBsonString(bytes, offset + 4);
  writer.writeAscii('{"code":');
  writer.writeQuotedJsonString(code);
  writer.writeAscii(',"scope":');
  serializeNestedObjectToJson(bytes, afterCode, depth + 1, writer);
  writer.writeByte(0x7d);
  // code_w_scope carries its own total byte length, so we trust that wrapper
  // rather than reconstructing the end position from the nested scope.
  return offset + totalLength;
}

function writeDbPointerJson(bytes: Buffer, offset: number, writer: JsonBufferWriter) {
  const { value: collection, nextOffset } = readBsonString(bytes, offset);
  writer.writeAscii('{"collection":');
  writer.writeQuotedJsonString(collection);
  writer.writeAscii(',"oid":');
  writer.writeQuotedHexLower(bytes, nextOffset, 12);
  writer.writeAscii(',"fields":{}}');
  return nextOffset + 12;
}

function serializeNestedBinaryElement(
  bytes: Buffer,
  offset: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  const length = readInt32LE(bytes, offset);
  if (length < 0) {
    throw new Error('Invalid BSON binary length');
  }
  const subtype = bytes[offset + 4];
  const dataStart = offset + 5;
  const dataEnd = dataStart + length;
  if (dataEnd > bytes.length) {
    throw new Error('Invalid BSON binary length');
  }

  const slice = binaryDataSlice(bytes, dataStart, dataEnd, subtype);
  // Nested binary values are omitted from JSON unless they are subtype 4 UUIDs,
  // which are represented as strings for parity with the default path.
  if (subtype === mongo.Binary.SUBTYPE_UUID && slice.length === 16) {
    writer.writeQuotedJsonString(uuidToString(slice));
    return { nextOffset: dataEnd, defined: true };
  }

  return { nextOffset: dataEnd, defined: false };
}

function assertAdvanced(previousOffset: number, nextOffset: number) {
  if (nextOffset <= previousOffset) {
    throw new Error('Invalid BSON parser state: non-advancing offset');
  }
}

function serializeNestedDbPointerElement(
  bytes: Buffer,
  offset: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  return { nextOffset: writeDbPointerJson(bytes, offset, writer), defined: true };
}

function serializeNestedCodeElement(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  return { nextOffset: writeCodeJson(bytes, offset, depth, writer), defined: true };
}

function serializeNestedSymbolElement(
  bytes: Buffer,
  offset: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  return serializeNestedStringElement(bytes, offset, writer);
}

function serializeNestedCodeWithScopeElement(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter
): { nextOffset: number; defined: boolean } {
  return { nextOffset: writeCodeWithScopeJson(bytes, offset, depth, writer), defined: true };
}
