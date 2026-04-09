import { mongo } from '@powersync/lib-service-mongodb';
import {
  CompatibilityContext,
  CompatibilityOption,
  DateTimeSourceOptions,
  DateTimeValue,
  SqliteRow,
  TimeValuePrecision
} from '@powersync/service-sync-rules';
import {
  BYTE_COLON,
  BYTE_COMMA,
  BYTE_LBRACE,
  BYTE_LBRACKET,
  BYTE_ONE,
  BYTE_RBRACE,
  BYTE_RBRACKET,
  BYTE_SPACE,
  BYTE_T,
  BYTE_ZERO,
  JsonBufferWriter
} from './JsonBufferWriter.js';

const NESTED_DEPTH_LIMIT = 20;
const SHARED_UTC_DATE = new Date(0);
const BSON_TYPE_DOUBLE = 0x01;
const BSON_TYPE_STRING = 0x02;
const BSON_TYPE_DOCUMENT = 0x03;
const BSON_TYPE_ARRAY = 0x04;
const BSON_TYPE_BINARY = 0x05;
const BSON_TYPE_UNDEFINED = 0x06;
const BSON_TYPE_OBJECT_ID = 0x07;
const BSON_TYPE_BOOLEAN = 0x08;
const BSON_TYPE_UTC_DATETIME = 0x09;
const BSON_TYPE_NULL = 0x0a;
const BSON_TYPE_REGEX = 0x0b;
const BSON_TYPE_DB_POINTER = 0x0c;
const BSON_TYPE_CODE = 0x0d;
const BSON_TYPE_SYMBOL = 0x0e;
const BSON_TYPE_CODE_WITH_SCOPE = 0x0f;
const BSON_TYPE_INT32 = 0x10;
const BSON_TYPE_TIMESTAMP = 0x11;
const BSON_TYPE_INT64 = 0x12;
const BSON_TYPE_DECIMAL128 = 0x13;
const BSON_TYPE_MIN_KEY = 0xff;
const BSON_TYPE_MAX_KEY = 0x7f;
const BSON_BINARY_SUBTYPE_BYTE_ARRAY = 2;
const BSON_BINARY_SUBTYPE_UUID = 4;

export const enum DateRenderMode {
  LEGACY_MILLISECONDS,
  ISO_MILLISECONDS,
  ISO_SECONDS
}

// We use a single shared write, to avoid repeatedly re-allocating buffers.
// Since this is only used in a synchronous call, this is safe.
// This never releases memory once a large buffer has been allocated, but that is fine
// for replication use.
const SHARED_WRITER = new JsonBufferWriter();

/**
 * Convert a buffer containing BSON bytes to a SqliteRow.
 *
 * This is using a custom BSON parser and JSON serializer for performance reasons. By bypassing bson.deserialize,
 * we avoid many small allocations, and can significantly increase throughput.
 *
 * This attempts to match the behavior of `bson.deserialize -> constructAfterRecord -> applyRowContext` for the most part,
 * with some intentional differences:
 * 1. Regular expression patterns options are preserved as-is, while the above normalizes to JS RegExp values.
 * 2. Full UTF-8 validation is not performed - we attempt to continue using replacement characters, as long as the resulting output remains valid.
 * 3. bson.deserialize has special-case handler for converting documents containing {$ref} -> DBRef. We don't do that here.
 *
 * General principles followed:
 * 1. Correctness: Never produce invalid JSON.
 * 2. Performance: Optimize to be as performant as possible for common cases.
 * 3. Full BSON support: Support all valid BSON documents as input, including deprecated types, but without specifically optimizing for performance here.
 * 4. The source database is responsible for producing valid BSON - we don't test for all edge cases of invalid BSON.
 * 5. We do a best-effort attempt to support "degenerate" BSON cases as documented at https://specifications.readthedocs.io/en/latest/bson-corpus/bson-corpus/, since MongoDB can produce many of these cases.
 *
 * @param bytes the source BSON bytes
 * @param dateRenderMode derive using getDateRenderMode(compatibilityContext)
 *
 * @returns a SqliteRow
 */
export function bufferToSqlite(bytes: Buffer, dateRenderMode: DateRenderMode): SqliteRow {
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
      case BSON_TYPE_OBJECT_ID: {
        row[key] = hexLower(bytes, offset, 12);
        offset += 12;
        break;
      }
      case BSON_TYPE_STRING: {
        const { value, nextOffset } = readBsonString(bytes, offset);
        row[key] = value;
        offset = nextOffset;
        break;
      }
      case BSON_TYPE_ARRAY: {
        jsonWriter.reset();
        const result = serializeNestedArrayToJson(bytes, offset, 0, jsonWriter, dateRenderMode);
        row[key] = jsonWriter.toString();
        offset = result.nextOffset;
        break;
      }
      case BSON_TYPE_DOCUMENT: {
        jsonWriter.reset();
        const result = serializeNestedObjectToJson(bytes, offset, 0, jsonWriter, dateRenderMode);
        row[key] = jsonWriter.toString();
        offset = result.nextOffset;
        break;
      }
      case BSON_TYPE_BOOLEAN: {
        row[key] = bytes[offset++] ? 1n : 0n;
        break;
      }
      case BSON_TYPE_UTC_DATETIME: {
        // Even though this is not JSON, we use the same JSON writer for this.
        jsonWriter.reset();
        appendDateTimeToWriter(jsonWriter, Number(bytes.readBigInt64LE(offset)), false, dateRenderMode);
        row[key] = jsonWriter.toString();
        offset += 8;
        break;
      }
      case BSON_TYPE_INT32: {
        row[key] = BigInt(readInt32LE(bytes, offset));
        offset += 4;
        break;
      }
      case BSON_TYPE_TIMESTAMP: {
        row[key] = timestampToBigInt(bytes, offset);
        offset += 8;
        break;
      }
      case BSON_TYPE_INT64: {
        row[key] = bytes.readBigInt64LE(offset);
        offset += 8;
        break;
      }
      case BSON_TYPE_DECIMAL128: {
        row[key] = decimal128ToString(bytes, offset);
        offset += 16;
        break;
      }
      case BSON_TYPE_BINARY: {
        const { value, nextOffset } = parseTopLevelBinary(bytes, offset);
        row[key] = value;
        offset = nextOffset;
        break;
      }
      case BSON_TYPE_REGEX: {
        const { pattern, options, nextOffset } = parseRegex(bytes, offset);
        jsonWriter.reset();
        writeRegexJson(jsonWriter, pattern, options);
        row[key] = jsonWriter.toString();
        offset = nextOffset;
        break;
      }
      case BSON_TYPE_DB_POINTER: {
        // DBPointer
        jsonWriter.reset();
        const nextOffset = writeDbPointerJson(bytes, offset, jsonWriter);
        row[key] = jsonWriter.toString();
        offset = nextOffset;
        break;
      }
      case BSON_TYPE_CODE: {
        // JavaScript code
        jsonWriter.reset();
        const nextOffset = writeCodeJson(bytes, offset, 0, jsonWriter);
        row[key] = jsonWriter.toString();
        offset = nextOffset;
        break;
      }
      case BSON_TYPE_SYMBOL: {
        // Symbol
        const { value, nextOffset } = readBsonString(bytes, offset);
        row[key] = value;
        offset = nextOffset;
        break;
      }
      case BSON_TYPE_CODE_WITH_SCOPE: {
        jsonWriter.reset();
        const nextOffset = writeCodeWithScopeJson(bytes, offset, 0, jsonWriter, dateRenderMode);
        row[key] = jsonWriter.toString();
        offset = nextOffset;
        break;
      }
      case BSON_TYPE_UNDEFINED:
      case BSON_TYPE_NULL:
      case BSON_TYPE_MIN_KEY:
      case BSON_TYPE_MAX_KEY:
        row[key] = null;
        break;
      case BSON_TYPE_DOUBLE: {
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

function readBsonStringEnd(bytes: Buffer, offset: number): number {
  const length = readInt32LE(bytes, offset);
  const stringStart = offset + 4;
  const stringEnd = stringStart + length;
  if (length < 1 || stringEnd > bytes.length) {
    throw new Error('Invalid BSON string length');
  }
  if (bytes[stringEnd - 1] !== 0) {
    throw new Error('Invalid BSON string terminator');
  }
  return stringEnd;
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
  // Just use the upstream parser for this
  return new mongo.Decimal128(bytes.subarray(offset, offset + 16)).toString();
}

function timestampToBigInt(bytes: Buffer, offset: number): bigint {
  return (BigInt(bytes.readUInt32LE(offset + 4)) << 32n) | BigInt(bytes.readUInt32LE(offset));
}

/**
 * @param bytes must be exactly 16 bytes in length - check before calling this.
 * @returns lower-case hex form of the UUID
 */
function uuidToString(bytes: Buffer): string {
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function parseTopLevelBinary(bytes: Buffer, offset: number): { value: Uint8Array | string; nextOffset: number } {
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
  if (subtype === BSON_BINARY_SUBTYPE_UUID && data.length === 16) {
    return { value: uuidToString(data), nextOffset: dataEnd };
  }

  return { value: bufferToUint8Array(data), nextOffset: dataEnd };
}

function bufferToUint8Array(bytes: Buffer): Uint8Array {
  return new Uint8Array(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength));
}

/**
 * Handle a sub-array for binary data, including the legacy 2 subtype.
 */
function binaryDataSlice(bytes: Buffer, dataStart: number, dataEnd: number, subtype: number): Buffer {
  if (subtype !== BSON_BINARY_SUBTYPE_BYTE_ARRAY) {
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
    case BSON_TYPE_DOUBLE: // Double
      return offset + 8;
    case BSON_TYPE_STRING: {
      // String
      const length = readInt32LE(bytes, offset);
      return offset + 4 + length;
    }
    case BSON_TYPE_DOCUMENT: // Embedded document
    case BSON_TYPE_ARRAY: // Array
      return offset + readInt32LE(bytes, offset);
    case BSON_TYPE_BINARY: {
      // Binary
      const length = readInt32LE(bytes, offset);
      return offset + 4 + 1 + length;
    }
    case BSON_TYPE_UNDEFINED: // Undefined
    case BSON_TYPE_NULL: // Null
    case BSON_TYPE_MIN_KEY: // MinKey
    case BSON_TYPE_MAX_KEY: // MaxKey
      return offset;
    case BSON_TYPE_OBJECT_ID: // ObjectId
      return offset + 12;
    case BSON_TYPE_BOOLEAN: // Boolean
      return offset + 1;
    case BSON_TYPE_UTC_DATETIME: // UTC datetime
      return offset + 8;
    case BSON_TYPE_REGEX: {
      // Regular expression
      const patternEnd = bytes.indexOf(0, offset);
      const optionsEnd = bytes.indexOf(0, patternEnd + 1);
      if (patternEnd < 0 || optionsEnd < 0) {
        throw new Error('Invalid BSON regex');
      }
      return optionsEnd + 1;
    }
    case BSON_TYPE_DB_POINTER: {
      // DBPointer
      const nextOffset = readBsonStringEnd(bytes, offset);
      return nextOffset + 12;
    }
    case BSON_TYPE_CODE: {
      // JavaScript code
      return readBsonStringEnd(bytes, offset);
    }
    case BSON_TYPE_SYMBOL: {
      // Symbol
      return readBsonStringEnd(bytes, offset);
    }
    case BSON_TYPE_CODE_WITH_SCOPE: {
      // JavaScript code with scope
      const length = readInt32LE(bytes, offset);
      return offset + length;
    }
    case BSON_TYPE_INT32: // Int32
      return offset + 4;
    case BSON_TYPE_TIMESTAMP: // Timestamp
      return offset + 8;
    case BSON_TYPE_INT64: // Int64
      return offset + 8;
    case BSON_TYPE_DECIMAL128: // Decimal128
      return offset + 16;
    default:
      throw new Error(`Unsupported BSON type for skip: 0x${type.toString(16)}`);
  }
}

function serializeNestedObjectToJson(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter,
  dateRenderMode: DateRenderMode
): { nextOffset: number } {
  if (depth > NESTED_DEPTH_LIMIT) {
    throw new Error(`json nested object depth exceeds the limit of ${NESTED_DEPTH_LIMIT}`);
  }

  const totalLength = readDocumentLength(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  writer.writeByte(BYTE_LBRACE);
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
      writer.writeByte(BYTE_COMMA);
    }
    writer.writeQuotedUtf8Slice(bytes, cursor, keyEnd);
    writer.writeByte(BYTE_COLON);
    cursor = keyEnd + 1;

    const { nextOffset: afterValue, defined } = serializeNestedElementValue(
      bytes,
      cursor,
      type,
      depth,
      writer,
      dateRenderMode
    );
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

  writer.writeByte(BYTE_RBRACE);
  return { nextOffset: offset + totalLength };
}

function serializeNestedArrayToJson(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter,
  dateRenderMode: DateRenderMode
): { nextOffset: number } {
  if (depth > NESTED_DEPTH_LIMIT) {
    throw new Error(`json nested object depth exceeds the limit of ${NESTED_DEPTH_LIMIT}`);
  }

  const totalLength = readDocumentLength(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  writer.writeByte(BYTE_LBRACKET);
  let first = true;

  while (cursor < bodyEnd) {
    const previousCursor = cursor;
    const type = bytes[cursor++];
    cursor = skipCString(bytes, cursor);

    if (!first) {
      writer.writeByte(BYTE_COMMA);
    }
    first = false;

    const { nextOffset: afterValue, defined } = serializeNestedElementValue(
      bytes,
      cursor,
      type,
      depth,
      writer,
      dateRenderMode
    );
    cursor = afterValue;
    assertAdvanced(previousCursor, cursor);

    if (!defined) {
      writer.writeAscii('null');
    }
  }

  writer.writeByte(BYTE_RBRACKET);
  return { nextOffset: offset + totalLength };
}

function serializeNestedElementValue(
  bytes: Buffer,
  offset: number,
  type: number,
  depth: number,
  writer: JsonBufferWriter,
  dateRenderMode: DateRenderMode
): { nextOffset: number; defined: boolean } {
  switch (type) {
    case BSON_TYPE_DOUBLE: // Double
      return serializeNestedDoubleElement(bytes, offset, writer);
    case BSON_TYPE_STRING: // String
      return serializeNestedStringElement(bytes, offset, writer);
    case BSON_TYPE_DOCUMENT: // Embedded document
      return serializeNestedObjectElement(bytes, offset, depth, writer, dateRenderMode);
    case BSON_TYPE_ARRAY: // Array
      return serializeNestedArrayElement(bytes, offset, depth, writer, dateRenderMode);
    case BSON_TYPE_BINARY: // Binary
      return serializeNestedBinaryElement(bytes, offset, writer);
    case BSON_TYPE_UNDEFINED: // Undefined
      return { nextOffset: offset, defined: false };
    case BSON_TYPE_OBJECT_ID: {
      // ObjectId
      writer.writeQuotedHexLower(bytes, offset, 12);
      return { nextOffset: offset + 12, defined: true };
    }
    case BSON_TYPE_BOOLEAN: // Boolean
      writer.writeByte(bytes[offset] ? BYTE_ONE : BYTE_ZERO);
      return { nextOffset: offset + 1, defined: true };
    case BSON_TYPE_UTC_DATETIME: // UTC datetime
      return serializeNestedDateTimeElement(bytes, offset, writer, dateRenderMode);
    case BSON_TYPE_NULL: // Null
    case BSON_TYPE_MIN_KEY: // MinKey
    case BSON_TYPE_MAX_KEY: // MaxKey
      writer.writeAscii('null');
      return { nextOffset: offset, defined: true };
    case BSON_TYPE_REGEX: // Regular expression
      return serializeNestedRegexElement(bytes, offset, writer);
    case BSON_TYPE_DB_POINTER: // DBPointer
      return serializeNestedDbPointerElement(bytes, offset, writer);
    case BSON_TYPE_CODE: // JavaScript code
      return serializeNestedCodeElement(bytes, offset, depth, writer);
    case BSON_TYPE_SYMBOL: // Symbol
      return serializeNestedSymbolElement(bytes, offset, writer);
    case BSON_TYPE_CODE_WITH_SCOPE: // JavaScript code with scope
      return serializeNestedCodeWithScopeElement(bytes, offset, depth, writer, dateRenderMode);
    case BSON_TYPE_INT32: {
      // Int32
      writer.writeAscii(String(readInt32LE(bytes, offset)));
      return { nextOffset: offset + 4, defined: true };
    }
    case BSON_TYPE_TIMESTAMP: {
      // Timestamp
      writer.writeAscii(timestampToBigInt(bytes, offset).toString());
      return { nextOffset: offset + 8, defined: true };
    }
    case BSON_TYPE_INT64: {
      // Int64
      writer.writeAscii(bytes.readBigInt64LE(offset).toString());
      return { nextOffset: offset + 8, defined: true };
    }
    case BSON_TYPE_DECIMAL128: // Decimal128
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
  const nextOffset = readBsonStringEnd(bytes, offset);
  const stringStart = offset + 4;
  const length = nextOffset - stringStart;
  writer.writeQuotedUtf8Slice(bytes, stringStart, stringStart + length - 1);
  return { nextOffset, defined: true };
}

function serializeNestedObjectElement(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter,
  dateRenderMode: DateRenderMode
): { nextOffset: number; defined: boolean } {
  const result = serializeNestedObjectToJson(bytes, offset, depth + 1, writer, dateRenderMode);
  return { nextOffset: result.nextOffset, defined: true };
}

function serializeNestedArrayElement(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter,
  dateRenderMode: DateRenderMode
): { nextOffset: number; defined: boolean } {
  const result = serializeNestedArrayToJson(bytes, offset, depth + 1, writer, dateRenderMode);
  return { nextOffset: result.nextOffset, defined: true };
}

function serializeNestedDateTimeElement(
  bytes: Buffer,
  offset: number,
  writer: JsonBufferWriter,
  dateRenderMode: DateRenderMode
): { nextOffset: number; defined: boolean } {
  appendDateTimeToWriter(writer, Number(bytes.readBigInt64LE(offset)), true, dateRenderMode);
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

/**
 * KLUDGE: The DateTimeValue API needs a CompatibilityContext, but we don't want to pass that
 * around through the entire stack when the DateRenderMode encapsulates it.
 *
 * This translates back from DateRenderMode to CompatibilityContext.
 */
const DATETIME_COMPATIBILITY_OPTIONS: Record<DateRenderMode, CompatibilityContext> = {
  [DateRenderMode.LEGACY_MILLISECONDS]: CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY,
  [DateRenderMode.ISO_MILLISECONDS]: new CompatibilityContext({
    edition: 2,
    maxTimeValuePrecision: TimeValuePrecision.milliseconds
  }),
  [DateRenderMode.ISO_SECONDS]: new CompatibilityContext({
    edition: 2,
    maxTimeValuePrecision: TimeValuePrecision.seconds
  })
};

const MONGO_TIME_OPTIONS: DateTimeSourceOptions = {
  subSecondPrecision: TimeValuePrecision.milliseconds,
  defaultSubSecondPrecision: TimeValuePrecision.milliseconds
};

/**
 * Fallback date serialization.
 *
 * This is slow, but handles edge cases.
 */
function extendedDateTimeString(millis: number, dateRenderMode: DateRenderMode): string {
  const isoString = new Date(millis).toISOString();
  const compatibilityContext = DATETIME_COMPATIBILITY_OPTIONS[dateRenderMode];
  return new DateTimeValue(isoString, undefined, MONGO_TIME_OPTIONS).toSqliteValue(compatibilityContext) as string;
}

export function getDateRenderMode(compatibilityContext: CompatibilityContext): DateRenderMode {
  if (!compatibilityContext.isEnabled(CompatibilityOption.timestampsIso8601)) {
    return DateRenderMode.LEGACY_MILLISECONDS;
  }

  const maxPrecision = compatibilityContext.maxTimeValuePrecision ?? TimeValuePrecision.milliseconds;
  if (maxPrecision === TimeValuePrecision.seconds) {
    return DateRenderMode.ISO_SECONDS;
  }

  // MongoDB only supports millisecond precision, so this also convers configured values of
  // microseconds and nanoseconds.
  return DateRenderMode.ISO_MILLISECONDS;
}

function appendDateTimeToWriter(
  writer: JsonBufferWriter,
  millis: number,
  quoted: boolean,
  dateRenderMode: DateRenderMode
) {
  const date = SHARED_UTC_DATE;
  date.setTime(millis);

  if (Number.isNaN(date.getTime())) {
    throw new RangeError('Invalid time value');
  }

  const year = date.getUTCFullYear();
  if (year < 0 || year > 9999) {
    // Abnormal date ranges. We support these, but don't optimize for performance.
    const string = extendedDateTimeString(millis, dateRenderMode);
    if (quoted) {
      writer.writeQuotedJsonString(string);
    } else {
      writer.writeUtf8(string);
    }
    return;
  }

  writer.writeDateTime(
    year,
    date.getUTCMonth() + 1,
    date.getUTCDate(),
    date.getUTCHours(),
    date.getUTCMinutes(),
    date.getUTCSeconds(),
    date.getUTCMilliseconds(),
    quoted,
    dateRenderMode === DateRenderMode.LEGACY_MILLISECONDS ? BYTE_SPACE : BYTE_T,
    dateRenderMode !== DateRenderMode.ISO_SECONDS
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
  writer.writeByte(BYTE_RBRACE);
}

function writeCodeJson(bytes: Buffer, offset: number, depth: number, writer: JsonBufferWriter) {
  const { value: code, nextOffset } = readBsonString(bytes, offset);
  writer.writeAscii('{"code":');
  writer.writeQuotedJsonString(code);
  writer.writeAscii(',"scope":null}');
  return nextOffset;
}

function writeCodeWithScopeJson(
  bytes: Buffer,
  offset: number,
  depth: number,
  writer: JsonBufferWriter,
  dateRenderMode: DateRenderMode
) {
  const totalLength = readInt32LE(bytes, offset);
  const { value: code, nextOffset: afterCode } = readBsonString(bytes, offset + 4);
  writer.writeAscii('{"code":');
  writer.writeQuotedJsonString(code);
  writer.writeAscii(',"scope":');
  serializeNestedObjectToJson(bytes, afterCode, depth + 1, writer, dateRenderMode);
  writer.writeByte(BYTE_RBRACE);
  // code_w_scope carries its own total byte length, so we trust that wrapper
  // rather than reconstructing the end position from the nested scope.
  return offset + totalLength;
}

function writeDbPointerJson(bytes: Buffer, offset: number, writer: JsonBufferWriter) {
  const { value: collection, nextOffset } = readBsonString(bytes, offset);
  const separator = collection.indexOf('.');
  const db = separator >= 0 ? collection.slice(0, separator) : null;
  const collectionName = separator >= 0 ? collection.slice(separator + 1) : collection;
  writer.writeAscii('{"collection":');
  writer.writeQuotedJsonString(collectionName);
  writer.writeAscii(',"oid":');
  writer.writeQuotedHexLower(bytes, nextOffset, 12);
  if (db != null) {
    writer.writeAscii(',"db":');
    writer.writeQuotedJsonString(db);
  }
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
  if (subtype === BSON_BINARY_SUBTYPE_UUID && slice.length === 16) {
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
  writer: JsonBufferWriter,
  dateRenderMode: DateRenderMode
): { nextOffset: number; defined: boolean } {
  return {
    nextOffset: writeCodeWithScopeJson(bytes, offset, depth, writer, dateRenderMode),
    defined: true
  };
}
