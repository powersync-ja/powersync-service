import { mongo } from '@powersync/lib-service-mongodb';

const NESTED_DEPTH_LIMIT = 20;

export function runJsStreamingNestedForBuffer(bytes) {
  const row = {};
  const bodyEnd = readInt32LE(bytes, 0) - 1;
  let offset = 4;

  while (offset < bodyEnd) {
    const type = bytes[offset++];
    const [key, afterKey] = readCString(bytes, offset);
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
        row[key] = serializeNestedValueToJson(bytes, offset, true, 1);
        offset += readInt32LE(bytes, offset);
        break;
      }
      case 0x03: {
        row[key] = serializeNestedValueToJson(bytes, offset, false, 1);
        offset += readInt32LE(bytes, offset);
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
        const [value, nextOffset] = parseTopLevelBinary(bytes, offset);
        row[key] = value;
        offset = nextOffset;
        break;
      }
      case 0x0b: {
        const [pattern, options, nextOffset] = parseRegex(bytes, offset);
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

function readInt32LE(bytes, offset) {
  return bytes.readInt32LE(offset);
}

function readCString(bytes, offset) {
  const end = bytes.indexOf(0, offset);
  if (end < 0) {
    throw new Error('Invalid BSON: missing cstring terminator');
  }
  return [bytes.toString('utf8', offset, end), end + 1];
}

function skipCString(bytes, offset) {
  const end = bytes.indexOf(0, offset);
  if (end < 0) {
    throw new Error('Invalid BSON: missing cstring terminator');
  }
  return end + 1;
}

function quoteJsonFast(text) {
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

function parseRegex(bytes, offset) {
  const patternEnd = bytes.indexOf(0, offset);
  const optionsEnd = bytes.indexOf(0, patternEnd + 1);
  if (patternEnd < 0 || optionsEnd < 0) {
    throw new Error('Invalid BSON regex');
  }
  const pattern = bytes.toString('utf8', offset, patternEnd);
  const optionsRaw = bytes.toString('utf8', patternEnd + 1, optionsEnd);
  return [pattern, regexOptionsToJsFlags(optionsRaw), optionsEnd + 1];
}

function decimal128ToString(bytes, offset) {
  return new mongo.Decimal128(bytes.subarray(offset, offset + 16)).toString();
}

function uuidOrHex(bytes) {
  const hex = bytes.toString('hex');
  if (bytes.length != 16) {
    return hex;
  }
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function parseTopLevelBinary(bytes, offset) {
  const length = readInt32LE(bytes, offset);
  const subtype = bytes[offset + 4];
  const dataStart = offset + 5;
  const dataEnd = dataStart + length;
  const data = bytes.subarray(dataStart, dataEnd);

  if (subtype === mongo.Binary.SUBTYPE_UUID || subtype === mongo.Binary.SUBTYPE_UUID_OLD) {
    return [uuidOrHex(data), dataEnd];
  }

  return [data, dataEnd];
}

function skipBsonValue(bytes, offset, type) {
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

function serializeNestedValueToJson(bytes, offset, isArray, depth) {
  if (depth > NESTED_DEPTH_LIMIT) {
    throw new Error(`json nested object depth exceeds the limit of ${NESTED_DEPTH_LIMIT}`);
  }

  const totalLength = readInt32LE(bytes, offset);
  const bodyEnd = offset + totalLength - 1;
  let cursor = offset + 4;
  const parts = [isArray ? '[' : '{'];
  let first = true;

  while (cursor < bodyEnd) {
    const type = bytes[cursor++];
    let key = '';
    if (isArray) {
      cursor = skipCString(bytes, cursor);
    } else {
      [key, cursor] = readCString(bytes, cursor);
    }

    const [serialized, nextCursor, defined] = serializeNestedElementValue(bytes, cursor, type, depth);
    cursor = nextCursor;

    if (!defined && !isArray) {
      continue;
    }

    if (!first) {
      parts.push(',');
    }
    first = false;

    if (isArray) {
      parts.push(defined ? serialized : 'null');
    } else {
      parts.push(quoteJsonFast(key), ':', serialized);
    }
  }

  parts.push(isArray ? ']' : '}');
  return parts.join('');
}

function serializeNestedElementValue(bytes, offset, type, depth) {
  switch (type) {
    case 0x01: {
      const value = bytes.readDoubleLE(offset);
      const serialized = Number.isInteger(value) ? Math.trunc(value).toString() : Number(value).toString();
      return [serialized, offset + 8, true];
    }
    case 0x02: {
      const length = readInt32LE(bytes, offset);
      const stringStart = offset + 4;
      const text = bytes.toString('utf8', stringStart, stringStart + length - 1);
      return [quoteJsonFast(text), stringStart + length, true];
    }
    case 0x03: {
      const serialized = serializeNestedValueToJson(bytes, offset, false, depth + 1);
      return [serialized, offset + readInt32LE(bytes, offset), true];
    }
    case 0x04: {
      const serialized = serializeNestedValueToJson(bytes, offset, true, depth + 1);
      return [serialized, offset + readInt32LE(bytes, offset), true];
    }
    case 0x05: {
      const next = skipBsonValue(bytes, offset, type);
      return ['', next, false];
    }
    case 0x06:
      return ['', offset, false];
    case 0x07: {
      const value = quoteJsonFast(hexLower(bytes, offset, 12));
      return [value, offset + 12, true];
    }
    case 0x08:
      return [bytes[offset] ? '1' : '0', offset + 1, true];
    case 0x09: {
      const millis = Number(bytes.readBigInt64LE(offset));
      const value = quoteJsonFast(legacyDateTimeString(millis));
      return [value, offset + 8, true];
    }
    case 0x0a:
    case 0xff:
    case 0x7f:
      return ['null', offset, true];
    case 0x0b: {
      const [pattern, options, nextOffset] = parseRegex(bytes, offset);
      return [`{"pattern":${quoteJsonFast(pattern)},"options":${quoteJsonFast(options)}}`, nextOffset, true];
    }
    case 0x10: {
      const value = readInt32LE(bytes, offset);
      return [String(value), offset + 4, true];
    }
    case 0x11: {
      const increment = bytes.readUInt32LE(offset);
      const time = bytes.readUInt32LE(offset + 4);
      return [`{"t":${time},"i":${increment}}`, offset + 8, true];
    }
    case 0x12: {
      const value = bytes.readBigInt64LE(offset);
      return [value.toString(), offset + 8, true];
    }
    case 0x13:
      return [quoteJsonFast(decimal128ToString(bytes, offset)), offset + 16, true];
    default:
      throw new Error(`Unsupported BSON nested type: 0x${type.toString(16)}`);
  }
}

function legacyDateTimeString(millis) {
  const iso = new Date(millis).toISOString();
  return `${iso.slice(0, 10)} ${iso.slice(11)}`;
}

function regexOptionsToJsFlags(options) {
  let out = '';
  if (options.includes('s')) out += 'g';
  if (options.includes('i')) out += 'i';
  if (options.includes('m')) out += 'm';
  return out;
}

function hexLower(bytes, offset, length) {
  return bytes.toString('hex', offset, offset + length);
}
