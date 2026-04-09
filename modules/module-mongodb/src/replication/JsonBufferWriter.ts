const JSON_BUFFER_INITIAL_CAPACITY = 1024 * 1024;
const HEX_LOWER_BYTES = Buffer.from('0123456789abcdef', 'ascii');
export const BYTE_DQUOTE = 0x22; // "
export const BYTE_BACKSLASH = 0x5c; // \
export const BYTE_BACKSPACE = 0x08; // \b
export const BYTE_TAB = 0x09; // \t
export const BYTE_NEWLINE = 0x0a; // \n
export const BYTE_FORM_FEED = 0x0c; // \f
export const BYTE_CARRIAGE_RETURN = 0x0d; // \r
export const BYTE_LBRACE = 0x7b; // {
export const BYTE_RBRACE = 0x7d; // }
export const BYTE_LBRACKET = 0x5b; // [
export const BYTE_RBRACKET = 0x5d; // ]
export const BYTE_COMMA = 0x2c; // ,
export const BYTE_COLON = 0x3a; // :
export const BYTE_ZERO = 0x30; // 0
export const BYTE_ONE = 0x31; // 1
const BYTE_SPACE = 0x20; // ' '
const BYTE_DASH = 0x2d; // -
const BYTE_DOT = 0x2e; // .
const BYTE_Z = 0x5a; // Z
const BYTE_B = 0x62; // b
const BYTE_T = 0x74; // t
const BYTE_N = 0x6e; // n
const BYTE_F = 0x66; // f
const BYTE_R = 0x72; // r
const BYTE_U = 0x75; // u

/**
 * Low-level class to generate JSON.
 *
 * This writes to a Buffer, which can then be decoded as a string.
 */
export class JsonBufferWriter {
  /**
   * The raw buffer. Capacity can increase, but is never decreased.
   */
  private buffer: Buffer;

  /**
   * length of data written to the buffer.
   */
  private length = 0;

  constructor(capacity = JSON_BUFFER_INITIAL_CAPACITY) {
    // allocUnsafe is fine - we always write to the buffer when updating length.
    this.buffer = Buffer.allocUnsafe(capacity);
  }

  /**
   * Resets the length, equivalent to truncate(0).
   */
  reset() {
    this.length = 0;
  }

  toString() {
    return this.buffer.toString('utf8', 0, this.length);
  }

  getLength() {
    return this.length;
  }

  truncate(length: number) {
    this.length = length;
  }

  /**
   * Write a single raw byte.
   *
   * Caller is responsible for ensuring this produces valid JSON.
   */
  writeByte(value: number) {
    this.ensureCapacity(1);
    this.buffer[this.length++] = value;
  }

  /**
   * Write raw ascii string - one byte per character. No quoting or escaping is performed.
   *
   * Caller is responsible for ensuring this produces valid JSON.
   */
  writeAscii(text: string) {
    const length = text.length;
    this.ensureCapacity(length);
    this.buffer.write(text, this.length, length, 'ascii');
    this.length += length;
  }

  /**
   * Write UTF-8 string. No quoting or escaping is performed.
   *
   * Caller is responsible for ensuring this produces valid JSON.
   */
  writeUtf8(text: string) {
    const length = Buffer.byteLength(text);
    this.ensureCapacity(length);
    this.buffer.write(text, this.length, length, 'utf8');
    this.length += length;
  }

  /**
   * Quote and write a string, escaping characters as needed.
   */
  writeQuotedJsonString(text: string) {
    this.writeByte(BYTE_DQUOTE);
    let start = 0;

    for (let i = 0; i < text.length; i++) {
      const ch = text.charCodeAt(i);
      let escaped: string | undefined;

      switch (ch) {
        case BYTE_DQUOTE:
          escaped = '\\"';
          break;
        case BYTE_BACKSLASH:
          escaped = '\\\\';
          break;
        case BYTE_BACKSPACE:
          escaped = '\\b';
          break;
        case BYTE_TAB:
          escaped = '\\t';
          break;
        case BYTE_NEWLINE:
          escaped = '\\n';
          break;
        case BYTE_FORM_FEED:
          escaped = '\\f';
          break;
        case BYTE_CARRIAGE_RETURN:
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

    this.writeByte(BYTE_DQUOTE);
  }

  /**
   * Quotes and write an UTF-8 string from a source buffer, escaping characters as needed.
   *
   * @param bytes source buffer
   * @param start start offset, inclusive
   * @param end end offset, exclusive
   */
  writeQuotedUtf8Slice(bytes: Buffer, start: number, end: number): void {
    let firstEscape = -1;
    for (let index = start; index < end; index++) {
      const value = bytes[index];
      if (value < 0x20 || value === BYTE_DQUOTE || value === BYTE_BACKSLASH) {
        firstEscape = index;
        break;
      }
    }

    const rawLength = end - start;
    this.ensureCapacity(rawLength + 2);
    let buffer = this.buffer;
    let length = this.length;

    buffer[length++] = BYTE_DQUOTE;

    if (firstEscape < 0) {
      bytes.copy(buffer, length, start, end);
      length += rawLength;
      buffer[length++] = BYTE_DQUOTE;
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
      if (value >= 0x20 && value !== BYTE_DQUOTE && value !== BYTE_BACKSLASH) {
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
        case BYTE_DQUOTE:
          buffer[length++] = BYTE_BACKSLASH;
          buffer[length++] = BYTE_DQUOTE;
          break;
        case BYTE_BACKSLASH:
          buffer[length++] = BYTE_BACKSLASH;
          buffer[length++] = BYTE_BACKSLASH;
          break;
        case BYTE_BACKSPACE:
          buffer[length++] = BYTE_BACKSLASH;
          buffer[length++] = BYTE_B;
          break;
        case BYTE_TAB:
          buffer[length++] = BYTE_BACKSLASH;
          buffer[length++] = BYTE_T;
          break;
        case BYTE_NEWLINE:
          buffer[length++] = BYTE_BACKSLASH;
          buffer[length++] = BYTE_N;
          break;
        case BYTE_FORM_FEED:
          buffer[length++] = BYTE_BACKSLASH;
          buffer[length++] = BYTE_F;
          break;
        case BYTE_CARRIAGE_RETURN:
          buffer[length++] = BYTE_BACKSLASH;
          buffer[length++] = BYTE_R;
          break;
        default:
          buffer[length++] = BYTE_BACKSLASH;
          buffer[length++] = BYTE_U;
          buffer[length++] = BYTE_ZERO;
          buffer[length++] = BYTE_ZERO;
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

    buffer[length++] = BYTE_DQUOTE;
    this.length = length;
  }

  /**
   * Quote and write bytes as hex.
   */
  writeQuotedHexLower(bytes: Buffer, start: number, length: number) {
    this.ensureCapacity(length * 2 + 2);
    this.buffer[this.length++] = BYTE_DQUOTE;
    for (let index = start; index < start + length; index++) {
      const value = bytes[index];
      this.buffer[this.length++] = HEX_LOWER_BYTES[value >> 4];
      this.buffer[this.length++] = HEX_LOWER_BYTES[value & 0x0f];
    }
    this.buffer[this.length++] = BYTE_DQUOTE;
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
    millisecond: number,
    quoted: boolean
  ) {
    // Technically 24 when not quoted, but no harm in over-allocating
    this.ensureCapacity(26);
    const buffer = this.buffer;
    let offset = this.length;

    if (quoted) {
      buffer[offset++] = BYTE_DQUOTE;
    }

    buffer[offset++] = BYTE_ZERO + ((year / 1000) | 0);
    buffer[offset++] = BYTE_ZERO + (((year / 100) | 0) % 10);
    buffer[offset++] = BYTE_ZERO + (((year / 10) | 0) % 10);
    buffer[offset++] = BYTE_ZERO + (year % 10);
    buffer[offset++] = BYTE_DASH;
    buffer[offset++] = BYTE_ZERO + ((month / 10) | 0);
    buffer[offset++] = BYTE_ZERO + (month % 10);
    buffer[offset++] = BYTE_DASH;
    buffer[offset++] = BYTE_ZERO + ((day / 10) | 0);
    buffer[offset++] = BYTE_ZERO + (day % 10);
    buffer[offset++] = BYTE_SPACE;
    buffer[offset++] = BYTE_ZERO + ((hour / 10) | 0);
    buffer[offset++] = BYTE_ZERO + (hour % 10);
    buffer[offset++] = BYTE_COLON;
    buffer[offset++] = BYTE_ZERO + ((minute / 10) | 0);
    buffer[offset++] = BYTE_ZERO + (minute % 10);
    buffer[offset++] = BYTE_COLON;
    buffer[offset++] = BYTE_ZERO + ((second / 10) | 0);
    buffer[offset++] = BYTE_ZERO + (second % 10);
    buffer[offset++] = BYTE_DOT;
    buffer[offset++] = BYTE_ZERO + ((millisecond / 100) | 0);
    buffer[offset++] = BYTE_ZERO + (((millisecond / 10) | 0) % 10);
    buffer[offset++] = BYTE_ZERO + (millisecond % 10);
    buffer[offset++] = BYTE_Z;

    if (quoted) {
      buffer[offset++] = BYTE_DQUOTE;
    }

    this.length = offset;
  }
}
