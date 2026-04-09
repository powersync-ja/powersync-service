const JSON_BUFFER_INITIAL_CAPACITY = 1024 * 1024;
const HEX_LOWER_BYTES = Buffer.from('0123456789abcdef', 'ascii');

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

  /**
   * Quote and write a string, escaping characters as needed.
   */
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

  /**
   * Write bytes as hex.
   *
   * This does not add any quotes - the caller is responsible for that.
   */
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
