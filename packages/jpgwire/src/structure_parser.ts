import { delimiter } from 'path';

/**
 * Utility to parse encoded structural values, such as arrays, composite types, ranges and multiranges.
 */
export class StructureParser {
  private offset: number;

  constructor(readonly source: string) {
    this.offset = 0;
  }

  private currentCharCode(): number {
    return this.source.charCodeAt(this.offset);
  }

  private currentChar(): string {
    return this.source.charAt(this.offset);
  }

  private get isAtEnd(): boolean {
    return this.offset == this.source.length;
  }

  private checkNotAtEnd() {
    if (this.isAtEnd) {
      this.error('Unexpected end of input');
    }
  }

  private error(msg: string): never {
    throw new Error(`Error decoding Postgres sequence at position ${this.offset}: ${msg}`);
  }

  private check(expected: number) {
    if (this.currentCharCode() != expected) {
      this.error(`Expected ${String.fromCharCode(expected)}, got ${String.fromCharCode(this.currentCharCode())}`);
    }
  }

  private peek(): number {
    this.checkNotAtEnd();

    return this.source.charCodeAt(this.offset + 1);
  }

  private advance() {
    this.checkNotAtEnd();
    this.offset++;
  }

  private consume(expected: number) {
    this.check(expected);
    this.advance();
  }

  private maybeConsume(expected: number): boolean {
    if (this.currentCharCode() == expected) {
      this.advance();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Assuming that the current position contains a opening double quote for an escaped string, parses the value until
   * the closing quote.
   *
   * The returned value applies escape characters, so `"foo\"bar"` would return the string `foo"bar"`.
   */
  private quotedString(allowEscapingWithDoubleDoubleQuote: boolean = false): string {
    this.consume(CHAR_CODE_DOUBLE_QUOTE);

    const start = this.offset;
    let buffer = '';

    while (true) {
      const startPosition = this.offset;

      // Skip past "boring" chars that we just need to add to the buffer.
      let char = this.currentCharCode();
      while (char != CHAR_CODE_BACKSLASH && char != CHAR_CODE_DOUBLE_QUOTE) {
        this.advance();
        char = this.currentCharCode();
      }

      if (this.offset > startPosition) {
        buffer += this.source.substring(startPosition, this.offset);
      }

      if (char == CHAR_CODE_DOUBLE_QUOTE) {
        if (this.offset != start && allowEscapingWithDoubleDoubleQuote) {
          // If the next character is also a double quote, that escapes a single double quote
          if (this.offset < this.source.length - 1 && this.peek() == CHAR_CODE_DOUBLE_QUOTE) {
            this.offset += 2;
            buffer += STRING_DOUBLE_QUOTE;
            continue;
          }
        }

        break; // End of string.
      } else if (char == CHAR_CODE_BACKSLASH) {
        this.advance(); // Consume the backslash
        const nextChar = this.currentCharCode();
        if (nextChar != CHAR_CODE_DOUBLE_QUOTE && nextChar != CHAR_CODE_BACKSLASH) {
          this.error('Expected escaped double quote or escaped backslash');
        }

        buffer += this.currentChar();
        this.advance();
      }
    }

    this.consume(CHAR_CODE_DOUBLE_QUOTE);
    return buffer;
  }

  unquotedString(endedBy: number[], illegal: number[]): string {
    const start = this.offset;
    this.advance();

    let next = this.currentCharCode();
    while (endedBy.indexOf(next) == -1) {
      if (illegal.indexOf(next) != -1) {
        this.error('illegal char, should require escaping');
      }

      this.advance();
      next = this.currentCharCode();
    }

    return this.source.substring(start, this.offset);
  }

  checkAtEnd() {
    if (this.offset < this.source.length) {
      this.error('Unexpected trailing text');
    }
  }

  // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
  parseArray<T>(parseElement: (value: string) => T, delimiter: number = CHAR_CODE_COMMA): ElementOrArray<T>[] {
    const array = this.parseArrayInner(delimiter, parseElement);
    this.checkAtEnd();
    return array;
  }

  // Recursively parses a (potentially multi-dimensional) array.
  private parseArrayInner<T>(delimiter: number, parseElement: (value: string) => T): ElementOrArray<T>[] {
    this.consume(CHAR_CODE_LEFT_BRACE);
    if (this.maybeConsume(CHAR_CODE_RIGHT_BRACE)) {
      return []; // Empty array ({})
    }

    const elements: ElementOrArray<T>[] = [];
    do {
      // Parse a value in the array. This can either be an escaped string, an unescaped string, or a nested array.
      const currentChar = this.currentCharCode();
      if (currentChar == CHAR_CODE_LEFT_BRACE) {
        // Nested array
        elements.push(this.parseArrayInner(delimiter, parseElement));
      } else if (currentChar == CHAR_CODE_DOUBLE_QUOTE) {
        elements.push(parseElement(this.quotedString()));
      } else {
        const value = this.unquotedString(
          [delimiter, CHAR_CODE_RIGHT_BRACE],
          [CHAR_CODE_DOUBLE_QUOTE, CHAR_CODE_LEFT_BRACE]
        );
        elements.push(value == 'NULL' ? null : parseElement(value));
      }
    } while (this.maybeConsume(delimiter));

    this.consume(CHAR_CODE_RIGHT_BRACE);
    return elements;
  }

  // https://www.postgresql.org/docs/current/rowtypes.html#ROWTYPES-IO-SYNTAX
  parseComposite(onElement: (value: string | null) => void) {
    this.consume(CHAR_CODE_LEFT_PAREN);
    do {
      // Parse a composite value. This can either be an escaped string, an unescaped string, or an empty string.
      const currentChar = this.currentCharCode();
      if (currentChar == CHAR_CODE_COMMA) {
        // Empty value. The comma is consumed by the while() below.
        onElement(null);
      } else if (currentChar == CHAR_CODE_RIGHT_PAREN) {
        // Empty value before end. The right parent is consumed by the line after the loop.
        onElement(null);
      } else if (currentChar == CHAR_CODE_DOUBLE_QUOTE) {
        onElement(this.quotedString(true));
      } else {
        const value = this.unquotedString(
          [CHAR_CODE_COMMA, CHAR_CODE_RIGHT_PAREN],
          [CHAR_CODE_DOUBLE_QUOTE, CHAR_CODE_LEFT_PAREN]
        );
        onElement(value);
      }
    } while (this.maybeConsume(CHAR_CODE_COMMA));
    this.consume(CHAR_CODE_RIGHT_PAREN);
    this.checkAtEnd();
  }

  // https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-IO
  private parseRangeInner<T>(parseInner: (value: string) => T): Range<T> {
    const empty = 'empty';

    // Parse [ or ( to start the range
    let lowerBoundExclusive;
    switch (this.currentCharCode()) {
      case CHAR_CODE_LEFT_PAREN:
        lowerBoundExclusive = true;
        this.advance();
        break;
      case CHAR_CODE_LEFT_BRACKET:
        lowerBoundExclusive = false;
        this.advance();
        break;
      case empty.charCodeAt(0):
        // Consume the string "empty"
        for (let i = 0; i < empty.length; i++) {
          this.consume(empty.charCodeAt(i));
        }
        return empty;
      default:
        this.error('Expected [, ( or string empty');
    }

    // Parse value until comma (which may be empty)
    let lower = null;
    if (this.currentCharCode() == CHAR_CODE_DOUBLE_QUOTE) {
      lower = parseInner(this.quotedString());
    } else if (this.currentCharCode() != CHAR_CODE_COMMA) {
      lower = parseInner(this.unquotedString([CHAR_CODE_COMMA], []));
    }

    this.consume(CHAR_CODE_COMMA);

    let upper = null;
    if (this.currentCharCode() == CHAR_CODE_DOUBLE_QUOTE) {
      upper = parseInner(this.quotedString());
    } else if (this.currentCharCode() != CHAR_CODE_RIGHT_PAREN && this.currentCharCode() != CHAR_CODE_RIGHT_BRACKET) {
      upper = parseInner(this.unquotedString([CHAR_CODE_RIGHT_PAREN, CHAR_CODE_RIGHT_BRACKET], []));
    }

    let upperBoundExclusive;
    switch (this.currentCharCode()) {
      case CHAR_CODE_RIGHT_PAREN:
        upperBoundExclusive = true;
        this.advance();
        break;
      case CHAR_CODE_RIGHT_BRACKET:
        upperBoundExclusive = false;
        this.advance();
        break;
      default:
        this.error('Expected ] or )');
    }

    return {
      lower: lower,
      upper: upper,
      lower_exclusive: lowerBoundExclusive,
      upper_exclusive: upperBoundExclusive
    };
  }

  parseRange<T>(parseInner: (value: string) => T): Range<T> {
    const range = this.parseRangeInner(parseInner);
    this.checkAtEnd();
    return range;
  }

  parseMultiRange<T>(parseInner: (value: string) => T): Range<T>[] {
    this.consume(CHAR_CODE_LEFT_BRACE);
    if (this.maybeConsume(CHAR_CODE_RIGHT_BRACE)) {
      return [];
    }

    const values: Range<T>[] = [];
    do {
      values.push(this.parseRangeInner(parseInner));
    } while (this.maybeConsume(CHAR_CODE_COMMA));

    this.consume(CHAR_CODE_RIGHT_BRACE);
    this.checkAtEnd();
    return values;
  }
}

export type Range<T> =
  | {
      lower: T | null;
      upper: T | null;
      lower_exclusive: boolean;
      upper_exclusive: boolean;
    }
  | 'empty';

export type ElementOrArray<T> = null | T | ElementOrArray<T>[];

const CHAR_CODE_DOUBLE_QUOTE = 0x22;
const STRING_DOUBLE_QUOTE = '"';
const CHAR_CODE_BACKSLASH = 0x5c;
export const CHAR_CODE_COMMA = 0x2c;
export const CHAR_CODE_SEMICOLON = 0x3b;
const CHAR_CODE_LEFT_BRACE = 0x7b;
const CHAR_CODE_RIGHT_BRACE = 0x7d;
const CHAR_CODE_LEFT_PAREN = 0x28;
const CHAR_CODE_RIGHT_PAREN = 0x29;
const CHAR_CODE_LEFT_BRACKET = 0x5b;
const CHAR_CODE_RIGHT_BRACKET = 0x5d;
