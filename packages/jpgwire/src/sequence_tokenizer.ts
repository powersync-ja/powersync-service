export interface SequenceListener {
  /**
   * Invoked whenever the tokenizer has begun decoding a structure (that is, once in the beginning and then for
   * every sub-structure).
   */
  onStructureStart: () => void;
  /**
   * Invoked whenever the tokenizer has finished parsing a value that isn't a nested structure.
   *
   * @param value the raw value, with escape characters related to the outer structure being removed. `null` for the
   * literal text {@link Delimiters.nullLiteral}.
   */
  onValue: (value: string | null) => void;
  /**
   * Invoked whenever a tokenizer has completed a structure (meaning that it's closing brace has been consumed).
   */
  onStructureEnd: () => void;
}

export interface Delimiters {
  /** The char code opening the structure, e.g. `{` for arrays */
  openingCharCode: number;
  /** The char code opening the structure, e.g. `}` for arrays */
  closingCharCode: number;
  /** The char code opening the structure, e.g. `,` */
  delimiterCharCode: number;
  /**
   * Whether two subsequent double quotes are allowed to escape values.
   *
   * This is the case for composite values, but not for arrays. */
  allowEscapingWithDoubleDoubleQuote: boolean;
  /** Whether empty values are allowed, e.g. `(,)` */
  allowEmpty: boolean;
  /** The string literal that denotes a `NULL` value. */
  nullLiteral: string;
  /** Whether values can be nested sub-structures. */
  multiDimensional: boolean;
}

export interface DecodeSequenceOptions {
  /** The original text to parse */
  source: string;
  /** Delimiters for the outermost structure */
  delimiters: Delimiters;
  /** Callbacks to control how values are interpreted and how substructures should be parsed. */
  listener: SequenceListener;
}

/**
 * Decodes a sequence of values, such as arrays or composite types represented as text.
 *
 * It supports nested arrays and different options for escaping values needed for arrays and composites.
 */
export function decodeSequence(options: DecodeSequenceOptions) {
  let { source, delimiters, listener } = options;

  const stateStackTail: SequenceDecoderState[] = [];
  let currentState: SequenceDecoderState = SequenceDecoderState.BEFORE_SEQUENCE as SequenceDecoderState;

  consumeChar: for (let i = 0; i < source.length; i++) {
    function error(msg: string): never {
      throw new Error(`Error decoding Postgres sequence at position ${i}: ${msg}`);
    }

    function check(expected: number) {
      if (charCode != expected) {
        error(`Expected ${String.fromCharCode(expected)}, got ${String.fromCharCode(charCode)}`);
      }
    }

    function peek(): number {
      if (i == source.length - 1) {
        error('Unexpected end of input');
      }

      return source.charCodeAt(i + 1);
    }

    function advance(): number {
      const value = peek();
      i++;
      return value;
    }

    function quotedString(): string {
      const start = i;
      const charCodes: number[] = [];
      let previousWasBackslash = false;

      while (true) {
        const next = advance();
        if (previousWasBackslash) {
          if (next != CHAR_CODE_DOUBLE_QUOTE && next != CHAR_CODE_BACKSLASH) {
            error('Expected escaped double quote or escaped backslash');
          }
          charCodes.push(next);
          previousWasBackslash = false;
        } else if (next == CHAR_CODE_DOUBLE_QUOTE) {
          if (i != start && delimiters.allowEscapingWithDoubleDoubleQuote) {
            // If the next character is also a double quote, that escapes a single double quote
            if (i < source.length - 1 && peek() == CHAR_CODE_DOUBLE_QUOTE) {
              i++;
              charCodes.push(CHAR_CODE_DOUBLE_QUOTE);
              continue;
            }
          }

          break; // End of string.
        } else if (next == CHAR_CODE_BACKSLASH) {
          previousWasBackslash = true;
        } else {
          charCodes.push(next);
        }
      }

      return String.fromCharCode(...charCodes);
    }

    function unquotedString(): string {
      const start = i;
      let next = peek();
      while (next != delimiters.delimiterCharCode && next != delimiters.closingCharCode) {
        if (next == delimiters.openingCharCode || next == CHAR_CODE_DOUBLE_QUOTE) {
          error('illegal char, should require escaping');
        }

        i++;
        next = peek();
      }

      return source.substring(start, i + 1);
    }

    function endStructure() {
      currentState = SequenceDecoderState.AFTER_SEQUENCE;
      listener.onStructureEnd();
      if (stateStackTail.length > 0) {
        currentState = stateStackTail.pop()!;
      }
    }

    const charCode = source.charCodeAt(i);
    switch (currentState) {
      case SequenceDecoderState.BEFORE_SEQUENCE:
        check(delimiters.openingCharCode);
        currentState = SequenceDecoderState.BEFORE_ELEMENT_OR_END;
        listener.onStructureStart();
        break;
      case SequenceDecoderState.BEFORE_ELEMENT_OR_END:
        if (charCode == delimiters.closingCharCode) {
          endStructure();
          continue consumeChar;
        }
      // No break between these, end has been handled.
      case SequenceDecoderState.BEFORE_ELEMENT:
        // What follows is either NULL, a non-empty string value that does not contain delimiters, or an escaped string
        // value.
        if (charCode == CHAR_CODE_DOUBLE_QUOTE) {
          const value = quotedString();
          listener.onValue(value);
        } else if (charCode == delimiters.delimiterCharCode || charCode == delimiters.closingCharCode) {
          if (!delimiters.allowEmpty) {
            error('invalid empty element');
          }

          listener.onValue('' == delimiters.nullLiteral ? null : '');
          if (charCode == delimiters.delimiterCharCode) {
            // Since this is a comma, there'll be an element afterwards
            currentState = SequenceDecoderState.BEFORE_ELEMENT;
          } else {
            endStructure();
          }
          break;
        } else {
          if (delimiters.multiDimensional && charCode == delimiters.openingCharCode) {
            currentState = SequenceDecoderState.AFTER_ELEMENT;
            listener.onStructureStart();
            stateStackTail.push(currentState);

            // We've consumed the opening delimiter already, so the inner state can either parse an element or
            // immediately close.
            currentState = SequenceDecoderState.BEFORE_ELEMENT_OR_END;
            continue consumeChar;
          } else {
            // Parse the current cell as one value
            const value = unquotedString();
            listener.onValue(value == delimiters.nullLiteral ? null : value);
          }
        }
        currentState = SequenceDecoderState.AFTER_ELEMENT;
        break;
      case SequenceDecoderState.AFTER_ELEMENT:
        // There can be another element here, or a closing brace
        if (charCode == delimiters.closingCharCode) {
          endStructure();
        } else {
          check(delimiters.delimiterCharCode);
          currentState = SequenceDecoderState.BEFORE_ELEMENT;
        }
        break;
      case SequenceDecoderState.AFTER_SEQUENCE:
        error('Unexpected trailing text');
      default:
        error('Internal error: Unknown state');
    }
  }

  if (currentState != SequenceDecoderState.AFTER_SEQUENCE) {
    throw Error('Unexpected end of input');
  }
}

export type ElementOrArray<T> = null | T | ElementOrArray<T>[];

export interface DecodeArrayOptions<T> {
  source: string;
  delimiterCharCode?: number;
  decodeElement: (source: string) => T;
}

/**
 * A variant of {@link decodeSequence} that specifically decodes arrays.
 *
 * The {@link DecodeArrayOptions.decodeElement} method is responsible for parsing individual values with the array,
 * this method automatically recognizes multidimensional arrays and parses them appropriately.
 */
export function decodeArray<T>(options: DecodeArrayOptions<T>): ElementOrArray<T>[] {
  let results: ElementOrArray<T>[] = [];
  const stack: ElementOrArray<T>[][] = [];

  const listener: SequenceListener = {
    onStructureStart: () => {
      // We're parsing a new array
      stack.push([]);
    },
    onValue: function (value: string | null): void {
      // Atomic (non-array) value, add to current array.
      stack[stack.length - 1].push(value != null ? options.decodeElement(value) : null);
    },
    onStructureEnd: () => {
      // We're done parsing an array.
      const subarray = stack.pop()!;
      if (stack.length == 0) {
        // We are done with the outermost array, set results.
        results = subarray;
      } else {
        // We were busy parsing a nested array, continue outer array.
        stack[stack.length - 1].push(subarray);
      }
    }
  };

  decodeSequence({
    source: options.source,
    listener,
    delimiters: arrayDelimiters(options.delimiterCharCode)
  });

  return results!;
}

const CHAR_CODE_DOUBLE_QUOTE = 0x22;
const CHAR_CODE_BACKSLASH = 0x5c;
export const CHAR_CODE_COMMA = 0x2c;
export const CHAR_CODE_LEFT_BRACE = 0x7b;
export const CHAR_CODE_RIGHT_BRACE = 0x7d;
export const CHAR_CODE_LEFT_PAREN = 0x28;
export const CHAR_CODE_RIGHT_PAREN = 0x29;

// https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
export function arrayDelimiters(delimiterCharCode: number = CHAR_CODE_COMMA): Delimiters {
  return {
    openingCharCode: CHAR_CODE_LEFT_BRACE,
    closingCharCode: CHAR_CODE_RIGHT_BRACE,
    allowEscapingWithDoubleDoubleQuote: false,
    nullLiteral: 'NULL',
    allowEmpty: false, // Empty values must be escaped
    delimiterCharCode,
    multiDimensional: true
  };
}

// https://www.postgresql.org/docs/current/rowtypes.html#ROWTYPES-IO-SYNTAX
export const COMPOSITE_DELIMITERS = Object.freeze({
  openingCharCode: CHAR_CODE_LEFT_PAREN,
  closingCharCode: CHAR_CODE_RIGHT_PAREN,
  delimiterCharCode: CHAR_CODE_COMMA,
  allowEscapingWithDoubleDoubleQuote: true,
  allowEmpty: true, // Empty values encode NULL
  nullLiteral: '',
  multiDimensional: false
} satisfies Delimiters);

enum SequenceDecoderState {
  BEFORE_SEQUENCE = 1,
  BEFORE_ELEMENT_OR_END = 2,
  BEFORE_ELEMENT = 3,
  AFTER_ELEMENT = 4,
  AFTER_SEQUENCE = 5
}
