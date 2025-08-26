import { describe, expect, test } from 'vitest';
import {
  SequenceListener,
  Delimiters,
  CHAR_CODE_COMMA,
  CHAR_CODE_RIGHT_BRACE,
  CHAR_CODE_LEFT_BRACE,
  decodeSequence,
  CHAR_CODE_LEFT_PAREN,
  CHAR_CODE_RIGHT_PAREN
} from '../src/index';

test('empty array', () => {
  expect(recordParseEvents('{}', arrayDelimiters)).toStrictEqual(['structureStart', 'structureEnd']);
});

test('regular array', () => {
  expect(recordParseEvents('{foo,bar}', arrayDelimiters)).toStrictEqual([
    'structureStart',
    'foo',
    'bar',
    'structureEnd'
  ]);
});

test('null', () => {
  expect(recordParseEvents('{NULL}', arrayDelimiters)).toStrictEqual(['structureStart', null, 'structureEnd']);
  expect(recordParseEvents('{null}', arrayDelimiters)).toStrictEqual(['structureStart', 'null', 'structureEnd']);
});

test('escaped', () => {
  expect(recordParseEvents('{""}', arrayDelimiters)).toStrictEqual(['structureStart', '', 'structureEnd']);
  expect(recordParseEvents('{"foo"}', arrayDelimiters)).toStrictEqual(['structureStart', 'foo', 'structureEnd']);
  expect(recordParseEvents('{"fo\\"o,"}', arrayDelimiters)).toStrictEqual(['structureStart', 'fo"o,', 'structureEnd']);
  expect(recordParseEvents('{"fo\\\\o,"}', arrayDelimiters)).toStrictEqual([
    'structureStart',
    'fo\\o,',
    'structureEnd'
  ]);
});

test('nested array', () => {
  expect(
    recordParseEvents('{0,{0,{}}}', arrayDelimiters, (c) => (c == CHAR_CODE_LEFT_BRACE ? arrayDelimiters : null))
  ).toStrictEqual([
    'structureStart',
    '0',
    'structureStart',
    '0',
    'structureStart',
    'structureEnd',
    'structureEnd',
    'structureEnd'
  ]);
});

test('other structures', () => {
  const outerDelimiters: Delimiters = {
    openingCharCode: CHAR_CODE_LEFT_PAREN,
    closingCharCode: CHAR_CODE_RIGHT_PAREN,
    delimiterCharCode: CHAR_CODE_COMMA
  };

  expect(recordParseEvents('()', outerDelimiters)).toStrictEqual(['structureStart', 'structureEnd']);
  expect(
    recordParseEvents('(foo,bar,{baz})', outerDelimiters, (c) => (c == CHAR_CODE_LEFT_BRACE ? arrayDelimiters : null))
  ).toStrictEqual(['structureStart', 'foo', 'bar', 'structureStart', 'baz', 'structureEnd', 'structureEnd']);
});

describe('errors', () => {
  test('unclosed array', () => {
    expect(() => recordParseEvents('{', arrayDelimiters)).toThrow(/Unexpected end of input/);
  });

  test('trailing data', () => {
    expect(() => recordParseEvents('{foo,bar}baz', arrayDelimiters)).toThrow(/Unexpected trailing text/);
  });

  test('improper escaped string', () => {
    expect(() => recordParseEvents('{foo,"bar}', arrayDelimiters)).toThrow(/Unexpected end of input/);
  });

  test('illegal escape sequence', () => {
    expect(() => recordParseEvents('{foo,"b\\ar"}', arrayDelimiters)).toThrow(
      /Expected escaped double quote or escaped backslash/
    );
  });

  test('illegal delimiter in value', () => {
    expect(() => recordParseEvents('{foo{}', arrayDelimiters)).toThrow(/illegal char, should require escaping/);
  });

  test('illegal quote in value', () => {
    expect(() => recordParseEvents('{foo"}', arrayDelimiters)).toThrow(/illegal char, should require escaping/);
  });
});

const arrayDelimiters: Delimiters = {
  openingCharCode: CHAR_CODE_LEFT_BRACE,
  closingCharCode: CHAR_CODE_RIGHT_BRACE,
  delimiterCharCode: CHAR_CODE_COMMA
};

function recordParseEvents(
  source: string,
  delimiters: Delimiters,
  maybeParseSubStructure?: (firstChar: number) => Delimiters | null
) {
  maybeParseSubStructure ??= (_) => null;

  const events: any[] = [];
  const listener: SequenceListener = {
    maybeParseSubStructure,
    onStructureStart: () => events.push('structureStart'),
    onValue: (value) => {
      events.push(value);
    },
    onStructureEnd: () => events.push('structureEnd')
  };

  decodeSequence({ source, delimiters, listener });
  return events;
}
