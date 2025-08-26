import { describe, expect, test } from 'vitest';
import {
  SequenceListener,
  Delimiters,
  CHAR_CODE_LEFT_BRACE,
  decodeSequence,
  arrayDelimiters,
  COMPOSITE_DELIMITERS
} from '../src/index';

test('empty array', () => {
  expect(recordParseEvents('{}', arrayDelimiters())).toStrictEqual(['structureStart', 'structureEnd']);
});

test('regular array', () => {
  expect(recordParseEvents('{foo,bar}', arrayDelimiters())).toStrictEqual([
    'structureStart',
    'foo',
    'bar',
    'structureEnd'
  ]);
});

test('null', () => {
  expect(recordParseEvents('{NULL}', arrayDelimiters())).toStrictEqual(['structureStart', null, 'structureEnd']);
  expect(recordParseEvents('{null}', arrayDelimiters())).toStrictEqual(['structureStart', 'null', 'structureEnd']);
});

test('escaped', () => {
  expect(recordParseEvents('{""}', arrayDelimiters())).toStrictEqual(['structureStart', '', 'structureEnd']);
  expect(recordParseEvents('{"foo"}', arrayDelimiters())).toStrictEqual(['structureStart', 'foo', 'structureEnd']);
  expect(recordParseEvents('{"fo\\"o,"}', arrayDelimiters())).toStrictEqual([
    'structureStart',
    'fo"o,',
    'structureEnd'
  ]);
  expect(recordParseEvents('{"fo\\\\o,"}', arrayDelimiters())).toStrictEqual([
    'structureStart',
    'fo\\o,',
    'structureEnd'
  ]);
});

test('nested array', () => {
  expect(
    recordParseEvents('{0,{0,{}}}', arrayDelimiters(), (c) => (c == CHAR_CODE_LEFT_BRACE ? arrayDelimiters() : null))
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
  expect(recordParseEvents('()', COMPOSITE_DELIMITERS)).toStrictEqual(['structureStart', 'structureEnd']);
  expect(
    recordParseEvents('(foo,bar,{baz})', COMPOSITE_DELIMITERS, (c) =>
      c == CHAR_CODE_LEFT_BRACE ? arrayDelimiters() : null
    )
  ).toStrictEqual(['structureStart', 'foo', 'bar', 'structureStart', 'baz', 'structureEnd', 'structureEnd']);
});

test('composite null entries', () => {
  // CREATE TYPE nested AS (a BOOLEAN, b BOOLEAN); SELECT (NULL, NULL)::nested;
  expect(recordParseEvents('(,)', COMPOSITE_DELIMITERS)).toStrictEqual(['structureStart', null, null, 'structureEnd']);

  // CREATE TYPE triple AS (a BOOLEAN, b BOOLEAN, c BOOLEAN); SELECT (NULL, NULL, NULL)::triple
  expect(recordParseEvents('(,,)', COMPOSITE_DELIMITERS)).toStrictEqual([
    'structureStart',
    null,
    null,
    null,
    'structureEnd'
  ]);

  // NOTE: It looks like a single-element composite type has (NULL) encoded as NULL instead of a string like ()
});

test('composite string escaping', () => {
  expect(recordParseEvents('("foo""bar")', COMPOSITE_DELIMITERS)).toStrictEqual([
    'structureStart',
    'foo"bar',
    'structureEnd'
  ]);

  expect(recordParseEvents('("")', COMPOSITE_DELIMITERS)).toStrictEqual(['structureStart', '', 'structureEnd']);
});

describe('errors', () => {
  test('unclosed array', () => {
    expect(() => recordParseEvents('{', arrayDelimiters())).toThrow(/Unexpected end of input/);
  });

  test('trailing data', () => {
    expect(() => recordParseEvents('{foo,bar}baz', arrayDelimiters())).toThrow(/Unexpected trailing text/);
  });

  test('improper escaped string', () => {
    expect(() => recordParseEvents('{foo,"bar}', arrayDelimiters())).toThrow(/Unexpected end of input/);
  });

  test('illegal escape sequence', () => {
    expect(() => recordParseEvents('{foo,"b\\ar"}', arrayDelimiters())).toThrow(
      /Expected escaped double quote or escaped backslash/
    );
  });

  test('illegal delimiter in value', () => {
    expect(() => recordParseEvents('{foo{}', arrayDelimiters())).toThrow(/illegal char, should require escaping/);
  });

  test('illegal quote in value', () => {
    expect(() => recordParseEvents('{foo"}', arrayDelimiters())).toThrow(/illegal char, should require escaping/);
  });
});

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
