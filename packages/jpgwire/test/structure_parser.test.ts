import { describe, expect, test } from 'vitest';
import { CHAR_CODE_COMMA, CHAR_CODE_SEMICOLON, StructureParser } from '../src/index';

describe('StructureParser', () => {
  describe('array', () => {
    const parseArray = (source: string, delimiter: number = CHAR_CODE_COMMA) => {
      return new StructureParser(source).parseArray((source) => source, delimiter);
    };

    test('empty', () => {
      expect(parseArray('{}')).toStrictEqual([]);
    });

    test('regular', () => {
      expect(parseArray('{foo,bar}')).toStrictEqual(['foo', 'bar']);
    });

    test('custom delimiter', () => {
      expect(parseArray('{foo;bar}', CHAR_CODE_SEMICOLON)).toStrictEqual(['foo', 'bar']);
    });

    test('null elements', () => {
      expect(parseArray('{null}')).toStrictEqual(['null']);
      expect(parseArray('{NULL}')).toStrictEqual([null]);
    });

    test('escaped', () => {
      expect(parseArray('{""}')).toStrictEqual(['']);
      expect(parseArray('{"foo"}')).toStrictEqual(['foo']);
      expect(parseArray('{"fo\\"o"}')).toStrictEqual(['fo"o']);
      expect(parseArray('{"fo\\\\o"}')).toStrictEqual(['fo\\o']);

      // Regression test for https://github.com/powersync-ja/powersync-service/issues/419
      let largeStringValue = '';
      for (let i = 0; i < 65000; i++) {
        largeStringValue += 'hello world';
      }

      expect(parseArray(`{"${largeStringValue}"}`)).toStrictEqual([largeStringValue]);
    });

    test('nested', () => {
      expect(parseArray('{0,{0,{}}}')).toStrictEqual(['0', ['0', []]]);
    });

    test('trailing data', () => {
      expect(() => parseArray('{foo}bar')).toThrow(/Unexpected trailing text/);
    });

    test('unclosed array', () => {
      expect(() => parseArray('{')).toThrow(/Unexpected end of input/);
    });

    test('improper escaped string', () => {
      expect(() => parseArray('{foo,"bar}')).toThrow(/Unexpected end of input/);
    });

    test('illegal escape sequence', () => {
      expect(() => parseArray('{foo,"b\\ar"}')).toThrow(/Expected escaped double quote or escaped backslash/);
    });

    test('illegal delimiter in value', () => {
      expect(() => parseArray('{foo{}')).toThrow(/illegal char, should require escaping/);
    });

    test('illegal quote in value', () => {
      expect(() => parseArray('{foo"}')).toThrow(/illegal char, should require escaping/);
    });
  });

  describe('composite', () => {
    const parseComposite = (source: string) => {
      const events: any[] = [];
      new StructureParser(source).parseComposite((e) => events.push(e));
      return events;
    };

    test('empty composite', () => {
      // Both of the following render as '()':
      // create type foo as (); select ROW()::foo;  create type foo2 as (foo integer);
      // SELECT ROW()::foo, ROW(NULL)::foo2;
      // Here, we resolve the ambiguity by parsing () as an one-element composite - callers need to be aware of this.
      expect(parseComposite('()')).toStrictEqual([null]);
    });

    test('only null entries', () => {
      expect(parseComposite('(,)')).toStrictEqual([null, null]);
      expect(parseComposite('(,,)')).toStrictEqual([null, null, null]);
    });

    test('null before element', () => {
      expect(parseComposite('(,foo)')).toStrictEqual([null, 'foo']);
    });

    test('null after element', () => {
      expect(parseComposite('(foo,)')).toStrictEqual(['foo', null]);
    });

    test('nested', () => {
      expect(parseComposite('(foo,bar,{baz})')).toStrictEqual(['foo', 'bar', '{baz}']);
    });

    test('escaped strings', () => {
      expect(parseComposite('("foo""bar")')).toStrictEqual(['foo"bar']);
      expect(parseComposite('("")')).toStrictEqual(['']);
    });
  });

  describe('range', () => {
    const parseIntRange = (source: string) => {
      return new StructureParser(source).parseRange((source) => Number(source));
    };

    test('empty', () => {
      // select '(3, 3)'::int4range
      expect(parseIntRange('empty')).toStrictEqual('empty');
    });

    test('regular', () => {
      expect(parseIntRange('[1,2]')).toStrictEqual({
        lower: 1,
        upper: 2,
        lower_exclusive: false,
        upper_exclusive: false
      });
      expect(parseIntRange('[1,2)')).toStrictEqual({
        lower: 1,
        upper: 2,
        lower_exclusive: false,
        upper_exclusive: true
      });
      expect(parseIntRange('(1,2]')).toStrictEqual({
        lower: 1,
        upper: 2,
        lower_exclusive: true,
        upper_exclusive: false
      });
      expect(parseIntRange('(1,2)')).toStrictEqual({
        lower: 1,
        upper: 2,
        lower_exclusive: true,
        upper_exclusive: true
      });
    });

    test('no lower bound', () => {
      expect(parseIntRange('(,3]')).toStrictEqual({
        lower: null,
        upper: 3,
        lower_exclusive: true,
        upper_exclusive: false
      });
    });

    test('no upper bound', () => {
      expect(parseIntRange('(3,]')).toStrictEqual({
        lower: 3,
        upper: null,
        lower_exclusive: true,
        upper_exclusive: false
      });
    });

    test('no bounds', () => {
      expect(parseIntRange('(,)')).toStrictEqual({
        lower: null,
        upper: null,
        lower_exclusive: true,
        upper_exclusive: true
      });
    });
  });

  describe('multirange', () => {
    const parseIntMultiRange = (source: string) => {
      return new StructureParser(source).parseMultiRange((source) => Number(source));
    };

    test('empty', () => {
      expect(parseIntMultiRange('{}')).toStrictEqual([]);
    });

    test('single', () => {
      expect(parseIntMultiRange('{[3,7)}')).toStrictEqual([
        {
          lower: 3,
          upper: 7,
          lower_exclusive: false,
          upper_exclusive: true
        }
      ]);
    });

    test('multiple', () => {
      expect(parseIntMultiRange('{[3,7),[8,9)}')).toStrictEqual([
        {
          lower: 3,
          upper: 7,
          lower_exclusive: false,
          upper_exclusive: true
        },
        {
          lower: 8,
          upper: 9,
          lower_exclusive: false,
          upper_exclusive: true
        }
      ]);
    });
  });
});
