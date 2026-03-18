import { Scalar } from 'yaml';
import { describe, expect, test } from 'vitest';
import { buildParsedToSourceValueMap } from '../../src/yaml_scalar_map.js';

describe('buildParsedToSourceValueMap', () => {
  describe('PLAIN scalars', () => {
    test('single line (identity)', () => {
      const src = 'hello world';
      const map = buildParsedToSourceValueMap(src, Scalar.PLAIN);
      // Each parsed char maps to the same source position
      expect(map).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, src.length]);
    });

    test('folded newline maps to start of next line content', () => {
      // "foo bar" after folding
      const src = 'foo  \n  bar';
      const map = buildParsedToSourceValueMap(src, Scalar.PLAIN);
      // trailing whitespace on first line is stripped (positions 3,4 are spaces)
      expect(map[0]).toBe(0); // f
      expect(map[1]).toBe(1); // o
      expect(map[2]).toBe(2); // o
      expect(map[3]).toBe(8); // folded space - maps to start of "bar" in source
      expect(map[4]).toBe(8); // b
      expect(map[5]).toBe(9); // a
      expect(map[6]).toBe(10); // r
      expect(map[7]).toBe(src.length); // sentinel
    });

    test('\\r\\n line endings count as one newline', () => {
      const src = 'foo\r\nbar';
      const map = buildParsedToSourceValueMap(src, Scalar.PLAIN);
      // parsed: "foo bar" (7 chars)
      expect(map[0]).toBe(0); // f
      expect(map[1]).toBe(1); // o
      expect(map[2]).toBe(2); // o
      expect(map[3]).toBe(5); // folded space - maps to start of "bar" in source
      expect(map[4]).toBe(5); // b
      expect(map[5]).toBe(6); // a
      expect(map[6]).toBe(7); // r
      expect(map[7]).toBe(src.length); // sentinel
    });
  });

  describe('QUOTE_DOUBLE scalars', () => {
    test('no escapes', () => {
      const src = 'hello';
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_DOUBLE);
      expect(map[0]).toBe(0); // h
      expect(map[1]).toBe(1); // e
      expect(map[2]).toBe(2); // l
      expect(map[3]).toBe(3); // l
      expect(map[4]).toBe(4); // o
      expect(map[5]).toBe(5); // sentinel
    });

    test('\\n escape: two source chars map to one parsed char', () => {
      const src = 'a\\nb';
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_DOUBLE);
      expect(map[0]).toBe(0); // a
      expect(map[1]).toBe(1); // \n (backslash)
      expect(map[2]).toBe(3); // b
      expect(map[3]).toBe(4); // sentinel
    });

    test('\\x escape', () => {
      const src = '\\x41b'; // \x41 = 'A'
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_DOUBLE);
      expect(map[0]).toBe(0); // \x41 (backslash)
      expect(map[1]).toBe(4); // b
      expect(map[2]).toBe(5); // sentinel
    });

    test('\\u escape', () => {
      const src = '\\u0041b'; // \u0041 = 'A'
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_DOUBLE);
      expect(map[0]).toBe(0); // \u0041 (backslash)
      expect(map[1]).toBe(6); // b
      expect(map[2]).toBe(7); // sentinel
    });

    test('\\U escape', () => {
      const src = '\\U00000041b'; // \U00000041 = 'A'
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_DOUBLE);
      expect(map[0]).toBe(0); // \U00000041 (backslash)
      expect(map[1]).toBe(10); // b
      expect(map[2]).toBe(11); // sentinel
    });
  });

  describe('QUOTE_SINGLE scalars', () => {
    test('no escapes', () => {
      const src = 'hello';
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_SINGLE);
      expect(map[0]).toBe(0);
      expect(map[4]).toBe(4);
      expect(map[5]).toBe(5); // sentinel
    });

    test("'' escape maps to first quote position", () => {
      const src = "it''s"; // parsed: "it's"
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_SINGLE);
      expect(map[0]).toBe(0); // i
      expect(map[1]).toBe(1); // t
      expect(map[2]).toBe(2); // '' (first quote)
      expect(map[3]).toBe(4); // s
      expect(map[4]).toBe(5); // sentinel
    });
  });

  describe('BLOCK_LITERAL / BLOCK_FOLDED scalars', () => {
    test('strips common indentation', () => {
      // src passed here is the content after the header line (| or >)
      // two-space indent on each line
      const src = '  hello\n  world\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_LITERAL);
      // parsed "hello\nworld\n" — positions after stripping 2-char indent
      expect(map[0]).toBe(2); // h
      expect(map[1]).toBe(3); // e
      expect(map[2]).toBe(4); // l
      expect(map[3]).toBe(5); // l
      expect(map[4]).toBe(6); // o
      expect(map[5]).toBe(7); // \n
      expect(map[6]).toBe(10); // w (skips indentation)
    });

    test('empty lines are not skipped past', () => {
      // An empty line (just \n) must not cause contentStart to jump into the next line
      const src = '  hello\n\n  world\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_LITERAL);
      expect(map[0]).toBe(2); // h (skips indentation)
      expect(map[5]).toBe(7); // first \n
      expect(map[6]).toBe(8); // second \n
      expect(map[7]).toBe(11); // w (skips indentation)
    });

    test('same indentation stripping for BLOCK_FOLDED', () => {
      const src = '  foo\n  bar\n';
      const mapLiteral = buildParsedToSourceValueMap(src, Scalar.BLOCK_LITERAL);
      const mapFolded = buildParsedToSourceValueMap(src, Scalar.BLOCK_FOLDED);
      expect(mapLiteral).toEqual(mapFolded);
    });
  });

  describe('invalid arguments', () => {
    test('unknown type returns empty map', () => {
      const map = buildParsedToSourceValueMap('hello', 'FLOW_MAP');
      expect(map).toEqual([]);
    });

    test('null/undefined type returns empty map', () => {
      expect(buildParsedToSourceValueMap('hello', null)).toEqual([]);
      expect(buildParsedToSourceValueMap('hello', undefined)).toEqual([]);
    });
  });
});
