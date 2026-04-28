import { describe, expect, test } from 'vitest';
import { Scalar } from 'yaml';
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

    test('escaped line break: backslash + newline + leading whitespace → nothing', () => {
      // parsed: "foobar" (no folded space for escaped newlines)
      const src = 'foo\\\n  bar';
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_DOUBLE);
      expect(map[0]).toBe(0); // f
      expect(map[1]).toBe(1); // o
      expect(map[2]).toBe(2); // o
      // nothing produced for \ + \n + indent
      expect(map[3]).toBe(7); // b
      expect(map[4]).toBe(8); // a
      expect(map[5]).toBe(9); // r
      expect(map[6]).toBe(src.length); // sentinel
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

    test('literal newline folds to space, stripping continuation-line indentation', () => {
      // "foo\n  bar" (literal newline + 2-space indent on continuation line)
      // parsed: "foo bar"
      // src positions: 0=f, 1=o, 2=o, 3=\n, 4=' ', 5=' ', 6=b, 7=a, 8=r
      const src = 'foo\n  bar';
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_DOUBLE);
      expect(map[0]).toBe(0); // f
      expect(map[1]).toBe(1); // o
      expect(map[2]).toBe(2); // o
      expect(map[3]).toBe(3); // folded space → \n at pos 3
      expect(map[4]).toBe(6); // b (leading '  ' stripped, so pos 6)
      expect(map[5]).toBe(7); // a
      expect(map[6]).toBe(8); // r
      expect(map[7]).toBe(src.length); // sentinel
    });

    test('literal newline with blank lines', () => {
      // "foo\n\n  bar": blank line produces \n, content \n is trimmed
      // parsed: "foo\nbar"
      const src = 'foo\n\n  bar';
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_DOUBLE);
      expect(map[0]).toBe(0); // f
      expect(map[1]).toBe(1); // o
      expect(map[2]).toBe(2); // o
      expect(map[3]).toBe(3); // \n (from blank line, mapped to content \n pos)
      expect(map[4]).toBe(7); // b (pos 7, after blank line and stripped '  ')
      expect(map[5]).toBe(8); // a
      expect(map[6]).toBe(9); // r
      expect(map[7]).toBe(src.length); // sentinel
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

    test('literal newline folds to space, stripping continuation-line indentation', () => {
      // same folding rules apply to single-quoted multiline
      const src = 'foo\n  bar';
      const map = buildParsedToSourceValueMap(src, Scalar.QUOTE_SINGLE);
      expect(map[0]).toBe(0); // f
      expect(map[1]).toBe(1); // o
      expect(map[2]).toBe(2); // o
      expect(map[3]).toBe(3); // folded space
      expect(map[4]).toBe(6); // b
      expect(map[5]).toBe(7); // a
      expect(map[6]).toBe(8); // r
      expect(map[7]).toBe(src.length); // sentinel
    });
  });

  describe('BLOCK_LITERAL / BLOCK_FOLDED scalars', () => {
    test('strips common indentation', () => {
      // src passed here is the content after the header line (| or >)
      // two-space indent on each line
      const src = '  hello\n  world\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_LITERAL);
      // parsed "hello\nworld\n" - positions after stripping 2-char indent
      expect(map[0]).toBe(2); // h
      expect(map[1]).toBe(3); // e
      expect(map[2]).toBe(4); // l
      expect(map[3]).toBe(5); // l
      expect(map[4]).toBe(6); // o
      expect(map[5]).toBe(7); // \n
      expect(map[6]).toBe(10); // w (skips indentation)
    });

    test('BLOCK_LITERAL does not fold empty lines', () => {
      // An empty line (just \n) must not cause contentStart to jump into the next line
      const src = '  hello\n\n  world\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_LITERAL);
      expect(map[0]).toBe(2); // h
      expect(map[5]).toBe(7); // first \n
      expect(map[6]).toBe(8); // second \n
      expect(map[7]).toBe(11); // w
    });

    test('BLOCK_FOLDED folds empty lines', () => {
      const src = '  hello\n\n  world\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_FOLDED);
      expect(map[0]).toBe(2); // h
      expect(map[5]).toBe(7); // first \n
      expect(map[6]).toBe(11); // w
    });

    test('BLOCK_FOLDED folds single newline to space', () => {
      // "foo bar": the newline between two normal lines folds to a space
      const src = '  foo\n  bar\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_FOLDED);
      expect(map[0]).toBe(2); // f
      expect(map[1]).toBe(3); // o
      expect(map[2]).toBe(4); // o
      expect(map[3]).toBe(8); // folded space - maps to start of "bar" (makes the most sense for error reporting)
      expect(map[4]).toBe(8); // b
      expect(map[5]).toBe(9); // a
      expect(map[6]).toBe(10); // r
      expect(map[7]).toBe(11); // trailing \n (clip)
      expect(map[8]).toBe(src.length); // sentinel
    });

    test('BLOCK_FOLDED more-indented line keeps surrounding newlines literal', () => {
      // YAML spec s-nb-spaced-text: lines with extra leading whitespace beyond base indent
      // are NOT folded - the surrounding newlines are kept literal.
      // Parsed value: "foo\n  bar\nbaz\n"
      const src = '  foo\n    bar\n  baz\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_FOLDED);
      expect(map[0]).toBe(2); // f
      expect(map[1]).toBe(3); // o
      expect(map[2]).toBe(4); // o
      // \n after "foo" - next line is more-indented, so keep literal
      expect(map[3]).toBe(5); // \n after foo
      expect(map[4]).toBe(8); // literal space
      expect(map[5]).toBe(9); // literal space
      expect(map[6]).toBe(10); // b
      expect(map[7]).toBe(11); // a
      expect(map[8]).toBe(12); // r
      // \n after "bar" - current line is "more-indented", so keep literal
      expect(map[9]).toBe(13); // \n after bar
      expect(map[10]).toBe(16); // b
      expect(map[11]).toBe(17); // a
      expect(map[12]).toBe(18); // z
      // trailing \n (clip)
      expect(map[13]).toBe(19); // \n at pos 19
      expect(map[14]).toBe(src.length); // sentinel
    });

    test('BLOCK_FOLDED mixed: blank lines then more-indented line', () => {
      // blank lines produce literal \n; more-indented line after them keeps its surrounding \n literal
      // parsed: "foo\n  bar\nbaz\n"
      const src = '  foo\n\n    bar\n  baz\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_FOLDED);
      expect(map[0]).toBe(2); // f
      expect(map[1]).toBe(3); // o
      expect(map[2]).toBe(4); // o
      expect(map[3]).toBe(5); // \n
      expect(map[4]).toBe(9); // space
      expect(map[5]).toBe(10); // space
      expect(map[6]).toBe(11); // b
      expect(map[7]).toBe(12); // a
      expect(map[8]).toBe(13); // r
      expect(map[9]).toBe(14); // \n at pos 14
      expect(map[10]).toBe(17); // b
      expect(map[11]).toBe(18); // a
      expect(map[12]).toBe(19); // z
      expect(map[13]).toBe(20); // trailing \n
      expect(map[14]).toBe(src.length); // sentinel
    });

    test('BLOCK_LITERAL with leading empty line preserves newlines', () => {
      // When the block starts with an empty line (just \n before content), it must produce a \n
      const src = '\n  hello\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_LITERAL);
      // parsed: "\nhello\n"
      expect(map[0]).toBe(0); // \n (empty first line)
      expect(map[1]).toBe(3); // h
      expect(map[2]).toBe(4); // e
      expect(map[3]).toBe(5); // l
      expect(map[4]).toBe(6); // l
      expect(map[5]).toBe(7); // o
      expect(map[6]).toBe(8); // \n
      expect(map[7]).toBe(src.length); // sentinel
    });

    test('BLOCK_FOLDED single line (no fold needed)', () => {
      const src = '  hello\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_FOLDED);
      // parsed: "hello\n"
      expect(map[0]).toBe(2);
      expect(map[5]).toBe(7); // trailing \n (clip)
      expect(map[6]).toBe(src.length); // sentinel
    });

    test('single-space indentation', () => {
      const src = ' foo\n bar\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_LITERAL);
      // parsed: "foo\nbar\n"
      expect(map[0]).toBe(1); // f (strips 1 space)
      expect(map[3]).toBe(4); // \n
      expect(map[4]).toBe(6); // b (strips 1 space from " bar")
      expect(map[7]).toBe(9); // \n
      expect(map[8]).toBe(src.length); // sentinel
    });

    test('zero indentation (trimIndent=0)', () => {
      // No leading whitespace: content starts at column 0
      const src = 'foo\nbar\n';
      const map = buildParsedToSourceValueMap(src, Scalar.BLOCK_LITERAL);
      // parsed: "foo\nbar\n"
      expect(map[0]).toBe(0); // f
      expect(map[3]).toBe(3); // \n
      expect(map[4]).toBe(4); // b
      expect(map[7]).toBe(7); // \n
      expect(map[8]).toBe(src.length); // sentinel
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
