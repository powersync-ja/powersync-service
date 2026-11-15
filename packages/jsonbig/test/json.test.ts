import { describe, expect, test } from 'vitest';
import { JSONBig } from '../src/index.js';

describe('JSONBig', () => {
  describe('stringify', () => {
    test('serializes NaN, Infinity, and -Infinity as null', () => {
      expect(JSONBig.stringify(NaN)).toBe('null');
      expect(JSONBig.stringify(Infinity)).toBe('null');
      expect(JSONBig.stringify(-Infinity)).toBe('null');
    });

    test('serializes non-finite numbers in arrays as null', () => {
      expect(JSONBig.stringify([NaN, Infinity, -Infinity])).toBe('[null,null,null]');
    });

    test('serializes non-finite numbers in objects as null', () => {
      expect(JSONBig.stringify({ a: NaN, b: Infinity, c: -Infinity })).toBe('{"a":null,"b":null,"c":null}');
    });

    test('serializes parsed out-of-range floats (1e400) as null', () => {
      expect(JSONBig.stringify(JSONBig.parse('{"x": 1e400}'))).toBe('{"x":null}');
    });

    test('preserves valid integers and floats', () => {
      expect(JSONBig.stringify(0)).toBe('0.0');
      expect(JSONBig.stringify(42)).toBe('42.0');
      expect(JSONBig.stringify(-10)).toBe('-10.0');
      expect(JSONBig.stringify(1.5)).toBe('1.5');
      expect(JSONBig.stringify(-3.14)).toBe('-3.14');
    });

    test('round-trips non-finite numbers safely', () => {
      const parsedArray = JSONBig.parse(JSONBig.stringify([NaN, Infinity, -Infinity]));
      expect(parsedArray).toStrictEqual([null, null, null]);

      const parsedObj = JSONBig.parse(JSONBig.stringify({ a: NaN, b: Infinity, c: -Infinity }));
      expect(parsedObj).toStrictEqual({ a: null, b: null, c: null });
    });
  });
});
