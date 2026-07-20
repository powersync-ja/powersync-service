import { describe, expect, test } from 'vitest';
import { parseDocument, YAMLMap, YAMLSeq } from 'yaml';
import { YamlError } from '../../src/errors.js';
import { YamlValidator } from '../../src/yaml_validation.js';

function createValidator() {
  const errors: YamlError[] = [];
  return { validator: new YamlValidator((e) => errors.push(e)), errors };
}

/** Fetches `key` from a top-level mapping document, keeping the scalar wrapper so it has a source position. */
function get(yaml: string, key: string): unknown {
  return parseDocument(yaml).get(key, true);
}

describe('YamlValidator', () => {
  describe('expectMap', () => {
    test('returns the map for a real mapping', () => {
      const { validator, errors } = createValidator();
      const result = validator.expectMap(get('foo:\n  bar: 1', 'foo'), "'foo'");
      expect(result).toBeInstanceOf(YAMLMap);
      expect(errors).toEqual([]);
    });

    test('treats an absent key as absent, without reporting', () => {
      const { validator, errors } = createValidator();
      const result = validator.expectMap(get('other: 1', 'foo'), "'foo'");
      expect(result).toBeUndefined();
      expect(errors).toEqual([]);
    });

    test('treats a bare null value as absent, without reporting', () => {
      const { validator, errors } = createValidator();
      const result = validator.expectMap(get('foo:', 'foo'), "'foo'");
      expect(result).toBeUndefined();
      expect(errors).toEqual([]);
    });

    test('reports a wrongly-typed value with a real location', () => {
      const { validator, errors } = createValidator();
      const yaml = 'foo: oops';
      const result = validator.expectMap(get(yaml, 'foo'), "'foo'");
      expect(result).toBeUndefined();
      expect(errors).toMatchObject([{ message: "'foo' must be a mapping.", type: 'fatal' }]);
      expect(yaml.slice(errors[0].location.start, errors[0].location.end)).toEqual('oops');
    });
  });

  describe('expectSeq', () => {
    test('reports a mapping where an array was expected', () => {
      const { validator, errors } = createValidator();
      const result = validator.expectSeq(get('foo:\n  bar: 1', 'foo'), "'foo'");
      expect(result).toBeUndefined();
      expect(errors).toMatchObject([{ message: "'foo' must be an array.", type: 'fatal' }]);
    });

    test('returns the seq for a real array', () => {
      const { validator, errors } = createValidator();
      const result = validator.expectSeq(get('foo: [1, 2]', 'foo'), "'foo'");
      expect(result).toBeInstanceOf(YAMLSeq);
      expect(errors).toEqual([]);
    });
  });

  describe('expectString / expectBoolean / expectInteger', () => {
    test('expectString rejects a number', () => {
      const { validator, errors } = createValidator();
      expect(validator.expectString(get('foo: 1', 'foo'), "'foo'")).toBeUndefined();
      expect(errors).toMatchObject([{ message: "'foo' must be a string." }]);
    });

    test('expectBoolean rejects a string', () => {
      const { validator, errors } = createValidator();
      expect(validator.expectBoolean(get('foo: "yes"', 'foo'), "'foo'")).toBeUndefined();
      expect(errors).toMatchObject([{ message: "'foo' must be a boolean." }]);
    });

    test('expectBoolean accepts a real boolean', () => {
      const { validator, errors } = createValidator();
      expect(validator.expectBoolean(get('foo: true', 'foo'), "'foo'")).toBe(true);
      expect(errors).toEqual([]);
    });

    test('expectInteger rejects a non-integer number', () => {
      const { validator, errors } = createValidator();
      expect(validator.expectInteger(get('foo: 1.5', 'foo'), "'foo'")).toBeUndefined();
      expect(errors).toMatchObject([{ message: "'foo' must be an integer." }]);
    });

    test('expectInteger enforces the given range', () => {
      const { validator, errors } = createValidator();
      expect(validator.expectInteger(get('foo: 5', 'foo'), "'foo'", { min: 0, max: 3 })).toBeUndefined();
      expect(errors).toMatchObject([{ message: "'foo' must be an integer between 0 and 3 (inclusive)." }]);
    });

    test('an absent value reports nothing and returns undefined', () => {
      const { validator, errors } = createValidator();
      expect(validator.expectInteger(get('other: 5', 'foo'), "'foo'")).toBeUndefined();
      expect(errors).toEqual([]);
    });
  });

  describe('checkAdditionalKeys', () => {
    test('reports every key not in the allow-list, at that key location', () => {
      const { validator, errors } = createValidator();
      const yaml = 'a: 1\nb: 2\nc: 3';
      const map = parseDocument(yaml).contents as YAMLMap;
      validator.checkAdditionalKeys(map, ['a', 'c'], 'here');
      expect(errors).toMatchObject([{ message: "Unknown key 'b' here.", type: 'fatal' }]);
      expect(yaml.slice(errors[0].location.start, errors[0].location.end)).toEqual('b');
    });

    test('reports nothing when every key is allowed', () => {
      const { validator, errors } = createValidator();
      const map = parseDocument('a: 1\nb: 2').contents as YAMLMap;
      validator.checkAdditionalKeys(map, ['a', 'b'], 'here');
      expect(errors).toEqual([]);
    });
  });

  describe('isPresent', () => {
    test('is false for an absent key and a bare null value', () => {
      const { validator } = createValidator();
      expect(validator.isPresent(get('other: 1', 'foo'))).toBe(false);
      expect(validator.isPresent(get('foo:', 'foo'))).toBe(false);
    });

    test('is true for any real value, regardless of type', () => {
      const { validator } = createValidator();
      expect(validator.isPresent(get('foo: 0', 'foo'))).toBe(true);
      expect(validator.isPresent(get('foo: false', 'foo'))).toBe(true);
      expect(validator.isPresent(get('foo:\n  bar: 1', 'foo'))).toBe(true);
    });
  });
});
