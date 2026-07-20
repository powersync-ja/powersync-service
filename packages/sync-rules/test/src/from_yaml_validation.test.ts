import { describe, expect, test } from 'vitest';
import { CompatibilityEdition, SqlSyncRules } from '../../src/index.js';
import { PARSE_OPTIONS } from './util.js';

/**
 * Regression tests for the manual YAML shape validation in `from_yaml.ts`/`yaml_validation.ts`, which replaced a
 * final ajv `validateSyncRulesSchema` pass. These specifically target inputs that either crashed (blind
 * `as YAMLMap` casts on wrongly-typed top-level values) or were silently accepted (no manual check existed, only
 * the removed ajv step) before that change.
 */
describe('yaml shape validation', () => {
  test('non-mapping document does not crash', () => {
    expect(() => SqlSyncRules.fromYaml('just a string', PARSE_OPTIONS)).toThrow(/Sync rules must be a mapping/);
  });

  describe('config', () => {
    test('scalar value does not crash', () => {
      const { errors } = SqlSyncRules.fromYaml('config: oops\nbucket_definitions: {}', {
        ...PARSE_OPTIONS,
        throwOnError: false
      });
      expect(errors).toMatchObject([{ message: "'config' must be a mapping.", type: 'fatal' }]);
    });

    test('sequence value does not crash', () => {
      const { errors } = SqlSyncRules.fromYaml('config:\n  - a\n  - b\nbucket_definitions: {}', {
        ...PARSE_OPTIONS,
        throwOnError: false
      });
      expect(errors).toMatchObject([{ message: "'config' must be a mapping.", type: 'fatal' }]);
    });

    test('bare key (null) is treated as absent, not an error', () => {
      const { errors } = SqlSyncRules.fromYaml('config:\nbucket_definitions: {}', {
        ...PARSE_OPTIONS,
        throwOnError: false
      });
      expect(errors).toEqual([]);
    });

    test('edition out of range falls back to legacy', () => {
      const { config: rules, errors } = SqlSyncRules.fromYaml(
        'config:\n  edition: 99\nbucket_definitions: {}',
        { ...PARSE_OPTIONS, throwOnError: false }
      );
      expect(errors).toMatchObject([
        { message: "'edition' must be an integer between 1 and 3 (inclusive).", type: 'fatal' }
      ]);
      expect((rules as any).compatibility.edition).toEqual(CompatibilityEdition.LEGACY);
    });

    test('invalid timestamp_max_precision value is reported', () => {
      const { errors } = SqlSyncRules.fromYaml(
        'config:\n  edition: 2\n  timestamps_iso8601: true\n  timestamp_max_precision: bogus\nbucket_definitions: {}',
        { ...PARSE_OPTIONS, throwOnError: false }
      );
      expect(errors).toMatchObject([
        { message: expect.stringContaining("Invalid value for 'timestamp_max_precision'") }
      ]);
    });

    test('wrong-typed compatibility option is reported instead of silently coerced', () => {
      const { errors } = SqlSyncRules.fromYaml('config:\n  timestamps_iso8601: "yes"\nbucket_definitions: {}', {
        ...PARSE_OPTIONS,
        throwOnError: false
      });
      expect(errors).toMatchObject([{ message: "'timestamps_iso8601' must be a boolean.", type: 'fatal' }]);
    });
  });

  describe('bucket_definitions / streams / with', () => {
    test('bucket_definitions as a string does not crash', () => {
      const { errors } = SqlSyncRules.fromYaml('bucket_definitions: oops', { ...PARSE_OPTIONS, throwOnError: false });
      expect(errors).toMatchObject([
        { message: "'bucket_definitions' must be a mapping.", type: 'fatal' },
        { message: "'bucket_definitions' or 'streams' is required", type: 'fatal' }
      ]);
    });

    test('streams as an array does not silently parse as empty', () => {
      const { errors } = SqlSyncRules.fromYaml('config:\n  edition: 3\nstreams: []', {
        ...PARSE_OPTIONS,
        throwOnError: false
      });
      expect(errors).toMatchObject([
        { message: "'streams' must be a mapping.", type: 'fatal' },
        { message: "'streams' are required.", type: 'fatal' }
      ]);
    });

    test('with as a number does not crash', () => {
      const { errors } = SqlSyncRules.fromYaml('bucket_definitions: {}\nwith: 1', {
        ...PARSE_OPTIONS,
        throwOnError: false
      });
      expect(errors).toMatchObject([{ message: "'with' must be a mapping.", type: 'fatal' }]);
    });
  });

  describe('unknown keys', () => {
    test('typo in top-level key is flagged', () => {
      const { errors } = SqlSyncRules.fromYaml('bucket_definitions: {}\nbucket_definitons: {}', {
        ...PARSE_OPTIONS,
        throwOnError: false
      });
      expect(errors).toMatchObject([
        { message: "Unknown key 'bucket_definitons' at the top level.", type: 'fatal' }
      ]);
    });

    test('unknown key in a bucket definition', () => {
      const { errors } = SqlSyncRules.fromYaml('bucket_definitions:\n  mybucket:\n    data: []\n    bogus: true', {
        ...PARSE_OPTIONS,
        throwOnError: false
      });
      expect(errors).toMatchObject([
        { message: "Unknown key 'bogus' in bucket definition 'mybucket'.", type: 'fatal' }
      ]);
    });

    test('unknown key in a compiled stream', () => {
      const { errors } = SqlSyncRules.fromYaml(
        'config:\n  edition: 3\nstreams:\n  foo:\n    query: SELECT * FROM users\n    bogus: true',
        { ...PARSE_OPTIONS, throwOnError: false }
      );
      expect(errors).toMatchObject([{ message: "Unknown key 'bogus' in stream 'foo'.", type: 'fatal' }]);
    });

    test('unknown key in an event definition', () => {
      const { errors } = SqlSyncRules.fromYaml(
        'bucket_definitions: {}\nevent_definitions:\n  foo:\n    payloads:\n      - SELECT user_id FROM checkpoints\n    extra: 1',
        { ...PARSE_OPTIONS, throwOnError: false }
      );
      expect(errors).toMatchObject([{ message: "Unknown key 'extra' in event definition 'foo'.", type: 'fatal' }]);
    });
  });

  describe('stream entries', () => {
    test('non-mapping stream value in the compiled compiler does not crash', () => {
      const { errors } = SqlSyncRules.fromYaml('config:\n  edition: 3\nstreams:\n  foo: oops', {
        ...PARSE_OPTIONS,
        throwOnError: false
      });
      expect(errors).toMatchObject([{ message: "Stream 'foo' must be a mapping.", type: 'fatal' }]);
    });

    test('non-mapping stream value in the legacy compiler does not crash', () => {
      const { errors } = SqlSyncRules.fromYaml('streams:\n  foo: oops', { ...PARSE_OPTIONS, throwOnError: false });
      expect(errors).toMatchObject([
        { message: expect.stringContaining('alpha version of Sync Streams'), type: 'warning' },
        { message: "Stream 'foo' must be a mapping.", type: 'fatal' }
      ]);
    });
  });
});
