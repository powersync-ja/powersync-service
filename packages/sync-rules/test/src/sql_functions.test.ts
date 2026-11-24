import { JSONBig } from '@powersync/service-jsonbig';
import { describe, expect, test } from 'vitest';
import {
  CompatibilityContext,
  CompatibilityEdition,
  generateSqlFunctions,
  cast as javaScriptCast,
  SqliteRow,
  SqliteValue
} from '../../src/index.js';
import { createRustEvaluator, syncTest, TEST_TABLE } from './sync_plan/evaluator/utils.js';

const compatibilityFunctions = generateSqlFunctions(CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
const fn = compatibilityFunctions.callable;

// Utility to evaluate a scalar expression on JavaScript inputs with the Rust addon.
const scalarRustEvaluationTest = syncTest.extend('evaluateWithRust', ({ rust, sync }) => {
  return function (expression: string) {
    const yaml = `
config:
  edition: 3
streams:
  a:
    query: SELECT ${expression} AS output FROM tbl
`;
    const config = sync.prepareSyncStreams(yaml);
    const evaluator = createRustEvaluator(rust, config)!;

    return {
      config,
      evaluateWithRust(record: SqliteRow) {
        const { rows: rustRows, errors: rustErrors } = evaluator.evaluateSerialized(TEST_TABLE, record);

        expect(rustErrors).toStrictEqual([]);
        return (JSONBig.parse(rustRows[0].data) as SqliteRow).output;
      }
    };
  };
});

// Evaluates a function with both JavaScript and Rust, asserting that outputs are equal.
const functionTest = scalarRustEvaluationTest.extend('compileFunction', ({ evaluateWithRust }) => {
  return function (name: keyof typeof fn, argumentCount: number) {
    const columns = Array.from({ length: argumentCount }, (_, i) => `a${i}`);
    const preparedRust = evaluateWithRust(`${name}(${columns.join(', ')})`);
    const jsFunction = generateSqlFunctions(preparedRust.config.compatibility).callable[name];

    return function (...args: SqliteValue[]) {
      expect(args).toHaveLength(argumentCount);

      const rust = preparedRust.evaluateWithRust(Object.fromEntries(columns.map((column, i) => [column, args[i]])));
      const js = jsFunction(...args);

      expect(rust).toStrictEqual(js);
      return js;
    };
  };
});

describe('SQL functions', () => {
  test('json extract', () => {
    expect(fn.json_extract(JSON.stringify({ foo: 'bar' }), '$.foo')).toEqual('bar');
    expect(fn.json_extract(JSON.stringify({ foo: 42 }), '$.foo')).toEqual(42n);
    expect(fn.json_extract(JSON.stringify({ foo: true }), '$.foo')).toEqual(1n);
    expect(fn.json_extract(`{"foo": 42.0}`, '$.foo')).toEqual(42.0);
    expect(fn.json_extract(JSON.stringify({ foo: 42 }), '$.bar.baz')).toEqual(null);
    expect(fn.json_extract(JSON.stringify({ foo: 42 }), '$')).toEqual('{"foo":42}');
    expect(fn.json_extract(`{"foo": 42.0}`, '$')).toEqual('{"foo":42.0}');
    expect(fn.json_extract(`{"foo": true}`, '$')).toEqual('{"foo":true}');
    // SQLite gives null instead of 'null'. We should match that, but it's a breaking change.
    expect(fn.json_extract(`{"foo": null}`, '$.foo')).toEqual('null');
    // Matches SQLite
    expect(fn.json_extract(`{}`, '$.foo')).toBeNull();
  });

  test('->>', () => {
    const jsonExtract = compatibilityFunctions.jsonExtract;

    expect(jsonExtract(JSON.stringify({ foo: 'bar' }), '$.foo', '->>')).toEqual('bar');
    expect(jsonExtract(JSON.stringify({ foo: 42 }), '$.foo', '->>')).toEqual(42n);
    expect(jsonExtract(`{"foo": 42.0}`, '$.foo', '->>')).toEqual(42.0);
    expect(jsonExtract(JSON.stringify({ foo: 'bar' }), 'foo', '->>')).toEqual('bar');
    expect(jsonExtract(JSON.stringify({ foo: 42 }), 'foo', '->>')).toEqual(42n);
    expect(jsonExtract(`{"foo": 42.0}`, 'foo', '->>')).toEqual(42.0);
    expect(jsonExtract(`{"foo": 42.0}`, '$', '->>')).toEqual(`{"foo":42.0}`);
    expect(jsonExtract(`{"foo": true}`, '$.foo', '->>')).toEqual(1n);
    // SQLite gives null instead of 'null'. We should match that, but it's a breaking change.
    expect(jsonExtract(`{"foo": null}`, '$.foo', '->>')).toEqual('null');
    // Matches SQLite
    expect(jsonExtract(`{}`, '$.foo', '->>')).toBeNull();
  });

  test('->', () => {
    const jsonExtract = compatibilityFunctions.jsonExtract;

    expect(jsonExtract(JSON.stringify({ foo: 'bar' }), '$.foo', '->')).toEqual('"bar"');
    expect(jsonExtract(JSON.stringify({ foo: 42 }), '$.foo', '->')).toEqual('42');
    expect(jsonExtract(`{"foo": 42.0}`, '$.foo', '->')).toEqual('42.0');
    expect(jsonExtract(JSON.stringify({ foo: 'bar' }), 'foo', '->')).toEqual('"bar"');
    expect(jsonExtract(JSON.stringify({ foo: 42 }), 'foo', '->')).toEqual('42');
    expect(jsonExtract(JSON.stringify({ foo: 42 }), 'bar', '->')).toBeNull();
    expect(jsonExtract(`{"foo": 42.0}`, 'foo', '->')).toEqual('42.0');
    expect(jsonExtract(`{"foo": 42.0}`, '$', '->')).toEqual(`{"foo":42.0}`);
    expect(jsonExtract(`{"foo": true}`, '$.foo', '->')).toEqual('true');
    // SQLite gives 'null' instead of null. We should match that, but it's a breaking change.
    expect(jsonExtract(`{"foo": null}`, '$.foo', '->')).toBeNull();
    // Matches SQLite
    expect(jsonExtract(`{}`, '$.foo', '->')).toBeNull();
  });

  test('fixed json extract', () => {
    const { jsonExtract, callable } = generateSqlFunctions(
      new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS })
    );

    expect(callable.json_extract(`{"foo": null}`, '$.foo')).toBeNull();
    expect(jsonExtract(`{"foo": null}`, '$.foo', '->>')).toBeNull();

    expect(jsonExtract(`{"foo": null}`, '$.foo', '->')).toStrictEqual('null');
    expect(jsonExtract(`{"foo": null}`, '$.bar', '->')).toBeNull();
  });

  test('json_array_length', () => {
    expect(fn.json_array_length(`[1,2,3,4]`)).toEqual(4n);
    expect(fn.json_array_length(`[1,2,3,4]`, '$')).toEqual(4n);
    expect(fn.json_array_length(`[1,2,3,4]`, '$.a')).toEqual(null);
    expect(fn.json_array_length(`{"a":[1,2,3,4]}`, '$.a')).toEqual(4n);
    expect(fn.json_array_length(`{"a":[1,2,3,4]}`)).toEqual(0n);
  });

  test('json_keys', () => {
    expect(fn.json_keys(`{"a": 1, "b": "2", "0": "test", "c": {"d": "e"}}`)).toEqual(`["0","a","b","c"]`);
    expect(fn.json_keys(`{}`)).toEqual(`[]`);
    expect(fn.json_keys(null)).toEqual(null);
    expect(fn.json_keys()).toEqual(null);
    expect(() => fn.json_keys(`{"a": 1, "a": 2}`)).toThrow();
    expect(() => fn.json_keys(`[1,2,3]`)).toThrow();
    expect(() => fn.json_keys(3)).toThrow();
  });

  test('json_valid', () => {
    expect(fn.json_valid(`{"a": 1, "b": "2", "0": "test", "c": {"d": "e"}}`)).toEqual(1n);
    expect(fn.json_valid(`{}`)).toEqual(1n);
    expect(fn.json_valid(null)).toEqual(0n);
    expect(fn.json_valid()).toEqual(0n);
    expect(fn.json_valid(`{"a": 1, "a": 2}`)).toEqual(0n);
    expect(fn.json_valid(`[1,2,3]`)).toEqual(1n);
    expect(fn.json_valid(3)).toEqual(1n);
    expect(fn.json_valid('test')).toEqual(0n);
    expect(fn.json_valid('"test"')).toEqual(1n);
    expect(fn.json_valid('true')).toEqual(1n);
    expect(fn.json_valid('TRUE')).toEqual(0n);
  });

  functionTest('typeof', ({ compileFunction }) => {
    const fnTypeof = compileFunction('typeof', 1);

    expect(fnTypeof(null)).toEqual('null');
    expect(fnTypeof('test')).toEqual('text');
    expect(fnTypeof('')).toEqual('text');
    expect(fnTypeof(1n)).toEqual('integer');
    expect(fnTypeof(1)).toEqual('real');
    expect(fnTypeof(1.5)).toEqual('real');
    expect(fnTypeof(Uint8Array.of(1, 2, 3))).toEqual('blob');
  });

  functionTest('length', ({ compileFunction }) => {
    const length = compileFunction('length', 1);

    expect(length(null)).toEqual(null);
    expect(length('abc')).toEqual(3n);
    expect(length(123n)).toEqual(3n);
    expect(length(123.4)).toEqual(5n);
    expect(length(Uint8Array.of(1, 2, 3))).toEqual(3n);
  });

  test('hex', () => {
    expect(fn.hex(null)).toEqual('');
    expect(fn.hex('abC')).toEqual('616243');
    expect(fn.hex(Uint8Array.of(0x61, 0x62, 0x43))).toEqual('616243');
    expect(fn.hex(123n)).toEqual('313233');
    expect(fn.hex(123.4)).toEqual('3132332E34');
  });

  test('base64', () => {
    expect(fn.base64(null)).toEqual('');
    expect(fn.base64('abc')).toEqual('YWJj');
    expect(fn.base64(Uint8Array.of(0x61, 0x62, 0x63))).toEqual('YWJj');
    expect(fn.base64(123n)).toEqual('MTIz');
    expect(fn.base64(123.4)).toEqual('MTIzLjQ=');
  });

  test('uuid_blob', () => {
    expect(fn.uuid_blob(null)).toEqual(null);
    expect(fn.uuid_blob('550e8400-e29b-41d4-a716-446655440000')).toEqual(
      new Uint8Array([85, 14, 132, 0, 226, 155, 65, 212, 167, 22, 68, 102, 85, 68, 0, 0])
    );
    expect(fn.uuid_blob('877b8be2-5a63-48e9-8ece-5e45b1d4f4ae')).toEqual(
      new Uint8Array([135, 123, 139, 226, 90, 99, 72, 233, 142, 206, 94, 69, 177, 212, 244, 174])
    );
    expect(() => fn.uuid_blob('non-uuid')).toThrowError();

    // Combine with base64
    const blob = fn.uuid_blob('550e8400-e29b-41d4-a716-446655440000');
    expect(fn.base64(blob)).toEqual('VQ6EAOKbQdSnFkRmVUQAAA==');
  });

  test('ifnull', () => {
    expect(fn.ifnull(null, null)).toEqual(null);
    expect(fn.ifnull('test', null)).toEqual('test');
    expect(fn.ifnull(null, 'test')).toEqual('test');
    expect(fn.ifnull('test1', 'test2')).toEqual('test1');
  });

  test('iif', () => {
    expect(fn.iif(null, 1, 2)).toEqual(2);
    expect(fn.iif(0, 1, 2)).toEqual(2);
    expect(fn.iif(1, 'first', 'second')).toEqual('first');
    expect(fn.iif(0.1, 'is_true', 'is_false')).toEqual('is_true');
    expect(fn.iif('a', 'is_true', 'is_false')).toEqual('is_false');
    expect(fn.iif(0n, 'is_true', 'is_false')).toEqual('is_false');
    expect(fn.iif(2n, 'is_true', 'is_false')).toEqual('is_true');
    expect(fn.iif(new Uint8Array([]), 'is_true', 'is_false')).toEqual('is_false');
    expect(fn.iif(Uint8Array.of(0x61, 0x62, 0x43), 'is_true', 'is_false')).toEqual('is_false');
  });

  functionTest('upper', ({ compileFunction }) => {
    const upper = compileFunction('upper', 1);

    expect(upper(null)).toEqual(null);
    expect(upper('abc')).toEqual('ABC');
    expect(upper(123n)).toEqual('123');
    expect(upper(3e60)).toEqual('3E+60');
    expect(upper(Uint8Array.of(0x61, 0x62, 0x43))).toEqual('ABC');

    expect(upper('straße')).toEqual('STRASSE');
  });

  functionTest('lower', ({ compileFunction }) => {
    const lower = compileFunction('lower', 1);

    expect(lower(null)).toEqual(null);
    expect(lower('ABC')).toEqual('abc');
    expect(lower(123n)).toEqual('123');
    expect(lower(3e60)).toEqual('3e+60');
    expect(lower(Uint8Array.of(0x61, 0x62, 0x43))).toEqual('abc');
  });

  functionTest('substring', ({ compileFunction }) => {
    const substring2 = compileFunction('substring', 2);
    const substring3 = compileFunction('substring', 3);

    // The compiler rejects calls with a single argument, so these can only be tested in JavaScript.
    expect(fn.substring(null)).toEqual(null);
    expect(fn.substring('abc')).toEqual(null);

    expect(substring3('abcde', 2, 3)).toEqual('bcd');
    expect(substring2('abcde', 2)).toEqual('bcde');
    expect(substring3('abcde', 2, null)).toEqual(null);
    expect(substring3('abcde', 0, 1)).toEqual('');
    expect(substring3('abcde', 0, 2)).toEqual('a');
    expect(substring3('abcde', 1, 2)).toEqual('ab');
    expect(substring2('abcde', -2)).toEqual('de');
    expect(substring3('abcde', -2, 1)).toEqual('d');
    expect(substring3('abcde', 6, -5)).toEqual('abcde');
    expect(substring3('abcde', 5, -2)).toEqual('cd');
    expect(substring3('2023-06-28 14:12:00.999Z', 1, 10)).toEqual('2023-06-28');
  });

  scalarRustEvaluationTest('cast', ({ evaluateWithRust }) => {
    // The target type of a CAST must be known statically, so we select the cast to apply based on a column.
    const targets = ['text', 'integer', 'numeric', 'real', 'blob'] as const;
    const whens = targets.map((target) => {
      const cast = `CAST(input AS ${target})`;
      // Blobs aren't included in output columns of processed rows, so the evaluator trick doesn't work. We cast those
      // to text afterwards.
      return `WHEN '${target}' THEN ${target == 'blob' ? `CAST(${cast} AS text)` : cast}`;
    });
    const rustEvaluator = evaluateWithRust(`CASE target ${whens.join(' ')} END`);

    function cast(input: SqliteValue, target: (typeof targets)[number]) {
      const rust = rustEvaluator.evaluateWithRust({ input, target });
      const js = javaScriptCast(input, target);

      expect(rust).toStrictEqual(target == 'blob' ? javaScriptCast(js, 'text') : js);
      return js;
    }

    expect(cast(null, 'text')).toEqual(null);
    expect(cast(null, 'integer')).toEqual(null);
    expect(cast(null, 'numeric')).toEqual(null);
    expect(cast(null, 'real')).toEqual(null);
    expect(cast(null, 'blob')).toEqual(null);

    expect(cast(1n, 'text')).toEqual('1');
    expect(cast(1n, 'integer')).toEqual(1n);
    expect(cast(1n, 'numeric')).toEqual(1n);
    expect(cast(1n, 'real')).toEqual(1.0);
    // We differ from SQLite here
    expect(cast(1n, 'blob')).toEqual(Uint8Array.of(49));

    expect(cast(1.2, 'text')).toEqual('1.2');
    expect(cast(1.2, 'integer')).toEqual(1n);
    expect(cast(1.2, 'numeric')).toEqual(1.2);
    expect(cast(1.2, 'real')).toEqual(1.2);
    // We differ from SQLite here
    expect(cast(1.2, 'blob')).toEqual(Uint8Array.of(49, 46, 50));

    expect(cast('abc', 'text')).toEqual('abc');
    expect(cast('abc', 'integer')).toEqual(0n);
    expect(cast('abc', 'numeric')).toEqual(0n);
    expect(cast('abc', 'real')).toEqual(0.0);

    expect(cast('1.2abc', 'integer')).toEqual(1n);
    expect(cast('1.2abc', 'numeric')).toEqual(1.2);
    expect(cast('1.2abc', 'real')).toEqual(1.2);
    expect(cast('1.2e60abc', 'numeric')).toEqual(1.2e60);
    expect(cast(' 1e60abc', 'numeric')).toEqual(1e60);
    expect(cast('-12abc', 'integer')).toEqual(-12n);
    expect(cast('+12abc', 'integer')).toEqual(12n);
    expect(cast('-12.9abc', 'integer')).toEqual(-12n);
    expect(cast('-12.5abc', 'numeric')).toEqual(-12.5);
    expect(cast('-.5abc', 'numeric')).toEqual(-0.5);
    expect(cast('+1.2e2abc', 'numeric')).toEqual(120);
    expect(cast('-1.2e2abc', 'numeric')).toEqual(-120);

    // We differ from SQLite here
    expect(cast('abc', 'blob')).toEqual(Uint8Array.of(0x61, 0x62, 0x63));

    // The JavaScript implementation throws an error here.
    expect(rustEvaluator.evaluateWithRust({ input: -Infinity, target: 'integer' })).toStrictEqual(
      -9223372036854775808n
    );
    expect(rustEvaluator.evaluateWithRust({ input: Infinity, target: 'integer' })).toStrictEqual(9223372036854775807n);
    expect(rustEvaluator.evaluateWithRust({ input: NaN, target: 'integer' })).toStrictEqual(null);

    // JavaScript incorrectly uses arbitrary precision integers instead of clamping like SQLite.
    expect(rustEvaluator.evaluateWithRust({ input: 1e300, target: 'integer' })).toStrictEqual(9223372036854775807n);

    // Edge cases, make sure JavaScript and Rust agree on all target types.
    const bytes = (...values: number[]) => Uint8Array.of(...values);
    const edgeCases: SqliteValue[] = [
      // Integers
      -42n,
      9223372036854775807n,
      -9223372036854775808n,
      // Reals
      1.0,
      -0.0,
      1.5,
      1.9,
      -1.5,
      1e-7,
      // Text
      '42',
      '  -7abc',
      '+5',
      '12.9',
      '1.0',
      '5.',
      '.5',
      '+.5',
      '1e',
      '1e+',
      '1e3',
      '1E3',
      '1e5',
      '\u00a0\u3000\t3',
      '',
      '-',
      '.',
      '- 1',
      'e5',
      '  -2.5e2xyz',
      'infinity',
      '9223372036854775807',
      '-9223372036854775808',
      // Blobs
      bytes(0x69, 0x64),
      bytes(0x61, 0xff, 0x62),
      bytes(0x20, 0x31, 0x32),
      bytes(0xff, 0x31),
      bytes(0x32, 0x2e, 0x35)
    ];
    for (const input of edgeCases) {
      for (const target of targets) {
        cast(input, target);
      }
    }
  });

  test('unixepoch', () => {
    // Differences from SQLite
    // These versions are not supported
    expect(fn.unixepoch()).toEqual(null);
    expect(fn.unixepoch('now')).toEqual(null);
    expect(fn.unixepoch('2023-06-28 14:12', 'auto')).toEqual(null);

    // Same as SQLite
    expect(fn.unixepoch('2023-06-28 14:12')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28 14:12:00')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28 14:12:00+00:00')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28 14:12:00Z')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28 14:12:00.999')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28 14:12:00.999Z')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28T14:12:00.999Z')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28T14:12:00+00:00')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28T14:12+00:00')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28T16:12+02:00')).toEqual(1687961520n);
    expect(fn.unixepoch('2023-06-28T16:12+02')).toEqual(null);
    expect(fn.unixepoch('2023-06-28')).toEqual(1687910400n);
    expect(fn.unixepoch('0000-01-01T00:00')).toEqual(-62167219200n);
    expect(fn.unixepoch('3000-01-01T00:00')).toEqual(32503680000n);
    expect(fn.unixepoch('2023')).toEqual(-210691972800n);
    expect(fn.unixepoch('2023.')).toEqual(-210691972800n);
    expect(fn.unixepoch('2023.123')).toEqual(-210691962173n);
    expect(fn.unixepoch(2023)).toEqual(-210691972800n);
    expect(fn.unixepoch(2023n)).toEqual(-210691972800n);
    expect(fn.unixepoch(2023.123)).toEqual(-210691962173n);
    expect(fn.unixepoch(1687961520n, 'unixepoch')).toEqual(1687961520n);
    expect(fn.unixepoch(1687961520, 'unixepoch')).toEqual(1687961520n);
    expect(fn.unixepoch(1687961520.567, 'unixepoch')).toEqual(1687961520n);
    expect(fn.unixepoch(1687961520.567, 'unixepoch', 'subsec')).toEqual(1687961520.567);
    expect(fn.unixepoch(1687961520.567, 'unixepoch', 'subsecond')).toEqual(1687961520.567);

    expect(fn.unixepoch('2023-06-28 14:12:00.999', 'subsec')).toEqual(1687961520.999);
    expect(fn.unixepoch('2023-06-28 14:12:00.999', 'subsecond')).toEqual(1687961520.999);
    expect(fn.unixepoch('2023-06-28 14:12:00.0', 'subsecond')).toEqual(1687961520.0);
  });

  test('datetime', () => {
    // Differences from SQLite
    // These versions are not supported
    expect(fn.datetime()).toEqual(null);
    expect(fn.datetime('now')).toEqual(null);
    expect(fn.datetime('2023-06-28 14:12', 'auto')).toEqual(null);

    // Same as SQLite
    expect(fn.datetime('2023-06-28 14:12')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28 14:12:00')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28 14:12:00+00:00')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28 14:12:00Z')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28 14:12:00.999')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28 14:12:00.999Z')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28T14:12:00.999Z')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28T14:12:00+00:00')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28T14:12+00:00')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28T16:12+02:00')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime('2023-06-28T16:12+02')).toEqual(null);
    expect(fn.datetime('2023-06-28')).toEqual('2023-06-28 00:00:00');
    expect(fn.datetime('0000-01-01T00:00')).toEqual('0000-01-01 00:00:00');
    expect(fn.datetime('3000-01-01T00:00')).toEqual('3000-01-01 00:00:00');
    expect(fn.datetime('2023')).toEqual('-4707-06-08 12:00:00');
    expect(fn.datetime('2023.')).toEqual('-4707-06-08 12:00:00');
    expect(fn.datetime('2023.123')).toEqual('-4707-06-08 14:57:07');
    expect(fn.datetime(2023)).toEqual('-4707-06-08 12:00:00');
    expect(fn.datetime(2023n)).toEqual('-4707-06-08 12:00:00');
    expect(fn.datetime(2023.123)).toEqual('-4707-06-08 14:57:07');
    expect(fn.datetime(1687961520n, 'unixepoch')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime(1687961520, 'unixepoch')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime(1687961520.567, 'unixepoch')).toEqual('2023-06-28 14:12:00');
    expect(fn.datetime(1687961520.567, 'unixepoch', 'subsec')).toEqual('2023-06-28 14:12:00.567');
    expect(fn.datetime(1687961520.567, 'unixepoch', 'subsecond')).toEqual('2023-06-28 14:12:00.567');

    expect(fn.datetime('2023-06-28 14:12:00.999', 'subsec')).toEqual('2023-06-28 14:12:00.999');
    expect(fn.datetime('2023-06-28 14:12:00.999', 'subsecond')).toEqual('2023-06-28 14:12:00.999');
    expect(fn.datetime('2023-06-28 14:12:00.0', 'subsecond')).toEqual('2023-06-28 14:12:00.000');
  });

  test('instr', () => {
    // Null handling - SQLite returns NULL when either argument is NULL
    expect(fn.instr(null, null)).toEqual(null);
    expect(fn.instr(null, 'a')).toEqual(null);
    expect(fn.instr('abc', null)).toEqual(null);

    // Basic match - 1-indexed positions
    expect(fn.instr('hello world', 'world')).toEqual(7n);
    expect(fn.instr('hello world', 'hello')).toEqual(1n);
    expect(fn.instr('hello world', 'o')).toEqual(5n);

    // No match returns 0
    expect(fn.instr('hello world', 'xyz')).toEqual(0n);
    expect(fn.instr('hello', 'HELLO')).toEqual(0n);

    // Match at very start (verifies 1-indexed, not 0)
    expect(fn.instr('abc', 'a')).toEqual(1n);
    expect(fn.instr('abc', 'ab')).toEqual(1n);
    expect(fn.instr('abc', 'abc')).toEqual(1n);

    // Match at end
    expect(fn.instr('abc', 'c')).toEqual(3n);
    expect(fn.instr('abc', 'bc')).toEqual(2n);

    // Single character haystack and needle
    expect(fn.instr('a', 'a')).toEqual(1n);
    expect(fn.instr('a', 'b')).toEqual(0n);

    // Empty string behavior (matches SQLite: empty needle always found at position 1)
    expect(fn.instr('abc', '')).toEqual(1n);
    expect(fn.instr('', '')).toEqual(1n);
    expect(fn.instr('', 'a')).toEqual(0n);

    // First occurrence only (returns position of first match)
    expect(fn.instr('abcabc', 'b')).toEqual(2n);
    expect(fn.instr('aaa', 'a')).toEqual(1n);

    // Case sensitivity
    expect(fn.instr('Hello World', 'hello')).toEqual(0n);
    expect(fn.instr('Hello World', 'Hello')).toEqual(1n);

    // Multi-character needle
    expect(fn.instr('abcdef', 'cde')).toEqual(3n);
    expect(fn.instr('abcdef', 'ef')).toEqual(5n);

    // Special characters and emojis
    expect(fn.instr('foo|bar|baz', '|')).toEqual(4n);
    expect(fn.instr('foo.bar', '.')).toEqual(4n);
    expect(fn.instr('hello\nworld', '\n')).toEqual(6n);
    expect(fn.instr('path/to/file', '/')).toEqual(5n);
    expect(fn.instr('rofl 😂', '😂')).toEqual(6n);
    expect(fn.instr('😂😂', '😂')).toEqual(1n);

    // Numeric inputs (converted to string via toString)
    expect(fn.instr(12345, '3')).toEqual(3n);
    expect(fn.instr('12345', 3)).toEqual(3n);
    expect(fn.instr(123, 123)).toEqual(1n);

    // Both BLOBs: byte-level search (1-indexed byte position)
    const enc = new TextEncoder();
    expect(fn.instr(enc.encode('hello world'), enc.encode('world'))).toEqual(7n);
    expect(fn.instr(enc.encode('hello world'), enc.encode('hello'))).toEqual(1n);
    expect(fn.instr(enc.encode('hello world'), enc.encode('xyz'))).toEqual(0n);
    expect(fn.instr(enc.encode('abc'), enc.encode('bc'))).toEqual(2n);
    expect(fn.instr(new Uint8Array([0x00, 0xff, 0xab, 0xcd]), new Uint8Array([0xab, 0xcd]))).toEqual(3n);
    expect(fn.instr(new Uint8Array([0x00, 0xff, 0xab]), new Uint8Array([0xcd]))).toEqual(0n);

    // Both BLOBs: empty needle found at position 1
    expect(fn.instr(enc.encode('abc'), new Uint8Array([]))).toEqual(1n);
    expect(fn.instr(new Uint8Array([]), new Uint8Array([]))).toEqual(1n);
    expect(fn.instr(new Uint8Array([]), enc.encode('a'))).toEqual(0n);

    // Mixed BLOB/text: both coerced to text (matches SQLite behavior)
    expect(fn.instr(enc.encode('hello world'), 'world')).toEqual(7n);
    expect(fn.instr('hello world', enc.encode('world'))).toEqual(7n);
    expect(fn.instr(enc.encode('abc'), 'xyz')).toEqual(0n);

    // NULL handling
    expect(fn.instr(null, enc.encode('a'))).toEqual(null);
    expect(fn.instr(enc.encode('a'), null)).toEqual(null);
  });

  test('ST_AsGeoJSON', () => {
    expect(fn.st_asgeojson(null)).toEqual(null);
    expect(fn.st_asgeojson('0101000000029a081b9ede4340d7a3703d0a3f5ac0')).toEqual(
      '{"type":"Point","coordinates":[39.7392,-104.985]}'
    );
    expect(
      fn.st_asgeojson(
        '01030000000100000005000000000000000000000000000000000000000000000000000000000000000000f03f000000000000f03f000000000000f03f000000000000f03f000000000000000000000000000000000000000000000000'
      )
    ).toEqual('{"type":"Polygon","coordinates":[[[0.0,0.0],[0.0,1.0],[1.0,1.0],[1.0,0.0],[0.0,0.0]]]}');
  });

  test('ST_X', () => {
    expect(fn.st_x(null)).toEqual(null);
    expect(fn.st_x('0101000000029a081b9ede4340d7a3703d0a3f5ac0')).toEqual(39.7392);
    expect(
      fn.st_x(
        '01030000000100000005000000000000000000000000000000000000000000000000000000000000000000f03f000000000000f03f000000000000f03f000000000000f03f000000000000000000000000000000000000000000000000'
      )
    ).toEqual(null);
  });

  test('ST_Y', () => {
    expect(fn.st_y(null)).toEqual(null);
    expect(fn.st_y('0101000000029a081b9ede4340d7a3703d0a3f5ac0')).toEqual(-104.985);
  });

  test('ST_AsText', () => {
    expect(fn.st_astext(null)).toEqual(null);
    expect(fn.st_astext('0101000000029a081b9ede4340d7a3703d0a3f5ac0')).toEqual('POINT(39.7392 -104.985)');
    expect(
      fn.st_astext(
        `01030000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000`
      )
    ).toEqual('POLYGON((0 0,0 1,1 1,1 0,0 0))');
  });
});
