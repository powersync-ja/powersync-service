import {
  cast,
  CompatibilityContext,
  generateSqlFunctions,
  CompatibilityOption,
  CompatibilityEdition
} from '../../src/index.js';
import { describe, expect, test } from 'vitest';

const compatibilityFunctions = generateSqlFunctions(CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
const fn = compatibilityFunctions.callable;

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

  test('typeof', () => {
    expect(fn.typeof(null)).toEqual('null');
    expect(fn.typeof('test')).toEqual('text');
    expect(fn.typeof('')).toEqual('text');
    expect(fn.typeof(1n)).toEqual('integer');
    expect(fn.typeof(1)).toEqual('real');
    expect(fn.typeof(1.5)).toEqual('real');
    expect(fn.typeof(Uint8Array.of(1, 2, 3))).toEqual('blob');
  });

  test('length', () => {
    expect(fn.length(null)).toEqual(null);
    expect(fn.length('abc')).toEqual(3n);
    expect(fn.length(123n)).toEqual(3n);
    expect(fn.length(123.4)).toEqual(5n);
    expect(fn.length(Uint8Array.of(1, 2, 3))).toEqual(3n);
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

  test('upper', () => {
    expect(fn.upper(null)).toEqual(null);
    expect(fn.upper('abc')).toEqual('ABC');
    expect(fn.upper(123n)).toEqual('123');
    expect(fn.upper(3e60)).toEqual('3E+60');
    expect(fn.upper(Uint8Array.of(0x61, 0x62, 0x43))).toEqual('ABC');
  });

  test('lower', () => {
    expect(fn.lower(null)).toEqual(null);
    expect(fn.lower('ABC')).toEqual('abc');
    expect(fn.lower(123n)).toEqual('123');
    expect(fn.lower(3e60)).toEqual('3e+60');
    expect(fn.lower(Uint8Array.of(0x61, 0x62, 0x43))).toEqual('abc');
  });

  test('substring', () => {
    expect(fn.substring(null)).toEqual(null);
    expect(fn.substring('abc')).toEqual(null);
    expect(fn.substring('abcde', 2, 3)).toEqual('bcd');
    expect(fn.substring('abcde', 2)).toEqual('bcde');
    expect(fn.substring('abcde', 2, null)).toEqual(null);
    expect(fn.substring('abcde', 0, 1)).toEqual('');
    expect(fn.substring('abcde', 0, 2)).toEqual('a');
    expect(fn.substring('abcde', 1, 2)).toEqual('ab');
    expect(fn.substring('abcde', -2)).toEqual('de');
    expect(fn.substring('abcde', -2, 1)).toEqual('d');
    expect(fn.substring('abcde', 6, -5)).toEqual('abcde');
    expect(fn.substring('abcde', 5, -2)).toEqual('cd');
    expect(fn.substring('2023-06-28 14:12:00.999Z', 1, 10)).toEqual('2023-06-28');
  });

  test('cast', () => {
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

    // We differ from SQLite here
    expect(cast('abc', 'blob')).toEqual(Uint8Array.of(0x61, 0x62, 0x63));
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
