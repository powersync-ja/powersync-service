import { JSONBig } from '@powersync/service-jsonbig';
import { describe, expect, test } from 'vitest';
import { prepare } from './helpers.js';

describe('native custom functions', () => {
  test('extended ISO years and minute precision', () => {
    const { rust, expected } = prepare(["SELECT id, datetime(a, 'subsec') AS d FROM docs"]);
    const rows = ['9999-12-31', '-000001-01-01T00:00:00Z', '+010000-01-01T00:00:00Z', '2026-09-14T12:34'].map((a) => ({
      id: 'x',
      a
    }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });
  test('records the known V8 date-only year-zero compatibility difference', () => {
    const { rust, expected } = prepare(['SELECT id, datetime(a) AS d FROM docs']);
    const rows = [{ id: 'x', a: '0000-01-01' }];
    expect(JSONBig.parse(rust.evaluate(rows)[0].data.results[0].data)).toMatchObject({ d: '0000-01-01 00:00:00' });
    expect(JSONBig.parse(expected(rows)[0].data.results[0].data)).toMatchObject({ d: '2000-01-01 00:00:00' });
  });
  test.each(['upper', 'lower'])('%s Unicode and coercions', (fn) => {
    const { rust, expected } = prepare([`SELECT id, ${fn}(a) AS value FROM docs`]);
    const rows = ['Straße', 'ﬁle', 'İ', 'ΟΣ', 'Οσ', 'ß', 'a😀b', '', null, 42n, 1e21, new Uint8Array([0xff, 65])].map(
      (a) => ({ id: 'x', a })
    );
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });

  test.each([
    'unixepoch(a)',
    'datetime(a)',
    "unixepoch(a, 'subsec')",
    "datetime(a, 'subsec')",
    "unixepoch(a, 'unixepoch')",
    "datetime(a, 'unixepoch')",
    "unixepoch(a, 'unixepoch', 'subsecond')",
    "datetime(a, 'unixepoch', 'subsecond')",
    "datetime(a, 'unsupported')",
    "unixepoch(a, 'unixepoch', 'unsupported')"
  ])('%s', (expression) => {
    const { rust, expected } = prepare([`SELECT id, ${expression} AS value FROM docs`]);
    const inputs = [
      null,
      'now',
      'invalid',
      '1970-01-01',
      '2026-09-14T12:34:56.123Z',
      '2026-09-14 12:34:56',
      '2026-09-14T12:34:56+02:00',
      '1969-12-31T23:59:59.999Z',
      '2440587.5',
      2440587.5,
      0n,
      1n,
      -1n,
      -0.0019,
      1234.5678,
      new Uint8Array([1]),
      Infinity
    ];
    const rows = inputs.map((a) => ({ id: 'x', a }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });

  test('dates with variable modifiers and missing arguments', () => {
    const { rust, expected } = prepare([
      'SELECT id, datetime(a, modifier, last) AS d, unixepoch(null) AS empty FROM docs'
    ]);
    const rows = [null, 'unixepoch', 'subsec', 'subsecond', 'bad', 1n].flatMap((modifier) =>
      [null, 'subsec', 'subsecond', 'bad'].map((last) => ({ id: 'x', a: 0n, modifier, last }))
    );
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });

  test('JSON containment preserves types and large integers', () => {
    const { rust, expected } = prepare(['SELECT id, NOT (a NOT IN b) AS contained FROM docs']);
    const needles = [
      null,
      0n,
      1n,
      1,
      '1',
      'a',
      9007199254740993n,
      9007199254740992,
      '{"a":1}',
      '[1,2]',
      new Uint8Array([1])
    ];
    const haystacks = [null, '[]', '[null,true,false]', '[1,"1","a"]', '[9007199254740993]', '[{"a":1},[1,2]]'];
    const rows = needles.flatMap((a) => haystacks.map((b) => ({ id: 'x', a, b })));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });

  test('JSON containment invalid inputs are isolated to their query', () => {
    const { rust } = prepare(['SELECT id, NOT (a NOT IN b) AS contained FROM docs', 'SELECT id FROM docs']);
    const rows = ['{}', '{', '1', 1n, new Uint8Array([1])].map((b) => ({ id: 'x', a: 1n, b }));
    for (const row of rust.evaluate(rows)) {
      expect(row.data.errors).toHaveLength(1);
      expect(row.data.results).toHaveLength(1);
    }
  });

  function point(x: number, y: number, little = true, srid?: number) {
    const bytes = Buffer.alloc(srid === undefined ? 21 : 25);
    bytes[0] = little ? 1 : 0;
    const uint = little ? bytes.writeUInt32LE.bind(bytes) : bytes.writeUInt32BE.bind(bytes);
    const double = little ? bytes.writeDoubleLE.bind(bytes) : bytes.writeDoubleBE.bind(bytes);
    uint(srid === undefined ? 1 : 0x20000001, 1);
    if (srid !== undefined) uint(srid, 5);
    const offset = srid === undefined ? 5 : 9;
    double(x, offset);
    double(y, offset + 8);
    return bytes;
  }

  test.each(['st_astext', 'st_asgeojson', 'st_x', 'st_y'])('%s WKB, EWKB, endian and hex input', (fn) => {
    const { rust, expected } = prepare([`SELECT id, ${fn}(a) AS value FROM docs`]);
    const bytes = [point(1, 2), point(-12.5, 42.25, false), point(0, -0, true, 4326)];
    const rows = [...bytes, ...bytes.map((b) => b.toString('hex')), null, 1n].map((a) => ({ id: 'x', a }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });

  test('lines and collections, non-point coordinates return null', () => {
    const line = Buffer.alloc(41);
    line[0] = 1;
    line.writeUInt32LE(2, 1);
    line.writeUInt32LE(2, 5);
    [1, 2, 3, 4].forEach((v, i) => line.writeDoubleLE(v, 9 + i * 8));
    const collection = Buffer.concat([Buffer.from([1, 7, 0, 0, 0, 1, 0, 0, 0]), point(1, 2)]);
    const { rust, expected } = prepare([
      'SELECT id, st_astext(a) AS wkt, st_asgeojson(a) AS json, st_x(a) AS x, st_y(a) AS y FROM docs'
    ]);
    const rows = [line, collection].map((a) => ({ id: 'x', a }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });

  test('invalid geometry reports an error and subsequent rows still work', () => {
    const { rust } = prepare(['SELECT id, st_astext(a) AS value FROM docs']);
    const result = rust.evaluate(
      [new Uint8Array(), 'zz', new Uint8Array([1, 255]), point(1, 2)].map((a) => ({ id: 'x', a }))
    );
    expect(result.slice(0, 3).every((r) => r.data.errors.length === 1)).toBe(true);
    expect(JSONBig.parse(result[3].data.results[0].data)).toMatchObject({ value: 'POINT(1 2)' });
  });
});
