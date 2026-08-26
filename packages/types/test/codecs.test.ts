import * as t from 'ts-codec';
import { describe, expect, test } from 'vitest';

import { anyPrimitive, enumLiteral, orNull } from '../src/codecs.js';

test('orNull', () => {
  const codec = orNull(t.literal('allowed'));

  expect(codec.encode('allowed')).toStrictEqual('allowed');
  expect(codec.decode('allowed')).toStrictEqual('allowed');
  expect(codec.encode(null)).toStrictEqual(null);
  expect(codec.decode(null)).toStrictEqual(null);

  expect(() => codec.encode('invalid' as any)).throws();
  expect(() => codec.decode('invalid' as any)).throws();
});

test('enumLiteral', () => {
  const codec = enumLiteral('foo', 'bar', 'baz');

  for (const value of ['foo', 'bar', 'baz'] as const) {
    expect(codec.encode(value)).toStrictEqual(value);
    expect(codec.decode(value)).toStrictEqual(value);
  }

  expect(() => codec.encode('invalid' as any)).throws();
  expect(() => codec.decode('invalid' as any)).throws();
});

describe('anyPrimitive', () => {
  test('allows only the configured types', () => {
    const codec = anyPrimitive({ string: true, number: true }) as t.AnyCodec;

    expect(codec.decode('foo')).toStrictEqual('foo');
    expect(codec.decode(42)).toStrictEqual(42);
    expect(codec.encode('foo')).toStrictEqual('foo');
    expect(codec.encode(42)).toStrictEqual(42);

    expect(() => codec.decode(true)).throws();
    expect(() => codec.decode(null)).throws();
    expect(() => codec.decode(undefined)).throws();
    expect(() => codec.decode({})).throws();
  });

  test('allows null', () => {
    const codec = anyPrimitive({ null: true }) as t.AnyCodec;

    expect(codec.decode(null)).toStrictEqual(null);
    expect(codec.encode(null)).toStrictEqual(null);

    expect(() => codec.decode('foo')).throws();
    expect(() => codec.decode(undefined)).throws();
    expect(() => codec.decode({})).throws();
  });

  test('allows undefined', () => {
    const codec = anyPrimitive({ undefined: true }) as t.AnyCodec;

    expect(codec.decode(undefined)).toStrictEqual(undefined);
    expect(codec.encode(undefined)).toStrictEqual(undefined);

    expect(() => codec.decode(null)).throws();
    expect(() => codec.decode('foo')).throws();
  });

  test('rejects types not in the configuration, even when other types are allowed', () => {
    const codec = anyPrimitive({ boolean: true }) as t.AnyCodec;

    expect(codec.decode(true)).toStrictEqual(true);
    expect(codec.decode(false)).toStrictEqual(false);

    expect(() => codec.decode('foo')).throws();
    expect(() => codec.decode(1)).throws();
    expect(() => codec.decode(null)).throws();
    expect(() => codec.decode(undefined)).throws();
  });

  test('with no allowed types rejects everything', () => {
    const codec = anyPrimitive({}) as any as t.AnyCodec;

    expect(() => codec.decode('foo')).throws();
    expect(() => codec.decode(1)).throws();
    expect(() => codec.decode(true)).throws();
    expect(() => codec.decode(null)).throws();
    expect(() => codec.decode(undefined)).throws();
  });
});
