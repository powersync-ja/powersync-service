import { describe, expect, test } from 'vitest';
import { Equality, HashMap, listEquality, StableHasher, unorderedEquality } from '../../../src/compiler/equality.js';

test('hasher', () => {
  expect(StableHasher.hashWith(stringEquality, 'hello')).toEqual(333190410);
  expect(StableHasher.hashWith(stringEquality, 'world')).toEqual(242621632);
});

describe('equality', () => {
  test('list', () => {
    const a = ['a', 'b', 'c'];
    const equality = listEquality(stringEquality);

    expectEqual(equality, a, ['a', 'b', 'c']);
    expectNotEqual(equality, a, ['c', 'b', 'a']);
    expectNotEqual(equality, a, ['a', 'b']);
    expectNotEqual(equality, a, ['a', 'b', 'c', 'd']);
    expectNotEqual(equality, a, ['a', 'b', 'd']);
  });

  test('set', () => {
    const a = ['a', 'b', 'c'];
    const equality = unorderedEquality(stringEquality);

    expectEqual(equality, a, ['a', 'b', 'c']);
    expectEqual(equality, a, ['c', 'b', 'a']);
    expectNotEqual(equality, a, ['a', 'b']);
    expectNotEqual(equality, a, ['a', 'b', 'c', 'd']);
    expectNotEqual(equality, a, ['a', 'b', 'd']);
  });
});

test('map', () => {
  const map = new HashMap(listEquality(stringEquality));

  expect(map.get(['a'])).toBeUndefined();
  expect(map.set(['a'], 'foo')).toBeUndefined();
  expect(map.get(['a'])).toStrictEqual('foo');

  map.setOrUpdate(['a'], (old) => {
    expect(old).toStrictEqual('foo');
    return 'bar';
  });
  expect(map.get(['a'])).toStrictEqual('bar');

  map.setOrUpdate(['a', 'b', 'c'], (old) => {
    expect(old).toBeUndefined();
    return 'baz';
  });
  expect(map.get(['a', 'b', 'c'])).toStrictEqual('baz');
});

function expectEqual<T>(equality: Equality<T>, a: T, b: T) {
  expect(equality.equals(a, b)).toBeTruthy();
  expect(StableHasher.hashWith(equality, a)).toEqual(StableHasher.hashWith(equality, b));
}

function expectNotEqual<T>(equality: Equality<T>, a: T, b: T) {
  expect(equality.equals(a, b)).toBeFalsy();

  // This is technically incorrect, distinct objects are allowed to have the same hash code.
  // In practice, it's almost guaranteed to hold.
  expect(StableHasher.hashWith(equality, a)).not.toEqual(StableHasher.hashWith(equality, b));
}

const stringEquality: Equality<string> = {
  equals: (a, b) => a === b,
  hash: (hasher, e) => hasher.addString(e)
};
