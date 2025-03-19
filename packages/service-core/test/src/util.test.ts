import { getIntersection, hasIntersection } from '@/index.js';
import { describe, expect, test } from 'vitest';

describe('utils', () => {
  function testInstersection(a: Set<any>, b: Set<any>, expected: boolean) {
    expect(hasIntersection(a, b)).toBe(expected);
    expect(hasIntersection(b, a)).toBe(expected);
    const mapA = new Map([...a].map((v) => [v, 1]));
    const mapB = new Map([...b].map((v) => [v, 1]));
    expect(hasIntersection(mapA, b)).toBe(expected);
    expect(hasIntersection(mapB, a)).toBe(expected);
    expect(hasIntersection(mapA, mapB)).toBe(expected);
  }

  test('hasIntersection', async () => {
    testInstersection(new Set(['a']), new Set(['a']), true);
    testInstersection(new Set(['a', 'b', 'c']), new Set(['a', 'b', 'c']), true);
    testInstersection(new Set(['a', 'b', 'c']), new Set(['d', 'e']), false);
    testInstersection(new Set(['a', 'b', 'c']), new Set(['d', 'c', 'e']), true);
    testInstersection(new Set(['a', 'b', 'c']), new Set(['c', 'e']), true);
    testInstersection(new Set(['a', 'b', 'c', 2]), new Set([1, 2, 3]), true);
    testInstersection(new Set(['a', 'b', 'c', 4]), new Set([1, 2, 3]), false);
    testInstersection(new Set([]), new Set([1, 2, 3]), false);
    testInstersection(new Set([]), new Set([]), false);
  });

  function testGetInstersection(a: Set<any>, b: Set<any>, expected: any[]) {
    expect([...getIntersection(a, b)]).toEqual(expected);
    expect([...getIntersection(b, a)]).toEqual(expected);
    const mapA = new Map([...a].map((v) => [v, 1]));
    const mapB = new Map([...b].map((v) => [v, 1]));
    expect([...getIntersection(mapA, b)]).toEqual(expected);
    expect([...getIntersection(mapB, a)]).toEqual(expected);
    expect([...getIntersection(mapA, mapB)]).toEqual(expected);
  }

  test('getIntersection', async () => {
    testGetInstersection(new Set(['a']), new Set(['a']), ['a']);
    testGetInstersection(new Set(['a', 'b', 'c']), new Set(['a', 'b', 'c']), ['a', 'b', 'c']);
    testGetInstersection(new Set(['a', 'b', 'c']), new Set(['d', 'e']), []);
    testGetInstersection(new Set(['a', 'b', 'c']), new Set(['d', 'c', 'e']), ['c']);
    testGetInstersection(new Set(['a', 'b', 'c']), new Set(['c', 'e']), ['c']);
    testGetInstersection(new Set(['a', 'b', 'c', 2]), new Set([1, 2, 3]), [2]);
    testGetInstersection(new Set(['a', 'b', 'c', 4]), new Set([1, 2, 3]), []);
    testGetInstersection(new Set([]), new Set([1, 2, 3]), []);
    testGetInstersection(new Set([]), new Set([]), []);
  });
});
