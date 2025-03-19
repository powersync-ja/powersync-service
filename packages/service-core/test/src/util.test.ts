import { hasIntersection } from '@/index.js';
import { describe, expect, test } from 'vitest';

describe('utils', () => {
  test('hasIntersection', async () => {
    expect(hasIntersection(new Set(['a', 'b', 'c']), new Set(['d', 'e']))).toBe(false);
    expect(hasIntersection(new Set(['a', 'b', 'c']), new Set(['d', 'c', 'e']))).toBe(true);
    expect(hasIntersection(new Set(['d', 'e']), new Set(['a', 'b', 'c']))).toBe(false);
    expect(hasIntersection(new Set(['a', 'b', 'c']), new Set(['c', 'e']))).toBe(true);
    expect(hasIntersection(new Set(['c', 'e']), new Set(['a', 'b', 'c']))).toBe(true);
    expect(hasIntersection(new Set(['a', 'b', 'c', 2]), new Set([1, 2, 3]))).toBe(true);
    expect(hasIntersection(new Set(['a', 'b', 'c', 4]), new Set([1, 2, 3]))).toBe(false);
    expect(hasIntersection(new Set([]), new Set([1, 2, 3]))).toBe(false);
    expect(hasIntersection(new Set([]), new Set([]))).toBe(false);
  });
});
