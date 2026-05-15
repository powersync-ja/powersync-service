import { describe, expect, test } from 'vitest';
import { evaluateOperator } from '../../src/index.js';

describe('SQL operators', () => {
  test('=', () => {
    expect(evaluateOperator('=', 1, 1n)).toStrictEqual(1n);
    expect(evaluateOperator('=', 1.1, 1n)).toStrictEqual(0n);
    expect(evaluateOperator('=', '1', 1n)).toStrictEqual(0n);
    expect(evaluateOperator('=', '', 0n)).toStrictEqual(0n);
    expect(evaluateOperator('=', 1, null)).toStrictEqual(null);
    expect(evaluateOperator('=', 0, null)).toStrictEqual(null);
    expect(evaluateOperator('=', null, '')).toStrictEqual(null);
    expect(evaluateOperator('=', null, null)).toStrictEqual(null);
  });

  test('!=', () => {
    expect(evaluateOperator('!=', 1, 1n)).toStrictEqual(0n);
    expect(evaluateOperator('!=', 1.1, 1n)).toStrictEqual(1n);
    expect(evaluateOperator('!=', '1', 1n)).toStrictEqual(1n);
    expect(evaluateOperator('!=', '', 0n)).toStrictEqual(1n);
    expect(evaluateOperator('!=', 1, null)).toStrictEqual(null);
    expect(evaluateOperator('!=', 0, null)).toStrictEqual(null);
    expect(evaluateOperator('!=', null, '')).toStrictEqual(null);
    expect(evaluateOperator('!=', null, null)).toStrictEqual(null);
  });

  test('IS', () => {
    expect(evaluateOperator('IS', 1, 1n)).toStrictEqual(1n);
    expect(evaluateOperator('IS', 1.1, 1n)).toStrictEqual(0n);
    expect(evaluateOperator('IS', '1', 1n)).toStrictEqual(0n);
    expect(evaluateOperator('IS', '', 0n)).toStrictEqual(0n);
    expect(evaluateOperator('IS', 1, null)).toStrictEqual(0n);
    expect(evaluateOperator('IS', 0, null)).toStrictEqual(0n);
    expect(evaluateOperator('IS', null, '')).toStrictEqual(0n);
    expect(evaluateOperator('IS', null, null)).toStrictEqual(1n);
  });

  test('IS NOT', () => {
    expect(evaluateOperator('IS NOT', 1, 1n)).toStrictEqual(0n);
    expect(evaluateOperator('IS NOT', 1.1, 1n)).toStrictEqual(1n);
    expect(evaluateOperator('IS NOT', '1', 1n)).toStrictEqual(1n);
    expect(evaluateOperator('IS NOT', '', 0n)).toStrictEqual(1n);
    expect(evaluateOperator('IS NOT', 1, null)).toStrictEqual(1n);
    expect(evaluateOperator('IS NOT', 0, null)).toStrictEqual(1n);
    expect(evaluateOperator('IS NOT', null, '')).toStrictEqual(1n);
    expect(evaluateOperator('IS NOT', null, null)).toStrictEqual(0n);
  });

  test('>', () => {
    expect(evaluateOperator('>', 1, 1n)).toStrictEqual(0n);
    expect(evaluateOperator('>', 2, 1n)).toStrictEqual(1n);
    expect(evaluateOperator('>', 2n, 1)).toStrictEqual(1n);
    expect(evaluateOperator('>', 'text', 1)).toStrictEqual(1n);
    expect(evaluateOperator('>', 1, 'text')).toStrictEqual(0n);
    expect(evaluateOperator('>', 1, null)).toStrictEqual(null);
    expect(evaluateOperator('>', null, null)).toStrictEqual(null);
  });

  test('>=', () => {
    expect(evaluateOperator('>=', 1, 1n)).toStrictEqual(1n);
    expect(evaluateOperator('>=', 2, 1n)).toStrictEqual(1n);
    expect(evaluateOperator('>=', 2n, 1)).toStrictEqual(1n);
    expect(evaluateOperator('>=', 'text', 1)).toStrictEqual(1n);
    expect(evaluateOperator('>=', 1, 'text')).toStrictEqual(0n);
    expect(evaluateOperator('>=', 1, null)).toStrictEqual(null);
    expect(evaluateOperator('>=', null, null)).toStrictEqual(null);
  });

  test('<', () => {
    expect(evaluateOperator('<', 1, 1n)).toStrictEqual(0n);
    expect(evaluateOperator('<', 1n, 2)).toStrictEqual(1n);
    expect(evaluateOperator('<', 1, 2n)).toStrictEqual(1n);
    expect(evaluateOperator('<', 'text', 1)).toStrictEqual(0n);
    expect(evaluateOperator('<', 1, 'text')).toStrictEqual(1n);
    expect(evaluateOperator('<', 1, null)).toStrictEqual(null);
    expect(evaluateOperator('<', null, null)).toStrictEqual(null);
  });

  test('<=', () => {
    expect(evaluateOperator('<=', 1, 1n)).toStrictEqual(1n);
    expect(evaluateOperator('<=', 1n, 2)).toStrictEqual(1n);
    expect(evaluateOperator('<=', 1, 2n)).toStrictEqual(1n);
    expect(evaluateOperator('<=', 'text', 1)).toStrictEqual(0n);
    expect(evaluateOperator('<=', 1, 'text')).toStrictEqual(1n);
    expect(evaluateOperator('<=', 1, null)).toStrictEqual(null);
    expect(evaluateOperator('<=', null, null)).toStrictEqual(null);
  });

  test('||', () => {
    expect(evaluateOperator('||', 'test', null)).toStrictEqual(null);
    expect(evaluateOperator('||', 'a', 31n)).toStrictEqual('a31');
    expect(evaluateOperator('||', 3.1, 'a')).toStrictEqual('3.1a');
  });

  test('+', () => {
    expect(evaluateOperator('+', 1n, null)).toStrictEqual(null);
    expect(evaluateOperator('+', 1n, 2)).toStrictEqual(3);
    expect(evaluateOperator('+', 1n, 2n)).toStrictEqual(3n);
    expect(evaluateOperator('+', 1, 2)).toStrictEqual(3);
    expect(evaluateOperator('+', 1n, '2')).toStrictEqual(3n);
    expect(evaluateOperator('+', 1n, '2.0')).toStrictEqual(3.0);
  });

  test('/', () => {
    expect(evaluateOperator('/', 1n, null)).toStrictEqual(null);
    expect(evaluateOperator('/', 5n, 2)).toStrictEqual(2.5);
    expect(evaluateOperator('/', 5n, 2n)).toStrictEqual(2n);
    expect(evaluateOperator('/', 5, 2)).toStrictEqual(2.5);
    expect(evaluateOperator('/', 5n, '2')).toStrictEqual(2n);
    expect(evaluateOperator('/', 5n, '2.0')).toStrictEqual(2.5);
  });

  test('*', () => {
    expect(evaluateOperator('*', 3n, null)).toStrictEqual(null);
    expect(evaluateOperator('*', 3n, 2)).toStrictEqual(6);
    expect(evaluateOperator('*', 3n, 2n)).toStrictEqual(6n);
    expect(evaluateOperator('*', 3, 2)).toStrictEqual(6);
    expect(evaluateOperator('*', 3n, '2')).toStrictEqual(6n);
    expect(evaluateOperator('*', 3n, '2.0')).toStrictEqual(6.0);
    expect(evaluateOperator('*', '3', '2.0')).toStrictEqual(6.0);
  });

  test('-', () => {
    expect(evaluateOperator('-', 3n, null)).toStrictEqual(null);
    expect(evaluateOperator('-', 3n, 2)).toStrictEqual(1);
    expect(evaluateOperator('-', 3n, 2n)).toStrictEqual(1n);
    expect(evaluateOperator('-', 3, 2)).toStrictEqual(1);
    expect(evaluateOperator('-', 3n, '2')).toStrictEqual(1n);
    expect(evaluateOperator('-', 3n, '2.0')).toStrictEqual(1.0);
    expect(evaluateOperator('-', '3', '2.0')).toStrictEqual(1.0);
  });

  test('&&', () => {
    // null short-circuit
    expect(evaluateOperator('&&', null, '["foo"]')).toStrictEqual(null);
    expect(evaluateOperator('&&', '["foo"]', null)).toStrictEqual(null);

    // disjoint arrays
    expect(evaluateOperator('&&', '["bar"]', '["foo"]')).toStrictEqual(0n);
    expect(evaluateOperator('&&', '[]', '["foo"]')).toStrictEqual(0n);
    expect(evaluateOperator('&&', '["foo"]', '[]')).toStrictEqual(0n);

    // overlapping arrays
    expect(evaluateOperator('&&', '["foo"]', '["foo"]')).toStrictEqual(1n);
    expect(evaluateOperator('&&', '["bar","foo"]', '["foo"]')).toStrictEqual(1n);
    expect(evaluateOperator('&&', '["foo"]', '["bar","foo"]')).toStrictEqual(1n);

    // regression: index-like string on the left must not satisfy any right-hand array.
    // The previous implementation iterated indexes of the left array, so "0" would
    // overlap any non-empty right-hand array.
    expect(evaluateOperator('&&', '["0"]', '["foo"]')).toStrictEqual(0n);
    expect(evaluateOperator('&&', '["0","1"]', '["foo","bar"]')).toStrictEqual(0n);
    expect(evaluateOperator('&&', '["0"]', '["0"]')).toStrictEqual(1n);

    // numeric affinity: bigint on one side, number on the other should still match.
    expect(evaluateOperator('&&', '[1]', '[1]')).toStrictEqual(1n);
    expect(evaluateOperator('&&', '[1,2,3]', '[4,3]')).toStrictEqual(1n);
  });
});
