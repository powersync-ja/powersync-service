import { evaluateOperator } from '../../src/index.js';
import { describe, expect, test } from 'vitest';

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
});
