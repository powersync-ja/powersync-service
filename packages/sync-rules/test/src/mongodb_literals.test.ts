import { assert, describe, expect, test } from 'vitest';
import { ZodError } from 'zod';
import type { JsonValue } from '../../src/index.js';
import { MONGO_FILTER_VALIDATOR } from '../../src/mongo/MongoFilterExpression.js';
function parseExpression({ config }: { config: unknown }) {
  return MONGO_FILTER_VALIDATOR.parse(config);
}
function parseMongoLiteral({ config }: { config: unknown }) {
  const expression = parseExpression({ config: { $eq: ['$$doc.value', config] } });
  return '$eq' in expression ? expression.$eq[1] : undefined;
}

describe('MongoDB expression literals', () => {
  test.each([
    undefined,
    null,
    NaN,
    Infinity,
    2147483648,
    -2147483649,
    [1],
    { region: 'eu' },
    {},
    '$internal',
    '$$doc.other',
    { $literal: 5 },
    { $oid: 'private-invalid-object-id' },
    { $numberInt: '2147483648' },
    { $numberLong: '9223372036854775808' },
    { $numberDouble: 'Infinity' },
    { $numberDouble: '0xFF' },
    { $numberDecimal: 'NaN' },
    { $date: '2026-01-01' },
    { $date: '2026-01-01T00:00:00' },
    { $date: { $numberLong: '8640000000000001' } },
    { $timestamp: { t: -1, i: 0 } },
    { $timestamp: { t: 0, i: 4294967296 } },
    { $binary: { base64: '/x==', subType: '00' } },
    { $binary: { base64: 'AQI', subType: '00' } },
    { $binary: { base64: 'AQI=', subType: '0' } },
    { $binary: { base64: 'AQI=', subType: '04' } },
    { $uuid: 'private-invalid-uuid' },
    { $regex: 'private-pattern' }
  ])('rejects invalid or unsupported constants: %j', (config) => {
    expect(() => parseMongoLiteral({ config })).toThrow(ZodError);
    expect(() => parseExpression({ config: { $eq: ['$$doc.value', config] } })).toThrow(ZodError);
    expect(() => parseExpression({ config: { $in: ['$$doc.value', [config]] } })).toThrow(ZodError);
  });

  test.each<JsonValue>([
    'text',
    '',
    'a$b',
    0,
    -1.5,
    true,
    false,
    { $literal: '$internal' },
    { $literal: '$$doc.not.a.reference' }
  ])('accepts scalar constants: %j', (config) => {
    expect(parseMongoLiteral({ config })).toEqual(config);
  });

  test.each(['', 'AQI=', '/w==', 'AAAA'])('accepts canonical base64 %j', (base64) => {
    expect(parseMongoLiteral({ config: { $binary: { base64, subType: '00' } } })).toEqual({
      $binary: { base64, subType: '00' }
    });
  });

  test('retains nested error locations without printing configured literal values', () => {
    const secret = 'private-invalid-id';
    try {
      parseExpression({ config: { $in: ['$$doc.owner', ['a', { $oid: secret }]] } });
      expect.fail('Expected validation to fail');
    } catch (error) {
      assert(error instanceof ZodError, 'Expected a literal validation error');
      expect(error.issues[0].path).toEqual(['$in', 1, 1, '$oid']);
      expect(error.message).not.toContain(secret);
    }
  });

  test('accepts deeply nested filters', () => {
    let expression: JsonValue = { $eq: ['$$doc.active', true] };
    for (let i = 0; i < 110; i++) {
      expression = i % 2 ? { $and: [expression] } : { $or: [expression] };
    }
    expect(parseExpression({ config: expression })).toEqual(expression);
  });
});
