import { limitParamsForLogging } from '@/util/param-logging.js';
import { StreamingSyncRequest } from '@/util/protocol-types.js';
import { schema } from '@powersync/lib-services-framework';
import { deserialize } from 'bson';
import { describe, expect, test } from 'vitest';

describe('parameter logging', () => {
  test('handles BSON Undefined accepted by the sync request validator', () => {
    // Raw BSON is required: the serializer normally omits undefined values.
    // This encodes { parameters: { x: undefined } } using BSON type 0x06.
    const request = deserialize(Buffer.from('1900000003706172616d657465727300080000000678000000', 'hex'));
    const validator = schema.createTsCodecValidator(StreamingSyncRequest, { allowAdditional: true });

    expect(validator.validate(request).valid).toBe(true);
    expect(Object.hasOwn(request.parameters, 'x')).toBe(true);
    expect(request.parameters.x).toBeUndefined();
    expect(limitParamsForLogging(request.parameters)).toEqual({ x: '[undefined]' });
  });

  test('handles values without a JSON representation and serialization errors', () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;

    expect(limitParamsForLogging({ missing: undefined, fn: () => {}, symbol: Symbol(), bigint: 1n, circular })).toEqual(
      {
        missing: '[undefined]',
        fn: '[undefined]',
        symbol: '[undefined]',
        bigint: '[unserializable]',
        circular: '[unserializable]'
      }
    );
  });

  test('bounds keys and values, including JSON-encoded values', () => {
    const params = { ['k'.repeat(100_000)]: 'v'.repeat(1000), nested: { text: 'v'.repeat(1000) }, null: null };
    const result = limitParamsForLogging(params);

    expect(result['k'.repeat(97) + '...']).toBe('v'.repeat(97) + '...');
    expect(result.nested).toHaveLength(100);
    expect(result.null).toBe('null');
    expect(Object.keys(params)[0]).toHaveLength(100_000);
  });

  test('omits excess entries without serializing their values', () => {
    const params = Object.fromEntries(Array.from({ length: 20 }, (_, i) => ['key' + i, 'value']));
    const result = limitParamsForLogging({
      ...params,
      omitted: {
        toJSON: () => {
          throw new Error('must not serialize');
        }
      },
      alsoOmitted: 'value'
    });

    expect(result).toEqual({ ...params, '⚠️': 'Additional parameters omitted' });
  });
});
