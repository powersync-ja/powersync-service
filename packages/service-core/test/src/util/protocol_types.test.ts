import { StreamingSyncRequest } from '@/index.js';
import { schema } from '@powersync/lib-services-framework';
import { describe, test, expect, it } from 'vitest';

describe('protocol types', () => {
  describe('StreamingSyncRequest', () => {
    const validator = schema.createTsCodecValidator(StreamingSyncRequest, { allowAdditional: true });

    test('with streams', () => {
      expect(
        validator.validate({
          buckets: [],
          include_checksum: true,
          raw_data: true,
          binary_data: true,
          client_id: '0da33a94-c140-4b42-b3b3-a1df3b1352a3',
          parameters: {},
          streams: {
            include_defaults: true,
            subscriptions: [{ stream: 'does_not_exist', parameters: null, override_priority: null }]
          }
        } as any)
      ).toMatchObject({ valid: true });
    });
  });
});
