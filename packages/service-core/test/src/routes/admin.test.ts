import { BasicRouterRequest, Context, JwtPayload } from '@/index.js';
import { logger } from '@powersync/lib-services-framework';
import { describe, expect, it } from 'vitest';
import { validate } from '../../../src/routes/endpoints/admin.js';
import { mockServiceContext } from './mocks.js';

describe('admin routes', () => {
  describe('validate', () => {
    it('reports errors with source location', async () => {
      const context: Context = {
        logger: logger,
        service_context: mockServiceContext(null),
        token_payload: new JwtPayload({
          sub: '',
          exp: 0,
          iat: 0
        })
      };

      const request: BasicRouterRequest = {
        headers: {},
        hostname: '',
        protocol: 'http'
      };

      const response = await validate.handler({
        context,
        params: {
          sync_rules: `
bucket_definitions:
  missing_table:
    data:
      - SELECT * FROM missing_table
`
        },
        request
      });

      expect(response.errors).toEqual([
        expect.objectContaining({
          level: 'warning',
          location: { start_offset: 70, end_offset: 83 },
          message: 'Table public.missing_table not found'
        })
      ]);
    });
  });
});
