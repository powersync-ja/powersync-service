import { ConvexModule } from '@module/module/ConvexModule.js';
import { describe, expect, test } from 'vitest';
import { TEST_CONNECTION_OPTIONS } from '../test-utils/util.js';

describe('ConvexModule', () => {
  test('Testing Connections should succeed for valid connections', async () => {
    // It's not easy to test for in invalid Convex backend, since we only configure a valid backend.
    const result = await ConvexModule.testConnection(TEST_CONNECTION_OPTIONS);
    expect(result.connectionDescription).eq(TEST_CONNECTION_OPTIONS.deployment_url);
  });
});
