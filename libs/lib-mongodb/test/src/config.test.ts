import { describe, expect, test } from 'vitest';
import { normalizeMongoConfig } from '../../src/types/types.js';

describe('config', () => {
  test('Should resolve database', () => {
    const normalized = normalizeMongoConfig({
      type: 'mongodb',
      uri: 'mongodb://localhost:27017/powersync_test'
    });
    expect(normalized.database).equals('powersync_test');
  });
});
