import { describe, expect, test } from 'vitest';
import { normalizeConnectionConfig } from '../../src/types/types.js';

describe('config', () => {
  test('Should resolve database', () => {
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test'
    });
    expect(normalized.database).equals('powersync_test');
  });
});
