import { normalizeConnectionConfig } from '@module/types/types.js';
import { describe, expect, test } from 'vitest';

const BASE_CONFIG = {
  type: 'postgresql' as const,
  uri: 'postgresql://postgres:postgres@localhost:5432/powersync_test',
  sslmode: 'disable' as const
};

describe('Postgres connection config', () => {
  test('defaults heartbeat_interval_seconds to 60 seconds', () => {
    expect(normalizeConnectionConfig(BASE_CONFIG).heartbeat_interval_seconds).toBe(60);
  });

  test('uses the default for a null heartbeat_interval_seconds', () => {
    expect(
      normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds: null }).heartbeat_interval_seconds
    ).toBe(60);
  });

  test('allows the Postgres maximum heartbeat interval', () => {
    expect(
      normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds: 60 }).heartbeat_interval_seconds
    ).toBe(60);
  });

  test.each([0, 4, 61, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects invalid heartbeat_interval_seconds: %s',
    (heartbeat_interval_seconds) => {
      expect(() => normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds })).toThrow(
        'heartbeat_interval_seconds'
      );
    }
  );
});
