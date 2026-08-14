import { normalizeConnectionConfig } from '@module/types/types.js';
import { describe, expect, test } from 'vitest';

const BASE_CONFIG = {
  type: 'mongodb' as const,
  uri: 'mongodb://localhost:27017/powersync_test'
};

describe('MongoDB connection config', () => {
  test('defaults heartbeat_interval_seconds to 60 seconds', () => {
    expect(normalizeConnectionConfig(BASE_CONFIG).heartbeat_interval_seconds).toBe(60);
  });

  test('defaults a null heartbeat_interval_seconds to 60 seconds', () => {
    expect(
      normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds: null }).heartbeat_interval_seconds
    ).toBe(60);
  });

  test('allows the MongoDB maximum heartbeat interval', () => {
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
