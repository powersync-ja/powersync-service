import { normalizeConnectionConfig } from '@module/types/types.js';
import { describe, expect, test } from 'vitest';

const BASE_CONFIG = {
  type: 'mongodb' as const,
  uri: 'mongodb://localhost:27017/powersync_test'
};

describe('MongoDB connection config', () => {
  test('defaults heartbeat_interval_seconds to 0 seconds', () => {
    expect(normalizeConnectionConfig(BASE_CONFIG).heartbeat_interval_seconds).toBe(0);
  });

  test('defaults a null heartbeat_interval_seconds to 0 seconds', () => {
    expect(
      normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds: null }).heartbeat_interval_seconds
    ).toBe(0);
  });

  test('disables heartbeat_interval_seconds with 0', () => {
    expect(
      normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds: 0 }).heartbeat_interval_seconds
    ).toBe(0);
  });

  test('allows the MongoDB maximum heartbeat interval', () => {
    expect(
      normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds: 300 }).heartbeat_interval_seconds
    ).toBe(300);
  });

  test.each([4, 301, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects invalid heartbeat_interval_seconds: %s',
    (heartbeat_interval_seconds) => {
      expect(() => normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds })).toThrow(
        'heartbeat_interval_seconds'
      );
    }
  );
});
