import { describe, expect, test } from 'vitest';
import { normalizeConnectionConfig, parseConnectTimeout } from '../../src/types/types.js';

describe('config', () => {
  test('Should resolve database', () => {
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test'
    });
    expect(normalized.database).equals('powersync_test');
  });

  describe('connect_timeout', () => {
    test('parses connect_timeout from URL query string', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?connect_timeout=300'
      });
      // 300 seconds = 300_000 ms
      expect(normalized.connect_timeout_ms).equals(300_000);
    });

    test('URL without connect_timeout returns undefined', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test'
      });
      expect(normalized.connect_timeout_ms).toBeUndefined();
    });

    test('explicit config value takes precedence over URL query param', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?connect_timeout=300',
        connect_timeout: 60
      });
      // Explicit 60 seconds = 60_000 ms (URL's 300 is ignored)
      expect(normalized.connect_timeout_ms).equals(60_000);
    });

    test('explicit config value without URI', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        hostname: 'localhost',
        port: 4321,
        database: 'powersync_test',
        username: 'postgres',
        password: 'postgres',
        connect_timeout: 10
      });
      expect(normalized.connect_timeout_ms).equals(10_000);
    });

    test('converts seconds to milliseconds', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?connect_timeout=5'
      });
      expect(normalized.connect_timeout_ms).equals(5_000);
    });

    test('handles fractional seconds', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?connect_timeout=1.5'
      });
      expect(normalized.connect_timeout_ms).equals(1_500);
    });
  });
});

describe('parseConnectTimeout', () => {
  test('returns undefined when no value provided', () => {
    expect(parseConnectTimeout(undefined, undefined)).toBeUndefined();
    expect(parseConnectTimeout(undefined, null)).toBeUndefined();
  });

  test('parses valid explicit value', () => {
    expect(parseConnectTimeout(30, undefined)).equals(30_000);
  });

  test('parses valid URI query value', () => {
    expect(parseConnectTimeout(undefined, '300')).equals(300_000);
  });

  test('explicit value takes precedence over URI value', () => {
    expect(parseConnectTimeout(60, '300')).equals(60_000);
  });

  // Invalid values should be silently ignored
  test('ignores NaN explicit value', () => {
    expect(parseConnectTimeout(NaN, undefined)).toBeUndefined();
  });

  test('ignores negative explicit value', () => {
    expect(parseConnectTimeout(-5, undefined)).toBeUndefined();
  });

  test('ignores zero explicit value', () => {
    expect(parseConnectTimeout(0, undefined)).toBeUndefined();
  });

  test('ignores Infinity explicit value', () => {
    expect(parseConnectTimeout(Infinity, undefined)).toBeUndefined();
  });

  test('ignores non-numeric URI value', () => {
    expect(parseConnectTimeout(undefined, 'abc')).toBeUndefined();
  });

  test('ignores empty string URI value', () => {
    expect(parseConnectTimeout(undefined, '')).toBeUndefined();
  });

  test('ignores negative URI value', () => {
    expect(parseConnectTimeout(undefined, '-10')).toBeUndefined();
  });

  test('ignores zero URI value', () => {
    expect(parseConnectTimeout(undefined, '0')).toBeUndefined();
  });

  test('invalid explicit value does NOT fall through to URI value', () => {
    // If explicit is set but invalid, we don't fall back to URI
    expect(parseConnectTimeout(NaN, '300')).toBeUndefined();
    expect(parseConnectTimeout(-5, '300')).toBeUndefined();
  });
});
