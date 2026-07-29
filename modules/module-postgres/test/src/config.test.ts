import { describe, expect, test } from 'vitest';
import { normalizeConnectionConfig } from '../../src/types/types.js';

describe('config', () => {
  describe('snapshot_socket_timeout', () => {
    test('normalizes snapshot socket timeout from seconds to milliseconds', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test',
        snapshot_socket_timeout: 90
      });

      expect(normalized.snapshot_socket_timeout_ms).equals(90_000);
    });

    test('leaves snapshot socket timeout unset by default', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test'
      });

      expect(normalized.snapshot_socket_timeout_ms).toBeUndefined();
    });

    test('ignores invalid snapshot socket timeout values', () => {
      for (const invalid of [0, -5, NaN, Infinity]) {
        const normalized = normalizeConnectionConfig({
          type: 'postgresql',
          uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test',
          snapshot_socket_timeout: invalid
        });

        expect(normalized.snapshot_socket_timeout_ms, `value ${invalid}`).toBeUndefined();
      }
    });
  });
});
