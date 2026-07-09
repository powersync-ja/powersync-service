import { describe, expect, test } from 'vitest';
import { normalizeConnectionConfig } from '../../src/types/types.js';

describe('config', () => {
  describe('replication_socket_timeout', () => {
    test('normalizes replication socket timeout from seconds to milliseconds', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test',
        replication_socket_timeout: 45
      });

      expect(normalized.replication_socket_timeout_ms).equals(45_000);
    });

    test('leaves replication socket timeout unset by default', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test'
      });

      expect(normalized.replication_socket_timeout_ms).toBeUndefined();
    });

    test('ignores invalid replication socket timeout values', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test',
        replication_socket_timeout: 0
      });

      expect(normalized.replication_socket_timeout_ms).toBeUndefined();
    });
  });
});
