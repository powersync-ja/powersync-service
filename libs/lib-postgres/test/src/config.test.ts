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

  describe('connection parameters', () => {
    test('Should parse connect_timeout from URI', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?connect_timeout=300'
      });
      expect(normalized.connection_parameters).toBeDefined();
      expect(normalized.connection_parameters?.connect_timeout).equals(300);
    });

    test('Should parse keepalives from URI', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?keepalives=1'
      });
      expect(normalized.connection_parameters).toBeDefined();
      expect(normalized.connection_parameters?.keepalives).equals(1);
    });

    test('Should parse keepalives=0 (disabled) from URI', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?keepalives=0'
      });
      expect(normalized.connection_parameters).toBeDefined();
      expect(normalized.connection_parameters?.keepalives).equals(0);
    });

    test('Should parse multiple connection parameters from URI', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?connect_timeout=300&keepalives=1&keepalives_idle=60&keepalives_interval=10&keepalives_count=10'
      });
      expect(normalized.connection_parameters).toBeDefined();
      expect(normalized.connection_parameters?.connect_timeout).equals(300);
      expect(normalized.connection_parameters?.keepalives).equals(1);
      expect(normalized.connection_parameters?.keepalives_idle).equals(60);
      expect(normalized.connection_parameters?.keepalives_interval).equals(10);
      expect(normalized.connection_parameters?.keepalives_count).equals(10);
    });

    test('Should return undefined connection_parameters when no query params', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test'
      });
      expect(normalized.connection_parameters).toBeUndefined();
    });

    test('Should ignore unknown query parameters', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?unknown_param=value&connect_timeout=30'
      });
      expect(normalized.connection_parameters).toBeDefined();
      expect(normalized.connection_parameters?.connect_timeout).equals(30);
      expect((normalized.connection_parameters as any)?.unknown_param).toBeUndefined();
    });

    test('Should throw error for invalid numeric parameter', () => {
      expect(() =>
        normalizeConnectionConfig({
          type: 'postgresql',
          uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?connect_timeout=invalid'
        })
      ).toThrow('Invalid connection parameter: connect_timeout must be a number');
    });

    test('Should handle URL-encoded parameters', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test?connect_timeout=60'
      });
      expect(normalized.connection_parameters?.connect_timeout).equals(60);
    });

    test('Should work without URI (explicit config)', () => {
      const normalized = normalizeConnectionConfig({
        type: 'postgresql',
        hostname: 'localhost',
        port: 4321,
        database: 'powersync_test',
        username: 'postgres',
        password: 'postgres'
      });
      expect(normalized.connection_parameters).toBeUndefined();
    });
  });
});
