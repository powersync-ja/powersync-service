import { describe, expect, test } from 'vitest';

import { normalizeConnectionConfig } from '../../src/types/types.js';

describe('config', () => {
  test('Should normalize a simple URI', () => {
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri: 'postgresql://postgres:postgres@localhost:4321/powersync_test'
    });
    expect(normalized.database).equals('powersync_test');
    expect(normalized.hostname).equals('localhost');
    expect(normalized.port).equals(4321);
    expect(normalized.username).equals('postgres');
    expect(normalized.password).equals('postgres');
  });

  test('Should normalize an URI with auth', () => {
    const uri = 'postgresql://user:pass@localhost:5432/powersync_test';
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri
    });
    expect(normalized.database).equals('powersync_test');
    expect(normalized.username).equals('user');
    expect(normalized.password).equals('pass');
  });

  test('Should normalize an URI with query params', () => {
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri: 'postgresql://user:pass@host/db?other=param'
    });
    expect(normalized.database).equals('db');
  });

  test('Should prioritize username and password that are specified explicitly', () => {
    const uri = 'postgresql://user:pass@localhost:5432/powersync_test';
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri,
      username: 'user2',
      password: 'pass2'
    });
    expect(normalized.username).equals('user2');
    expect(normalized.password).equals('pass2');
  });

  test('Should parse connection parameters from URI query string', () => {
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri: 'postgresql://user:pass@host/db?connect_timeout=300&keepalives=1&keepalives_idle=60&keepalives_interval=10&keepalives_count=10'
    });
    expect(normalized.connect_timeout).equals(300);
    expect(normalized.keepalives).equals(1);
    expect(normalized.keepalives_idle).equals(60);
    expect(normalized.keepalives_interval).equals(10);
    expect(normalized.keepalives_count).equals(10);
  });

  test('Should prioritize explicit config over URI query params', () => {
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri: 'postgresql://user:pass@host/db?connect_timeout=300&keepalives_idle=60',
      connect_timeout: 600,
      keepalives_idle: 120
    });
    expect(normalized.connect_timeout).equals(600);
    expect(normalized.keepalives_idle).equals(120);
  });

  test('Should handle partial query parameters', () => {
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri: 'postgresql://user:pass@host/db?connect_timeout=300'
    });
    expect(normalized.connect_timeout).equals(300);
    expect(normalized.keepalives).toBeUndefined();
    expect(normalized.keepalives_idle).toBeUndefined();
  });

  test('Should ignore invalid query parameter values', () => {
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri: 'postgresql://user:pass@host/db?connect_timeout=invalid&keepalives_idle=-5'
    });
    expect(normalized.connect_timeout).toBeUndefined();
    expect(normalized.keepalives_idle).toBeUndefined();
  });

  test('Should handle keepalives=0 to disable keepalives', () => {
    const normalized = normalizeConnectionConfig({
      type: 'postgresql',
      uri: 'postgresql://user:pass@host/db?keepalives=0'
    });
    expect(normalized.keepalives).equals(0);
  });

  describe('errors', () => {
    test('Should throw error when no database specified', () => {
      ['postgresql://user:pass@localhost:5432', 'postgresql://user:pass@localhost:5432/'].forEach((uri) => {
        expect(() =>
          normalizeConnectionConfig({
            type: 'postgresql',
            uri
          })
        ).toThrow('[PSYNC_S1105] Postgres connection: database required');
      });
    });

    test('Should throw error when URI has invalid scheme', () => {
      expect(() =>
        normalizeConnectionConfig({
          type: 'postgresql',
          uri: 'http://user:pass@localhost:5432/powersync_test'
        })
      ).toThrow('[PSYNC_S1109] Invalid URI - protocol must be postgresql');
    });

    test('Should throw error when hostname is missing', () => {
      expect(() =>
        normalizeConnectionConfig({
          type: 'postgresql',
          uri: 'postgresql://user:pass@/powersync_test'
        })
      ).toThrow('[PSYNC_S1106] Postgres connection: hostname required');
    });

    test('Should throw error when username is missing', () => {
      expect(() =>
        normalizeConnectionConfig({
          type: 'postgresql',
          uri: 'postgresql://localhost:5432/powersync_test'
        })
      ).toThrow('[PSYNC_S1107] Postgres connection: username required');
    });

    test('Should throw error when password is missing', () => {
      expect(() =>
        normalizeConnectionConfig({
          type: 'postgresql',
          uri: 'postgresql://user@localhost:5432/powersync_test'
        })
      ).toThrow('[PSYNC_S1108] Postgres connection: password required');
    });
  });
});
