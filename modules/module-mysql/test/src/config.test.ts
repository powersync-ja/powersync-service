import { describe, expect, test } from 'vitest';

import { normalizeConnectionConfig } from '../../src/types/types.js';

describe('config', () => {
  test('Should normalize a simple URI', () => {
    const normalized = normalizeConnectionConfig({
      type: 'mysql',
      uri: 'mysql://user:pass@localhost:3306/powersync_test'
    });
    expect(normalized.database).equals('powersync_test');
    expect(normalized.hostname).equals('localhost');
    expect(normalized.port).equals(3306);
    expect(normalized.username).equals('user');
    expect(normalized.password).equals('pass');
  });

  test('Should normalize an URI with auth', () => {
    const uri = 'mysql://user:pass@localhost:3306/powersync_test';
    const normalized = normalizeConnectionConfig({
      type: 'mysql',
      uri
    });
    expect(normalized.database).equals('powersync_test');
    expect(normalized.username).equals('user');
    expect(normalized.password).equals('pass');
  });

  test('Should normalize an URI with query params', () => {
    const normalized = normalizeConnectionConfig({
      type: 'mysql',
      uri: 'mysql://user:pass@host/db?other=param'
    });
    expect(normalized.database).equals('db');
  });

  test('Should prioritize username and password that are specified explicitly', () => {
    const uri = 'mysql://user:pass@localhost:3306/powersync_test';
    const normalized = normalizeConnectionConfig({
      type: 'mysql',
      uri,
      username: 'user2',
      password: 'pass2'
    });
    expect(normalized.username).equals('user2');
    expect(normalized.password).equals('pass2');
  });

  test('Should parse connection parameters from URI query string', () => {
    const normalized = normalizeConnectionConfig({
      type: 'mysql',
      uri: 'mysql://user:pass@host/db?connectTimeout=10000&connectionLimit=20&queueLimit=10&timeout=30000'
    });
    expect(normalized.connectTimeout).equals(10000);
    expect(normalized.connectionLimit).equals(20);
    expect(normalized.queueLimit).equals(10);
    expect(normalized.timeout).equals(30000);
  });

  test('Should prioritize explicit config over URI query params', () => {
    const normalized = normalizeConnectionConfig({
      type: 'mysql',
      uri: 'mysql://user:pass@host/db?connectTimeout=10000&connectionLimit=20',
      connectTimeout: 20000,
      connectionLimit: 30
    });
    expect(normalized.connectTimeout).equals(20000);
    expect(normalized.connectionLimit).equals(30);
  });

  test('Should handle partial query parameters', () => {
    const normalized = normalizeConnectionConfig({
      type: 'mysql',
      uri: 'mysql://user:pass@host/db?connectTimeout=10000'
    });
    expect(normalized.connectTimeout).equals(10000);
    expect(normalized.connectionLimit).toBeUndefined();
    expect(normalized.queueLimit).toBeUndefined();
  });

  test('Should ignore invalid query parameter values', () => {
    const normalized = normalizeConnectionConfig({
      type: 'mysql',
      uri: 'mysql://user:pass@host/db?connectTimeout=invalid&connectionLimit=-5'
    });
    expect(normalized.connectTimeout).toBeUndefined();
    expect(normalized.connectionLimit).toBeUndefined();
  });

  describe('errors', () => {
    test('Should throw error when no database specified', () => {
      ['mysql://user:pass@localhost:3306', 'mysql://user:pass@localhost:3306/'].forEach((uri) => {
        expect(() =>
          normalizeConnectionConfig({
            type: 'mysql',
            uri
          })
        ).toThrow('[PSYNC_S1105] MySQL connection: database required');
      });
    });

    test('Should throw error when URI has invalid scheme', () => {
      expect(() =>
        normalizeConnectionConfig({
          type: 'mysql',
          uri: 'http://user:pass@localhost:3306/powersync_test'
        })
      ).toThrow('[PSYNC_S1109] Invalid URI - protocol must be mysql');
    });

    test('Should throw error when hostname is missing', () => {
      expect(() =>
        normalizeConnectionConfig({
          type: 'mysql',
          uri: 'mysql://user:pass@/powersync_test'
        })
      ).toThrow('[PSYNC_S1106] MySQL connection: hostname required');
    });

    test('Should throw error when username is missing', () => {
      expect(() =>
        normalizeConnectionConfig({
          type: 'mysql',
          uri: 'mysql://localhost:3306/powersync_test'
        })
      ).toThrow('[PSYNC_S1107] MySQL connection: username required');
    });

    test('Should throw error when password is missing', () => {
      expect(() =>
        normalizeConnectionConfig({
          type: 'mysql',
          uri: 'mysql://user@localhost:3306/powersync_test'
        })
      ).toThrow('[PSYNC_S1108] MySQL connection: password required');
    });
  });
});
