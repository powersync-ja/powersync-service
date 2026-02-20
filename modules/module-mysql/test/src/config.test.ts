import { describe, expect, test } from 'vitest';
import {
  normalizeConnectionConfig,
  parseMySQLConnectionParam,
  parseMySQLConnectionParams
} from '@module/types/types.js';

describe('config', () => {
  test('Should resolve database', () => {
    const normalized = normalizeConnectionConfig({
      type: 'mysql',
      uri: 'mysql://user:pass@localhost:3306/mydb'
    });
    expect(normalized.database).equals('mydb');
  });

  describe('connection parameters', () => {
    test('parses all connection parameters from URL query string', () => {
      const normalized = normalizeConnectionConfig({
        type: 'mysql',
        uri: 'mysql://user:pass@localhost:3306/mydb?connectTimeout=5000&connectionLimit=20&queueLimit=100'
      });
      expect(normalized.connectionParams.connectTimeout).equals(5000);
      expect(normalized.connectionParams.connectionLimit).equals(20);
      expect(normalized.connectionParams.queueLimit).equals(100);
    });

    test('URL without connection parameters returns empty connectionParams', () => {
      const normalized = normalizeConnectionConfig({
        type: 'mysql',
        uri: 'mysql://user:pass@localhost:3306/mydb'
      });
      expect(normalized.connectionParams).toEqual({});
    });

    test('parameters can be partially specified', () => {
      const normalized = normalizeConnectionConfig({
        type: 'mysql',
        uri: 'mysql://user:pass@localhost:3306/mydb?connectTimeout=5000&queueLimit=50'
      });
      expect(normalized.connectionParams.connectTimeout).equals(5000);
      expect(normalized.connectionParams.queueLimit).equals(50);
      expect(normalized.connectionParams.connectionLimit).toBeUndefined();
    });

    test('ignores invalid (non-numeric) connection parameter values', () => {
      const normalized = normalizeConnectionConfig({
        type: 'mysql',
        uri: 'mysql://user:pass@localhost:3306/mydb?connectTimeout=abc&connectionLimit=xyz'
      });
      expect(normalized.connectionParams.connectTimeout).toBeUndefined();
      expect(normalized.connectionParams.connectionLimit).toBeUndefined();
    });

    test('ignores negative connection parameter values', () => {
      const normalized = normalizeConnectionConfig({
        type: 'mysql',
        uri: 'mysql://user:pass@localhost:3306/mydb?connectTimeout=-5000&connectionLimit=-10'
      });
      expect(normalized.connectionParams.connectTimeout).toBeUndefined();
      expect(normalized.connectionParams.connectionLimit).toBeUndefined();
    });

    test('ignores zero connection parameter values', () => {
      const normalized = normalizeConnectionConfig({
        type: 'mysql',
        uri: 'mysql://user:pass@localhost:3306/mydb?connectTimeout=0&connectionLimit=0&queueLimit=0'
      });
      expect(normalized.connectionParams.connectTimeout).toBeUndefined();
      expect(normalized.connectionParams.connectionLimit).toBeUndefined();
      expect(normalized.connectionParams.queueLimit).toBeUndefined();
    });

    test('works without URI (config-only)', () => {
      const normalized = normalizeConnectionConfig({
        type: 'mysql',
        hostname: 'localhost',
        port: 3306,
        database: 'mydb',
        username: 'user',
        password: 'pass'
      });
      expect(normalized.connectionParams).toEqual({});
    });
  });
});

describe('parseMySQLConnectionParam', () => {
  test('returns undefined when no value provided', () => {
    expect(parseMySQLConnectionParam(undefined)).toBeUndefined();
    expect(parseMySQLConnectionParam(null)).toBeUndefined();
  });

  test('parses valid numeric string', () => {
    expect(parseMySQLConnectionParam('5000')).equals(5000);
  });

  test('parses fractional values', () => {
    expect(parseMySQLConnectionParam('1500.5')).equals(1500.5);
  });

  test('ignores non-numeric string', () => {
    expect(parseMySQLConnectionParam('abc')).toBeUndefined();
  });

  test('ignores empty string', () => {
    expect(parseMySQLConnectionParam('')).toBeUndefined();
  });

  test('ignores negative value', () => {
    expect(parseMySQLConnectionParam('-5000')).toBeUndefined();
  });

  test('ignores zero', () => {
    expect(parseMySQLConnectionParam('0')).toBeUndefined();
  });

  test('ignores Infinity', () => {
    expect(parseMySQLConnectionParam('Infinity')).toBeUndefined();
  });

  test('ignores NaN', () => {
    expect(parseMySQLConnectionParam('NaN')).toBeUndefined();
  });
});

describe('parseMySQLConnectionParams', () => {
  test('parses all supported parameters', () => {
    const params = new URLSearchParams('connectTimeout=5000&connectionLimit=20&queueLimit=100');
    const result = parseMySQLConnectionParams(params);
    expect(result).toEqual({
      connectTimeout: 5000,
      connectionLimit: 20,
      queueLimit: 100
    });
  });

  test('returns empty object when no connection params present', () => {
    const params = new URLSearchParams('someOther=value');
    const result = parseMySQLConnectionParams(params);
    expect(result).toEqual({});
  });

  test('returns empty object for undefined searchParams', () => {
    const result = parseMySQLConnectionParams(undefined);
    expect(result).toEqual({});
  });

  test('ignores invalid values and only includes valid ones', () => {
    const params = new URLSearchParams('connectTimeout=5000&connectionLimit=abc&queueLimit=-10');
    const result = parseMySQLConnectionParams(params);
    expect(result).toEqual({
      connectTimeout: 5000
    });
  });

  test('handles partial parameter specification', () => {
    const params = new URLSearchParams('connectTimeout=5000&queueLimit=100');
    const result = parseMySQLConnectionParams(params);
    expect(result).toEqual({
      connectTimeout: 5000,
      queueLimit: 100
    });
  });
});
