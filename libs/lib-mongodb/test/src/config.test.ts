import { describe, expect, test } from 'vitest';
import { normalizeMongoConfig, parseMongoConnectionParam, parseMongoConnectionParams } from '../../src/types/types.js';
import { LookupAddress } from 'node:dns';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';

describe('config', () => {
  test('Should normalize a simple URI', () => {
    const uri = 'mongodb://localhost:27017/powersync_test';
    const normalized = normalizeMongoConfig({
      type: 'mongodb',
      uri
    });
    expect(normalized.uri).equals(uri);
    expect(normalized.database).equals('powersync_test');
  });

  test('Should normalize an URI with auth', () => {
    const uri = 'mongodb://user:pass@localhost:27017/powersync_test';
    const normalized = normalizeMongoConfig({
      type: 'mongodb',
      uri
    });
    expect(normalized.uri).equals(uri.replace('user:pass@', ''));
    expect(normalized.database).equals('powersync_test');
  });

  test('Should normalize an URI with query params', () => {
    const uri = 'mongodb://localhost:27017/powersync_test?query=test';
    const normalized = normalizeMongoConfig({
      type: 'mongodb',
      uri
    });
    expect(normalized.uri).equals(uri);
    expect(normalized.database).equals('powersync_test');
  });

  test('Should normalize a replica set URI', () => {
    const uri =
      'mongodb://mongodb-0.mongodb.powersync.svc.cluster.local:27017,mongodb-1.mongodb.powersync.svc.cluster.local:27017,mongodb-2.mongodb.powersync.svc.cluster.local:27017/powersync_test?replicaSet=rs0';
    const normalized = normalizeMongoConfig({
      type: 'mongodb',
      uri
    });
    expect(normalized.uri).equals(uri);
    expect(normalized.database).equals('powersync_test');
  });

  test('Should normalize a replica set URI with auth', () => {
    const uri =
      'mongodb://user:pass@mongodb-0.mongodb.powersync.svc.cluster.local:27017,mongodb-1.mongodb.powersync.svc.cluster.local:27017,mongodb-2.mongodb.powersync.svc.cluster.local:27017/powersync_test?replicaSet=rs0';
    const normalized = normalizeMongoConfig({
      type: 'mongodb',
      uri
    });
    expect(normalized.uri).equals(uri.replace('user:pass@', ''));
    expect(normalized.database).equals('powersync_test');
    expect(normalized.username).equals('user');
    expect(normalized.password).equals('pass');
  });

  test('Should normalize a +srv URI', () => {
    const uri = 'mongodb+srv://user:pass@localhost/powersync_test';
    const normalized = normalizeMongoConfig({
      type: 'mongodb',
      uri
    });
    expect(normalized.uri).equals(uri.replace('user:pass@', ''));
    expect(normalized.database).equals('powersync_test');
    expect(normalized.username).equals('user');
    expect(normalized.password).equals('pass');
  });

  test('Should prioritize username and password that are specified explicitly', () => {
    const uri = 'mongodb://user:pass@localhost:27017/powersync_test';
    const normalized = normalizeMongoConfig({
      type: 'mongodb',
      uri,
      username: 'user2',
      password: 'pass2'
    });
    expect(normalized.uri).equals(uri.replace('user:pass@', ''));
    expect(normalized.database).equals('powersync_test');
    expect(normalized.username).equals('user2');
    expect(normalized.password).equals('pass2');
  });

  test('Should make a lookup function for a single IP host', async () => {
    let err: ServiceError | undefined;
    try {
      normalizeMongoConfig({
        type: 'mongodb',
        uri: 'mongodb://127.0.0.1/powersync_test',
        reject_ip_ranges: ['127.0.0.1/0']
      });
    } catch (e) {
      err = e as ServiceError;
    }

    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2203);
  });

  test('Should make a lookup function for a single hostname', async () => {
    const lookup = normalizeMongoConfig({
      type: 'mongodb',
      uri: 'mongodb://host/powersync_test',
      reject_ip_ranges: ['host']
    }).lookup;

    const result = await new Promise((resolve, reject) => {
      lookup!('host', {}, (e, address) => {
        resolve(e);
      });
    });

    expect(result instanceof Error).toBe(true);
  });

  test('Should make a lookup function for multiple IP hosts', async () => {
    let err: ServiceError | undefined;
    try {
      normalizeMongoConfig({
        type: 'mongodb',
        uri: 'mongodb://127.0.0.1,127.0.0.2/powersync_test',
        reject_ip_ranges: ['127.0.0.1/0']
      });
    } catch (e) {
      err = e as ServiceError;
    }

    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2203);
  });

  test('Should make a lookup function for multiple hosts', async () => {
    const lookup = normalizeMongoConfig({
      type: 'mongodb',
      uri: 'mongodb://host1,host2/powersync_test',
      reject_ip_ranges: ['host1']
    }).lookup;

    const result = await new Promise((resolve, reject) => {
      lookup!('host1', {}, (e, address) => {
        resolve(e);
      });
    });

    expect(result instanceof Error).toBe(true);
  });

  describe('connection parameters', () => {
    test('parses all connection parameters from URL query string', () => {
      const uri =
        'mongodb://localhost:27017/powersync_test?connectTimeoutMS=5000&socketTimeoutMS=30000&serverSelectionTimeoutMS=15000&maxPoolSize=20&maxIdleTimeMS=120000';
      const normalized = normalizeMongoConfig({
        type: 'mongodb',
        uri
      });
      expect(normalized.connectionParams.connectTimeoutMS).equals(5000);
      expect(normalized.connectionParams.socketTimeoutMS).equals(30000);
      expect(normalized.connectionParams.serverSelectionTimeoutMS).equals(15000);
      expect(normalized.connectionParams.maxPoolSize).equals(20);
      expect(normalized.connectionParams.maxIdleTimeMS).equals(120000);
    });

    test('URL without connection parameters returns empty connectionParams', () => {
      const normalized = normalizeMongoConfig({
        type: 'mongodb',
        uri: 'mongodb://localhost:27017/powersync_test'
      });
      expect(normalized.connectionParams).toEqual({});
    });

    test('parameters can be partially specified', () => {
      const uri = 'mongodb://localhost:27017/powersync_test?connectTimeoutMS=5000&maxPoolSize=20';
      const normalized = normalizeMongoConfig({
        type: 'mongodb',
        uri
      });
      expect(normalized.connectionParams.connectTimeoutMS).equals(5000);
      expect(normalized.connectionParams.maxPoolSize).equals(20);
      expect(normalized.connectionParams.socketTimeoutMS).toBeUndefined();
      expect(normalized.connectionParams.serverSelectionTimeoutMS).toBeUndefined();
      expect(normalized.connectionParams.maxIdleTimeMS).toBeUndefined();
    });

    test('preserves other query parameters alongside connection params', () => {
      const uri = 'mongodb://localhost:27017/powersync_test?replicaSet=rs0&connectTimeoutMS=5000';
      const normalized = normalizeMongoConfig({
        type: 'mongodb',
        uri
      });
      expect(normalized.connectionParams.connectTimeoutMS).equals(5000);
      // The URI should still contain the replicaSet param
      expect(normalized.uri).toContain('replicaSet=rs0');
    });

    test('ignores invalid (non-numeric) connection parameter values', () => {
      const uri = 'mongodb://localhost:27017/powersync_test?connectTimeoutMS=abc&maxPoolSize=xyz';
      const normalized = normalizeMongoConfig({
        type: 'mongodb',
        uri
      });
      expect(normalized.connectionParams.connectTimeoutMS).toBeUndefined();
      expect(normalized.connectionParams.maxPoolSize).toBeUndefined();
    });

    test('ignores negative connection parameter values', () => {
      const uri = 'mongodb://localhost:27017/powersync_test?connectTimeoutMS=-5000&maxPoolSize=-10';
      const normalized = normalizeMongoConfig({
        type: 'mongodb',
        uri
      });
      expect(normalized.connectionParams.connectTimeoutMS).toBeUndefined();
      expect(normalized.connectionParams.maxPoolSize).toBeUndefined();
    });

    test('ignores zero connection parameter values', () => {
      const uri = 'mongodb://localhost:27017/powersync_test?connectTimeoutMS=0&maxPoolSize=0';
      const normalized = normalizeMongoConfig({
        type: 'mongodb',
        uri
      });
      expect(normalized.connectionParams.connectTimeoutMS).toBeUndefined();
      expect(normalized.connectionParams.maxPoolSize).toBeUndefined();
    });

    test('works with replica set URI and connection params', () => {
      const uri = 'mongodb://host1:27017,host2:27017,host3:27017/powersync_test?replicaSet=rs0&connectTimeoutMS=10000';
      const normalized = normalizeMongoConfig({
        type: 'mongodb',
        uri
      });
      expect(normalized.connectionParams.connectTimeoutMS).equals(10000);
      expect(normalized.database).equals('powersync_test');
    });
  });

  describe('errors', () => {
    test('Should throw error when no database specified', () => {
      ['mongodb://localhost:27017', 'mongodb://localhost:27017/'].forEach((uri) => {
        expect(() =>
          normalizeMongoConfig({
            type: 'mongodb',
            uri
          })
        ).toThrow('[PSYNC_S1105] MongoDB connection: database required');
      });
    });

    test('Should throw error when URI has invalid scheme', () => {
      expect(() =>
        normalizeMongoConfig({
          type: 'mongodb',
          uri: 'not-a-uri'
        })
      ).toThrow('[PSYNC_S1109] MongoDB connection: invalid URI');
    });

    test('Should throw error when URI has invalid host', () => {
      expect(() =>
        normalizeMongoConfig({
          type: 'mongodb',
          uri: 'mongodb://'
        })
      ).toThrow('[PSYNC_S1109] MongoDB connection: invalid URI');
    });
  });
});

describe('parseMongoConnectionParam', () => {
  test('returns undefined when no value provided', () => {
    expect(parseMongoConnectionParam(undefined)).toBeUndefined();
    expect(parseMongoConnectionParam(null)).toBeUndefined();
  });

  test('parses valid numeric string', () => {
    expect(parseMongoConnectionParam('5000')).equals(5000);
  });

  test('parses fractional values', () => {
    expect(parseMongoConnectionParam('1500.5')).equals(1500.5);
  });

  test('ignores non-numeric string', () => {
    expect(parseMongoConnectionParam('abc')).toBeUndefined();
  });

  test('ignores empty string', () => {
    expect(parseMongoConnectionParam('')).toBeUndefined();
  });

  test('ignores negative value', () => {
    expect(parseMongoConnectionParam('-5000')).toBeUndefined();
  });

  test('ignores zero', () => {
    expect(parseMongoConnectionParam('0')).toBeUndefined();
  });

  test('ignores Infinity', () => {
    expect(parseMongoConnectionParam('Infinity')).toBeUndefined();
  });

  test('ignores NaN', () => {
    expect(parseMongoConnectionParam('NaN')).toBeUndefined();
  });
});

describe('parseMongoConnectionParams', () => {
  test('parses all supported parameters', () => {
    const params = new URLSearchParams(
      'connectTimeoutMS=5000&socketTimeoutMS=30000&serverSelectionTimeoutMS=15000&maxPoolSize=20&maxIdleTimeMS=120000'
    );
    const result = parseMongoConnectionParams(params);
    expect(result).toEqual({
      connectTimeoutMS: 5000,
      socketTimeoutMS: 30000,
      serverSelectionTimeoutMS: 15000,
      maxPoolSize: 20,
      maxIdleTimeMS: 120000
    });
  });

  test('returns empty object when no connection params present', () => {
    const params = new URLSearchParams('replicaSet=rs0&authSource=admin');
    const result = parseMongoConnectionParams(params);
    expect(result).toEqual({});
  });

  test('ignores invalid values and only includes valid ones', () => {
    const params = new URLSearchParams('connectTimeoutMS=5000&socketTimeoutMS=abc&maxPoolSize=-10');
    const result = parseMongoConnectionParams(params);
    expect(result).toEqual({
      connectTimeoutMS: 5000
    });
  });

  test('handles partial parameter specification', () => {
    const params = new URLSearchParams('connectTimeoutMS=5000&maxIdleTimeMS=120000');
    const result = parseMongoConnectionParams(params);
    expect(result).toEqual({
      connectTimeoutMS: 5000,
      maxIdleTimeMS: 120000
    });
  });
});
