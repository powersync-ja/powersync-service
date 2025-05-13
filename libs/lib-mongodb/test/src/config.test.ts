import { describe, expect, test } from 'vitest';
import { normalizeMongoConfig } from '../../src/types/types.js';
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
