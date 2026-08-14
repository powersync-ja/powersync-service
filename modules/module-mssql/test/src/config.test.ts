import { createConnectionPoolConfig } from '@module/replication/MSSQLConnectionManager.js';
import { normalizeConnectionConfig } from '@module/types/types.js';
import { describe, expect, test } from 'vitest';

const BASE_CONFIG = {
  type: 'mssql' as const,
  uri: 'mssql://user:password@localhost:1433/powersync_test'
};

describe('SQL Server connection config', () => {
  test('passes a custom CA to the SQL Server TLS configuration', () => {
    const cacert = `-----BEGIN CERTIFICATE-----
test certificate
-----END CERTIFICATE-----`;
    const normalized = normalizeConnectionConfig({ ...BASE_CONFIG, cacert });

    expect(normalized.cacert).toBe(cacert);
    expect(createConnectionPoolConfig(normalized, {}).options).toMatchObject({
      encrypt: true,
      trustServerCertificate: false,
      cryptoCredentialsDetails: { ca: cacert }
    });
  });

  test('passes a custom TLS server name to the SQL Server TLS configuration', () => {
    const normalized = normalizeConnectionConfig({ ...BASE_CONFIG, tls_servername: 'sql.internal.example' });

    expect(createConnectionPoolConfig(normalized, {}).options?.serverName).toBe('sql.internal.example');
  });

  test('rejects a custom CA when server certificate validation is disabled', () => {
    expect(() =>
      normalizeConnectionConfig({
        ...BASE_CONFIG,
        cacert: 'certificate',
        additionalConfig: { trustServerCertificate: true }
      })
    ).toThrow(/cacert cannot be used with trustServerCertificate/);
  });

  test('does not configure a custom TLS context without a custom CA', () => {
    const normalized = normalizeConnectionConfig(BASE_CONFIG);

    expect(createConnectionPoolConfig(normalized, {}).options?.cryptoCredentialsDetails).toBeUndefined();
  });

  test('defaults heartbeat_interval_seconds to 60 seconds', () => {
    expect(normalizeConnectionConfig(BASE_CONFIG).heartbeat_interval_seconds).toBe(60);
  });

  test('uses the default for a null heartbeat_interval_seconds', () => {
    expect(
      normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds: null }).heartbeat_interval_seconds
    ).toBe(60);
  });

  test('allows the SQL Server maximum heartbeat interval', () => {
    expect(
      normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds: 60 }).heartbeat_interval_seconds
    ).toBe(60);
  });

  test.each([0, 4, 61, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects invalid heartbeat_interval_seconds: %s',
    (heartbeat_interval_seconds) => {
      expect(() => normalizeConnectionConfig({ ...BASE_CONFIG, heartbeat_interval_seconds })).toThrow(
        'heartbeat_interval_seconds'
      );
    }
  );
});
