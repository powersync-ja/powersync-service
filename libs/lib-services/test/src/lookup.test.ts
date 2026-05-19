import { describe, expect, test } from 'vitest';

import { ErrorCode, ServiceError } from '../../src/errors/errors-index.js';
import { hostWithoutPort, validateIpHostname } from '../../src/ip/lookup.js';

function captureValidationError(hostname: string, options: { reject_ip_ranges: string[]; reject_ipv6?: boolean }) {
  let err: ServiceError | undefined;
  try {
    validateIpHostname(hostname, options);
  } catch (e) {
    err = e as ServiceError;
  }
  return err;
}

describe('validateIpHostname', () => {
  test('rejects an IPv4 literal in a blocked range', () => {
    const err = captureValidationError('127.0.0.1', { reject_ip_ranges: ['local'] });
    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2203);
  });

  test('rejects an IPv4 literal with an explicit port', () => {
    const err = captureValidationError('127.0.0.1:8080', { reject_ip_ranges: ['local'] });
    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2203);
  });

  test('treats unknown hostnames as DNS names', () => {
    expect(captureValidationError('example.com', { reject_ip_ranges: ['local'] })).toBeUndefined();
  });

  test('rejects a bracketed IPv6 loopback literal as used by URL.hostname', () => {
    const err = captureValidationError('[::1]', { reject_ip_ranges: ['local'] });
    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2203);
  });

  test('rejects a bracketed IPv6 literal against an explicit CIDR', () => {
    const err = captureValidationError('[::1]', { reject_ip_ranges: ['::1/128'] });
    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2203);
  });

  test('rejects a bracketed IPv6 literal when reject_ipv6 is set', () => {
    const err = captureValidationError('[::1]', { reject_ip_ranges: [], reject_ipv6: true });
    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2202);
  });
});

describe('hostWithoutPort', () => {
  test('returns a bare hostname unchanged', () => {
    expect(hostWithoutPort('example.com')).toEqual('example.com');
  });

  test('strips a port suffix from a hostname', () => {
    expect(hostWithoutPort('example.com:8080')).toEqual('example.com');
  });

  test('strips a port suffix from an IPv4 literal', () => {
    expect(hostWithoutPort('127.0.0.1:27017')).toEqual('127.0.0.1');
  });

  test('unwraps a bracketed IPv6 literal', () => {
    expect(hostWithoutPort('[::1]')).toEqual('::1');
  });

  test('unwraps a bracketed IPv6 literal with a port', () => {
    expect(hostWithoutPort('[::1]:27017')).toEqual('::1');
  });

  test('leaves a bare IPv6 literal unchanged', () => {
    // IPv6 with a port is always bracketed; bare multi-colon input has no port to strip.
    expect(hostWithoutPort('::1')).toEqual('::1');
    expect(hostWithoutPort('2001:db8::1')).toEqual('2001:db8::1');
  });

  test('leaves trailing non-numeric segments alone', () => {
    expect(hostWithoutPort('example.com:not-a-port')).toEqual('example.com:not-a-port');
  });
});
