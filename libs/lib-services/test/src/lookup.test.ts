import { describe, expect, test } from 'vitest';

import { ErrorCode, ServiceError } from '../../src/errors/errors-index.js';
import { hostnameFromSocketAddress, validateIpHostname } from '../../src/ip/lookup.js';

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

  test('rejects a bare IPv6 literal in a blocked range', () => {
    const err = captureValidationError('::1', { reject_ip_ranges: ['local'] });
    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2203);
  });

  test('rejects a bare IPv6 literal against an explicit CIDR', () => {
    const err = captureValidationError('::1', { reject_ip_ranges: ['::1/128'] });
    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2203);
  });

  test('rejects a bare IPv6 literal when reject_ipv6 is set', () => {
    const err = captureValidationError('::1', { reject_ip_ranges: [], reject_ipv6: true });
    expect(err?.toJSON().code).toEqual(ErrorCode.PSYNC_S2202);
  });

  test('treats unknown hostnames as DNS names', () => {
    expect(captureValidationError('example.com', { reject_ip_ranges: ['local'] })).toBeUndefined();
  });

  test('treats a port-suffixed input as a DNS name (precondition: caller normalizes first)', () => {
    expect(captureValidationError('127.0.0.1:5432', { reject_ip_ranges: ['local'] })).toBeUndefined();
  });
});

describe('hostnameFromSocketAddress', () => {
  test('returns a bare IPv4 unchanged', () => {
    expect(hostnameFromSocketAddress('127.0.0.1')).toEqual('127.0.0.1');
  });

  test('strips an explicit IPv4 port', () => {
    expect(hostnameFromSocketAddress('127.0.0.1:27017')).toEqual('127.0.0.1');
  });

  test('strips an empty IPv4 port', () => {
    expect(hostnameFromSocketAddress('127.0.0.1:')).toEqual('127.0.0.1');
  });

  test('strips a zero IPv4 port', () => {
    expect(hostnameFromSocketAddress('127.0.0.1:0')).toEqual('127.0.0.1');
  });

  test('unwraps a bracketed IPv6 literal', () => {
    expect(hostnameFromSocketAddress('[::1]')).toEqual('::1');
  });

  test('unwraps a bracketed IPv6 literal with a port', () => {
    expect(hostnameFromSocketAddress('[::1]:27017')).toEqual('::1');
  });

  test('unwraps a bracketed IPv6 literal with an empty port', () => {
    expect(hostnameFromSocketAddress('[::1]:')).toEqual('::1');
  });

  test('returns a bare IPv6 literal unchanged', () => {
    expect(hostnameFromSocketAddress('::1')).toEqual('::1');
    expect(hostnameFromSocketAddress('2001:db8::1')).toEqual('2001:db8::1');
  });

  test('returns DNS names unchanged', () => {
    expect(hostnameFromSocketAddress('example.com')).toEqual('example.com');
  });

  test('returns DNS-with-port unchanged (not an IP socket address)', () => {
    expect(hostnameFromSocketAddress('example.com:8080')).toEqual('example.com:8080');
  });

  test('returns IPv4 with a non-numeric port unchanged', () => {
    expect(hostnameFromSocketAddress('127.0.0.1:abc')).toEqual('127.0.0.1:abc');
  });
});
