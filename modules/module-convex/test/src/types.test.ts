import { CONVEX_CONNECTION_TYPE, normalizeConnectionConfig } from '@module/types/types.js';
import { describe, expect, it } from 'vitest';

describe('Convex connection config', () => {
  it('normalizes defaults', () => {
    const config = normalizeConnectionConfig({
      type: CONVEX_CONNECTION_TYPE,
      deployment_url: 'https://example.convex.cloud',
      deploy_key: 'secret-key'
    });

    expect(config.id).toBe('default');
    expect(config.tag).toBe('default');
    expect(config.polling_interval_ms).toBe(1000);
    expect(config.request_timeout_ms).toBe(60_000);
    expect(config.deployment_url).toBe('https://example.convex.cloud');
  });

  it('normalizes custom request timeout', () => {
    const config = normalizeConnectionConfig({
      type: CONVEX_CONNECTION_TYPE,
      deployment_url: 'https://example.convex.cloud',
      deploy_key: 'secret-key',
      request_timeout_ms: 30_000
    });

    expect(config.request_timeout_ms).toBe(30_000);
  });

  it('throws for invalid URL', () => {
    expect(() =>
      normalizeConnectionConfig({
        type: CONVEX_CONNECTION_TYPE,
        deployment_url: 'not-a-url',
        deploy_key: 'secret-key'
      })
    ).toThrow();
  });

  it('throws for empty deploy key', () => {
    expect(() =>
      normalizeConnectionConfig({
        type: CONVEX_CONNECTION_TYPE,
        deployment_url: 'https://example.convex.cloud',
        deploy_key: ''
      })
    ).toThrow();
  });

  it.each([-1, 0, Number.NaN, Number.POSITIVE_INFINITY])('throws for invalid polling_interval_ms: %s', (value) => {
    expect(() =>
      normalizeConnectionConfig({
        type: CONVEX_CONNECTION_TYPE,
        deployment_url: 'https://example.convex.cloud',
        deploy_key: 'secret-key',
        polling_interval_ms: value
      })
    ).toThrow('polling_interval_ms must be a positive finite number');
  });

  it.each([-1, 0, Number.NaN, Number.POSITIVE_INFINITY])('throws for invalid request_timeout_ms: %s', (value) => {
    expect(() =>
      normalizeConnectionConfig({
        type: CONVEX_CONNECTION_TYPE,
        deployment_url: 'https://example.convex.cloud',
        deploy_key: 'secret-key',
        request_timeout_ms: value
      })
    ).toThrow('request_timeout_ms must be a positive finite number');
  });

  it('rejects a bracketed IPv6 deployment URL when local IPs are blocked', () => {
    expect(() =>
      normalizeConnectionConfig({
        type: CONVEX_CONNECTION_TYPE,
        deployment_url: 'http://[::1]:3210',
        deploy_key: 'secret-key',
        reject_ip_ranges: ['local']
      })
    ).toThrow('IPs in this range are not supported');
  });

  it('rejects a bracketed IPv6 deployment URL against an explicit CIDR', () => {
    expect(() =>
      normalizeConnectionConfig({
        type: CONVEX_CONNECTION_TYPE,
        deployment_url: 'http://[::1]:3210',
        deploy_key: 'secret-key',
        reject_ip_ranges: ['::1/128']
      })
    ).toThrow('IPs in this range are not supported');
  });
});
