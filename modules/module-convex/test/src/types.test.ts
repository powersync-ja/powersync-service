import { describe, expect, it } from 'vitest';
import { CONVEX_CONNECTION_TYPE, normalizeConnectionConfig } from '@module/types/types.js';

describe('Convex connection config', () => {
  it('normalizes defaults', () => {
    const config = normalizeConnectionConfig({
      type: CONVEX_CONNECTION_TYPE,
      deployment_url: 'https://example.convex.cloud',
      deploy_key: 'secret-key'
    });

    expect(config.id).toBe('default');
    expect(config.tag).toBe('default');
    expect(config.pollingIntervalMs).toBe(1000);
    expect(config.requestTimeoutMs).toBe(30000);
    expect(config.deploymentUrl).toBe('https://example.convex.cloud');
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
});
