import * as types from '@module/types/types.js';
import { createPool, isVersionAtLeast, TCP_KEEPALIVE_INITIAL_DELAY } from '@module/utils/mysql-utils.js';
import { describe, expect, test } from 'vitest';

describe('MySQL Utility Tests', () => {
  test('Minimum version checking ', () => {
    const newerVersion = '8.4.0';
    const olderVersion = '5.7';
    const sameVersion = '8.0';
    // Improperly formatted semantic versions should be handled gracefully if possible
    const improperSemver = '5.7.42-0ubuntu0.18.04.1-log';

    expect(isVersionAtLeast(newerVersion, '8.0')).toBeTruthy();
    expect(isVersionAtLeast(sameVersion, '8.0')).toBeTruthy();
    expect(isVersionAtLeast(olderVersion, '8.0')).toBeFalsy();
    expect(isVersionAtLeast(improperSemver, '5.7')).toBeTruthy();
  });

  test('Pool connections are configured with a TCP keepalive initial delay', async () => {
    // mysql2 enables keepalive by default, but without an initial delay the OS default of
    // 7200 seconds applies, which is too late for common 3600 second firewall idle timeouts.
    const config = types.normalizeConnectionConfig({
      type: 'mysql',
      uri: 'mysql://root:password@localhost:3306/mydatabase'
    });
    // The pool is lazy, so no connection is made here.
    const pool = createPool(config);
    const { connectionConfig } = (pool as unknown as { config: { connectionConfig: Record<string, unknown> } }).config;

    expect(connectionConfig.enableKeepAlive).toBe(true);
    expect(connectionConfig.keepAliveInitialDelay).toBe(TCP_KEEPALIVE_INITIAL_DELAY);

    await pool.promise().end();
  });
});
