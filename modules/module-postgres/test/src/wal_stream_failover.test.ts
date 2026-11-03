import { WalStream } from '@module/replication/WalStream.js';
import semver from 'semver';
import { describe, expect, test } from 'vitest';

describe('WalStream failover slot support', () => {
  const createMockWalStream = (version: string | null, slotFailover?: boolean) => {
    return new WalStream({
      storage: {
        getParsedSyncRules: () => ({ getSourceTables: () => [] }),
        replicationStreamId: 1,
        replicationStreamName: 'powersync_test_slot'
      } as any,
      connections: {
        getServerVersion: async () => (version ? semver.parse(version) : null)
      } as any,
      metrics: {} as any,
      abort_signal: new AbortController().signal,
      slotFailover
    });
  };

  test('enables failover by default on Postgres 17.0.0', async () => {
    const stream = createMockWalStream('17.0.0');
    expect(await stream.checkFailoverSlotSupport()).toBe(true);
  });

  test('enables failover by default on Postgres 17.2.0', async () => {
    const stream = createMockWalStream('17.2.0');
    expect(await stream.checkFailoverSlotSupport()).toBe(true);
  });

  test('enables failover when slotFailover is explicitly true on Postgres 17+', async () => {
    const stream = createMockWalStream('17.0.0', true);
    expect(await stream.checkFailoverSlotSupport()).toBe(true);
  });

  test('disables failover when slotFailover is explicitly false on Postgres 17+', async () => {
    const stream = createMockWalStream('17.0.0', false);
    expect(await stream.checkFailoverSlotSupport()).toBe(false);
  });

  test('disables failover on Postgres 16.4.0', async () => {
    const stream = createMockWalStream('16.4.0');
    expect(await stream.checkFailoverSlotSupport()).toBe(false);
  });

  test('disables failover on Postgres 15.8.0 even if slotFailover is true', async () => {
    const stream = createMockWalStream('15.8.0', true);
    expect(await stream.checkFailoverSlotSupport()).toBe(false);
  });

  test('disables failover when server version is null', async () => {
    const stream = createMockWalStream(null);
    expect(await stream.checkFailoverSlotSupport()).toBe(false);
  });
});
