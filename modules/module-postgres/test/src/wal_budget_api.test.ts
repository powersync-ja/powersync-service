import { PostgresRouteAPIAdapter } from '@module/api/PostgresRouteAPIAdapter.js';
import { describe, expect, test } from 'vitest';
import { describeWithStorage, StorageVersionTestContext } from './util.js';
import { WalStreamTestContext, withMaxWalSize } from './wal_stream_utils.js';

describe('getSlotWalBudget', () => {
  describeWithStorage({ timeout: 20_000 }, defineWalBudgetTests);
});

function defineWalBudgetTests({ factory, storageVersion }: StorageVersionTestContext) {
  const openContext = (options?: Parameters<typeof WalStreamTestContext.open>[1]) => {
    return WalStreamTestContext.open(factory, { ...options, storageVersion });
  };

  test('returns WAL budget with configured limit', async () => {
    await using context = await openContext();
    const { pool } = context;

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"`);

    await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

    // Start replication to create the slot
    await context.replicateSnapshot();

    const serverVersion = await context.connectionManager.getServerVersion();
    if (serverVersion!.compareMain('13.0.0') < 0) {
      console.warn(`wal_status not supported on postgres ${serverVersion} - skipping test.`);
      return;
    }

    await using _walSize = await withMaxWalSize(pool, '1GB');

    const adapter = new PostgresRouteAPIAdapter(pool);
    const budget = await adapter.getSlotWalBudget({
      slotName: context.storage!.slot_name
    });

    expect(budget).toBeDefined();
    expect(budget!.wal_status).toBeTypeOf('string');
    expect(budget!.safe_wal_size).toBeTypeOf('number');
    expect(budget!.max_slot_wal_keep_size).toBeTypeOf('number');
    expect(budget!.max_slot_wal_keep_size).toBeGreaterThan(0);
  });

  test('returns undefined max when unlimited', async () => {
    await using context = await openContext();
    const { pool } = context;

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"`);

    await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

    await context.replicateSnapshot();

    const serverVersion = await context.connectionManager.getServerVersion();
    if (serverVersion!.compareMain('13.0.0') < 0) {
      console.warn(`wal_status not supported on postgres ${serverVersion} - skipping test.`);
      return;
    }

    // Default is -1 (unlimited) — ensure it's set
    await using _walSize = await withMaxWalSize(pool, '-1');

    const adapter = new PostgresRouteAPIAdapter(pool);
    const budget = await adapter.getSlotWalBudget({
      slotName: context.storage!.slot_name
    });

    expect(budget).toBeDefined();
    expect(budget!.wal_status).toBeTypeOf('string');
    expect(budget!.max_slot_wal_keep_size).toBeUndefined();
    // safe_wal_size is null when no limit is configured
    expect(budget!.safe_wal_size).toBeUndefined();
  });

  test('returns undefined when slot is missing', async () => {
    await using context = await openContext();
    const { pool } = context;

    const adapter = new PostgresRouteAPIAdapter(pool);
    const budget = await adapter.getSlotWalBudget({
      slotName: 'nonexistent_slot'
    });

    expect(budget).toBeUndefined();
  });
}
