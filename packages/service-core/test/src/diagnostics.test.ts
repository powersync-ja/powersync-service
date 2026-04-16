import { DiagnosticsOptions, getSyncRulesStatus } from '@/api/diagnostics.js';
import { RouteAPI, SlotWalBudgetInfo } from '@/api/RouteAPI.js';
import { BucketStorageFactory } from '@/index.js';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';

const GB = 1024 * 1024 * 1024;

const MINIMAL_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id FROM test_table
`;

function makeSyncRulesContent(overrides?: { slot_name?: string }) {
  return {
    id: 1,
    slot_name: overrides?.slot_name ?? 'test_slot',
    sync_rules_content: MINIMAL_SYNC_RULES,
    compiled_plan: null,
    active: true,
    storageVersion: 1,
    last_checkpoint_lsn: 'some_lsn',
    last_fatal_error: null,
    last_fatal_error_ts: null,
    last_keepalive_ts: new Date(),
    last_checkpoint_ts: new Date(),
    parsed(options?: any) {
      const syncRules = SqlSyncRules.fromYaml(MINIMAL_SYNC_RULES, {
        ...options,
        defaultSchema: 'public'
      });
      return {
        sync_rules: syncRules
      };
    },
    lock() {
      throw new Error('Not implemented in mock');
    },
    current_lock: undefined
  } as any;
}

function makeBucketStorage() {
  return {
    getInstance() {
      return {
        async getStatus() {
          return {
            snapshot_done: true,
            checkpoint_lsn: 'some_lsn',
            active: true
          };
        }
      };
    }
  } as unknown as BucketStorageFactory;
}

function makeRouteAPI(walBudget?: SlotWalBudgetInfo | undefined): RouteAPI {
  return {
    getParseSyncRulesOptions() {
      return { defaultSchema: 'public' };
    },
    async getSourceConfig() {
      return { tag: 'test', id: 'test', type: 'postgresql' };
    },
    async getConnectionStatus() {
      return { connected: true };
    },
    async getDebugTablesInfo() {
      return [];
    },
    async getReplicationLagBytes() {
      return 0;
    },
    ...(walBudget !== undefined
      ? {
          async getSlotWalBudget() {
            return walBudget;
          }
        }
      : {})
  } as unknown as RouteAPI;
}

const OPTIONS: DiagnosticsOptions = {
  live_status: true,
  check_connection: true,
  include_content: false
};

describe('getSyncRulesStatus WAL budget warnings', () => {
  test('warns when WAL budget is at 40%', async () => {
    const api = makeRouteAPI({
      wal_status: 'extended',
      safe_wal_size: 4 * GB,
      max_slot_wal_keep_size: 10 * GB
    });
    const result = await getSyncRulesStatus(makeBucketStorage(), api, makeSyncRulesContent(), OPTIONS);
    const walWarnings = result!.errors.filter((e) => e.message.includes('WAL budget'));
    expect(walWarnings).toHaveLength(1);
    expect(walWarnings[0].level).toBe('warning');
    expect(walWarnings[0].message).toContain('40%');
  });

  test('no warning when WAL budget is at 80%', async () => {
    const api = makeRouteAPI({
      wal_status: 'reserved',
      safe_wal_size: 8 * GB,
      max_slot_wal_keep_size: 10 * GB
    });
    const result = await getSyncRulesStatus(makeBucketStorage(), api, makeSyncRulesContent(), OPTIONS);
    const walWarnings = result!.errors.filter((e) => e.message.includes('WAL budget'));
    expect(walWarnings).toHaveLength(0);
  });

  test('clamps negative safe_wal_size to 0%', async () => {
    const api = makeRouteAPI({
      wal_status: 'unreserved',
      safe_wal_size: -2.4 * GB,
      max_slot_wal_keep_size: 1 * 1024 * 1024 // 1MB
    });
    const result = await getSyncRulesStatus(makeBucketStorage(), api, makeSyncRulesContent(), OPTIONS);
    const walWarnings = result!.errors.filter((e) => e.message.includes('WAL budget'));
    expect(walWarnings).toHaveLength(1);
    expect(walWarnings[0].message).toContain('0%');
    expect(walWarnings[0].message).not.toMatch(/-\d+%/);
  });

  test('no WAL budget error when slot status is lost', async () => {
    const api = makeRouteAPI({
      wal_status: 'lost'
    });
    const result = await getSyncRulesStatus(makeBucketStorage(), api, makeSyncRulesContent(), OPTIONS);
    const walErrors = result!.errors.filter(
      (e) => e.message.includes('WAL budget') || e.message.includes('PSYNC_S1146')
    );
    expect(walErrors).toHaveLength(0);
  });

  test('no WAL error when getSlotWalBudget is not defined', async () => {
    const api = makeRouteAPI();
    const result = await getSyncRulesStatus(makeBucketStorage(), api, makeSyncRulesContent(), OPTIONS);
    const walErrors = result!.errors.filter(
      (e) => e.message.includes('WAL budget') || e.message.includes('PSYNC_S1146')
    );
    expect(walErrors).toHaveLength(0);
  });
});
