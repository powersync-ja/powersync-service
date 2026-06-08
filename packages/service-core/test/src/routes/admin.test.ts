import { BasicRouterRequest, Context, JwtPayload, ParsedSyncConfigSet, storage } from '@/index.js';
import { logger } from '@powersync/lib-services-framework';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, it, vi } from 'vitest';
import { diagnostics, reprocess, validate } from '../../../src/routes/endpoints/admin.js';
import { mockServiceContext } from './mocks.js';

describe('admin routes', () => {
  const request: BasicRouterRequest = {
    headers: {},
    hostname: '',
    protocol: 'http'
  };

  function makeContext(activeBucketStorage?: any): Context {
    const service_context = mockServiceContext(null);
    if (activeBucketStorage != null) {
      (service_context.storageEngine as any).activeBucketStorage = activeBucketStorage;
    }

    return {
      logger: logger,
      service_context,
      token_payload: new JwtPayload({
        sub: '',
        exp: 0,
        iat: 0
      })
    };
  }

  function makeSyncConfigContent(options: {
    id?: number;
    syncConfigId?: string;
    active?: boolean;
    content?: string;
  }): storage.PersistedSyncConfigContent {
    const id = options.id ?? 1;
    const syncConfigId = options.syncConfigId ?? String(id);
    const active = options.active ?? true;
    const state = active ? storage.SyncRuleState.ACTIVE : storage.SyncRuleState.PROCESSING;
    const lastKeepaliveTs = new Date('2026-01-01T00:00:00.000Z');
    const lastCheckpointTs = new Date('2026-01-01T00:00:00.000Z');
    const content = {
      replicationStreamId: id,
      syncConfigId,
      replicationStreamName: `slot_${id}`,
      sync_rules_content:
        options.content ??
        `
bucket_definitions:
  global:
    data:
      - SELECT id FROM test
`,
      compiled_plan: null,
      active,
      state,
      storageVersion: storage.LEGACY_STORAGE_VERSION,
      last_checkpoint_lsn: null,
      last_fatal_error: null,
      last_fatal_error_ts: null,
      last_keepalive_ts: lastKeepaliveTs,
      last_checkpoint_ts: lastCheckpointTs,
      parsed(options?: any) {
        const syncRules = SqlSyncRules.fromYaml(content.sync_rules_content, {
          ...options,
          defaultSchema: 'public'
        });
        return {
          syncConfigs: [syncRules]
        } as ParsedSyncConfigSet;
      },
      asUpdateOptions: vi.fn(),
      getStorageConfig: vi.fn(),
      getSyncConfigStatus() {
        return {
          id: syncConfigId,
          replicationStreamId: id,
          state,
          last_checkpoint_lsn: null,
          last_fatal_error: null,
          last_fatal_error_ts: null,
          last_keepalive_ts: lastKeepaliveTs,
          last_checkpoint_ts: lastCheckpointTs
        };
      }
    };
    return content as unknown as storage.PersistedSyncConfigContent;
  }

  describe('validate', () => {
    it('reports errors with source location', async () => {
      const context = makeContext();

      const response = await validate.handler({
        context,
        params: {
          sync_rules: `
bucket_definitions:
  missing_table:
    data:
      - SELECT * FROM missing_table
`
        },
        request
      });

      expect(response.errors).toEqual([
        expect.objectContaining({
          level: 'warning',
          location: { start_offset: 70, end_offset: 83 },
          message: 'Table public.missing_table not found'
        })
      ]);
    });
  });

  describe('diagnostics', () => {
    it('returns deploying config status', async () => {
      const active = makeSyncConfigContent({ id: 1, syncConfigId: 'active-config' });
      const deploying = makeSyncConfigContent({ id: 2, syncConfigId: 'deploying-config', active: false });
      const getInstance = vi.fn(() => ({
        async getStatus() {
          return {
            active: true,
            snapshot_done: false,
            checkpoint_lsn: null
          };
        }
      }));
      const activeBucketStorage = {
        getActiveSyncConfigContent: vi.fn(async () => active),
        getActiveSyncConfigStatus: vi.fn(async () => active.getSyncConfigStatus()),
        getActiveStorage: vi.fn(async () => getInstance()),
        getDeployingSyncConfigContent: vi.fn(async () => deploying),
        getReplicationStream: vi.fn(async (id: number) => ({ id, slot_name: `slot_${id}` })),
        getInstance
      };

      const response = await diagnostics.handler({
        context: makeContext(activeBucketStorage),
        params: {},
        request
      });

      expect(response.deploying_sync_rules?.connections[0].slot_name).toBe('slot_2');
      expect(response.active_sync_rules?.connections[0].slot_name).toBe('slot_1');
    });
  });

  describe('reprocess', () => {
    it('reprocesses the active sync config', async () => {
      const active = makeSyncConfigContent({ id: 7, syncConfigId: 'active-config' });
      const updateSyncRules = vi.fn(async () => ({
        replicationStreamId: 8,
        replicationStreamName: 'new_slot',
        state: storage.SyncRuleState.PROCESSING,
        storageVersion: storage.LEGACY_STORAGE_VERSION,
        replicationJobId: '8',
        current_lock: null
      }));
      const activeBucketStorage = {
        getDeployingSyncConfigContent: vi.fn(async () => null),
        getActiveSyncConfigContent: vi.fn(async () => active),
        getSyncConfigContent: vi.fn(),
        updateSyncRules
      };

      const response = await reprocess.handler({
        context: makeContext(activeBucketStorage),
        params: {},
        request
      });

      expect(activeBucketStorage.getActiveSyncConfigContent).toHaveBeenCalledTimes(1);
      expect(activeBucketStorage.getSyncConfigContent).not.toHaveBeenCalled();
      expect(updateSyncRules).toHaveBeenCalledTimes(1);
      expect(response.connections[0].slot_name).toBe('new_slot');
    });

    it('rejects reprocess while a sync config is deploying', async () => {
      const activeBucketStorage = {
        getDeployingSyncConfigContent: vi.fn(async () => makeSyncConfigContent({ id: 2, active: false })),
        getActiveSyncConfigContent: vi.fn(),
        updateSyncRules: vi.fn()
      };

      await expect(
        reprocess.handler({
          context: makeContext(activeBucketStorage),
          params: {},
          request
        })
      ).rejects.toMatchObject({
        errorData: {
          status: 409,
          code: 'PSYNC_S4106',
          description: 'Busy processing sync config - cannot reprocess'
        }
      });
      expect(activeBucketStorage.getActiveSyncConfigContent).not.toHaveBeenCalled();
      expect(activeBucketStorage.updateSyncRules).not.toHaveBeenCalled();
    });
  });
});
