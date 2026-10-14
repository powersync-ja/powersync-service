import { BasicRouterRequest, Context, JwtPayload, ParsedSyncConfigSet, storage } from '@/index.js';
import { logger } from '@powersync/lib-services-framework';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, it, vi } from 'vitest';
import { diagnostics, reprocess, validate } from '../../../src/routes/endpoints/admin.js';
import { reprocessSyncRules } from '../../../src/routes/endpoints/sync-rules.js';
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
    state?: storage.SyncRuleState;
    content?: string;
    version_label?: string;
  }): storage.PersistedSyncConfigContent {
    const id = options.id ?? 1;
    const syncConfigId = options.syncConfigId ?? String(id);
    const active = options.active ?? true;
    const state = options.state ?? (active ? storage.SyncRuleState.ACTIVE : storage.SyncRuleState.PROCESSING);
    const lastKeepaliveTs = new Date('2026-01-01T00:00:00.000Z');
    const lastCheckpointTs = new Date('2026-01-01T00:00:00.000Z');
    const syncConfigStatus = {
      id: syncConfigId,
      replicationStreamId: id,
      state,
      last_checkpoint_lsn: null,
      last_fatal_error: null,
      last_fatal_error_ts: null,
      last_keepalive_ts: lastKeepaliveTs,
      last_checkpoint_ts: lastCheckpointTs
    };
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
      storageVersion: storage.LEGACY_STORAGE_VERSION,
      syncConfigState: syncConfigStatus.state,
      version_label: options.version_label,
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
      async getSyncConfigStatus() {
        return syncConfigStatus;
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

      expect(response.errors).toContainEqual(
        expect.objectContaining({
          level: 'warning',
          location: { start_offset: 70, end_offset: 83 },
          message: 'Table public.missing_table not found'
        })
      );
    });

    it('warns that bucket_definitions are deprecated', async () => {
      const context = makeContext();

      const response = await validate.handler({
        context,
        params: {
          sync_rules: `
bucket_definitions:
  mybucket:
    data:
      - SELECT * FROM missing_table
`
        },
        request
      });

      expect(response.errors).toContainEqual(
        expect.objectContaining({
          level: 'warning',
          message: expect.stringContaining('Sync Rules (`bucket_definitions`) are deprecated')
        })
      );
    });

    it('does not report the deprecation warning for a Sync Streams config', async () => {
      const context = makeContext();

      const response = await validate.handler({
        context,
        params: {
          sync_rules: `
config:
  edition: 3
streams:
  mystream:
    query: SELECT * FROM missing_table
`
        },
        request
      });

      expect(response.errors).not.toContainEqual(
        expect.objectContaining({ message: expect.stringContaining('are deprecated') })
      );
    });
  });

  describe('diagnostics', () => {
    it.each([storage.SyncRuleState.PROCESSING, storage.SyncRuleState.INVALID])(
      'returns %s config status as the latest deployment attempt',
      async (state) => {
        const active = makeSyncConfigContent({ id: 1, syncConfigId: 'active-config', version_label: 'v5' });
        const deploying = makeSyncConfigContent({ id: 2, syncConfigId: 'deploying-config', state });
        const makeStorage = () => ({
          getStatus: vi.fn(async () => ({
            snapshotDone: false,
            resumeLsn: null
          }))
        });
        const nextStorage = makeStorage();
        const activeBucketStorage = {
          getSyncConfigStatus: vi.fn(async () => ({
            active: { content: active, replicationStream: {}, storage: makeStorage() },
            next: { content: deploying, replicationStream: {}, storage: nextStorage }
          }))
        };

        const response = await diagnostics.handler({
          context: makeContext(activeBucketStorage),
          params: {},
          request
        });

        expect(response.deploying_sync_rules?.connections[0].slot_name).toBe('slot_2');
        expect(response.active_sync_rules?.connections[0].slot_name).toBe('slot_1');
        expect(response.active_sync_rules?.version_label).toBe('v5');
        // An invalid config never replicates, so its live replication state is not read.
        expect(nextStorage.getStatus).toHaveBeenCalledTimes(state == storage.SyncRuleState.INVALID ? 0 : 1);
      }
    );
  });

  describe.each([
    { name: 'admin reprocess', route: reprocess, response: { connections: [{ slot_name: 'new_slot' }] } },
    { name: 'sync config reprocess', route: reprocessSyncRules, response: { slot_name: 'new_slot' } }
  ])('$name', ({ route, response: expectedResponse }) => {
    it('reprocesses the active sync config when no deployment is pending', async () => {
      const active = makeSyncConfigContent({
        id: 7,
        syncConfigId: 'active-config',
        version_label: 'v6',
        content: /* yaml */ `
          # Active config to reprocess.
          config:
            edition: 3
          streams:
            todos:
              query: SELECT * FROM todos
        `
      });
      const updateSyncRules = vi.fn(async () => ({
        replicationStreamId: 8,
        replicationStreamName: 'new_slot',
        state: storage.SyncRuleState.PROCESSING,
        storageVersion: storage.LEGACY_STORAGE_VERSION,
        replicationJobId: '8',
        current_lock: null
      }));
      const activeBucketStorage = {
        getSyncConfigStatus: vi.fn(async () => ({
          active: { content: active, replicationStream: {}, storage: {} },
          next: null
        })),
        updateSyncRules
      };

      const response = await route.handler({
        context: makeContext(activeBucketStorage),
        params: {},
        request
      });

      expect(activeBucketStorage.getSyncConfigStatus).toHaveBeenCalledTimes(1);
      expect(updateSyncRules).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ version_label: 'v6', forceNewReplicationStream: true })
      );
      expect(response).toMatchObject(expectedResponse);
    });

    it.each([
      {
        state: storage.SyncRuleState.PROCESSING,
        description: 'Busy processing sync config - cannot reprocess'
      },
      {
        state: storage.SyncRuleState.INVALID,
        description:
          'Cannot reprocess while the latest sync config deployment is invalid - deploy a new sync config first'
      }
    ])('rejects reprocess without changing the pending $state deployment', async ({ state, description }) => {
      const activeBucketStorage = {
        getSyncConfigStatus: vi.fn(async () => ({
          active: { content: makeSyncConfigContent({ id: 1 }), replicationStream: {}, storage: {} },
          next: { content: makeSyncConfigContent({ id: 2, state }), replicationStream: {}, storage: {} }
        })),
        updateSyncRules: vi.fn()
      };

      await expect(
        route.handler({ context: makeContext(activeBucketStorage), params: {}, request })
      ).rejects.toMatchObject({
        errorData: { status: 409, code: 'PSYNC_S4106', description }
      });
      expect(activeBucketStorage.updateSyncRules).not.toHaveBeenCalled();
    });

    it('rejects reprocess when no active sync config exists', async () => {
      const activeBucketStorage = {
        getSyncConfigStatus: vi.fn(async () => ({ active: null, next: null })),
        updateSyncRules: vi.fn()
      };
      await expect(
        route.handler({ context: makeContext(activeBucketStorage), params: {}, request })
      ).rejects.toMatchObject({
        errorData: { status: 422, code: 'PSYNC_S4104', description: 'No active sync config' }
      });
      expect(activeBucketStorage.updateSyncRules).not.toHaveBeenCalled();
    });

    it('logs that bucket_definitions are deprecated', async () => {
      const warnSpy = vi.spyOn(logger, 'warn');
      const activeBucketStorage = {
        getSyncConfigStatus: vi.fn(async () => ({
          active: { content: makeSyncConfigContent({}), replicationStream: {}, storage: {} },
          next: null
        })),
        updateSyncRules: vi.fn(async () => ({ replicationStreamName: 'new_slot' }))
      };

      await reprocess.handler({
        context: makeContext(activeBucketStorage),
        params: {},
        request
      });

      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('Sync Rules (`bucket_definitions`) are deprecated'));
      warnSpy.mockRestore();
    });
  });
});
