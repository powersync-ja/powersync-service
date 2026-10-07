import { BasicRouterRequest, Context, JwtPayload, ParsedSyncConfigSet, storage } from '@/index.js';
import { logger } from '@powersync/lib-services-framework';
import { PrecompiledSyncConfig, SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, it, vi } from 'vitest';
import { diagnostics, reprocess, validate } from '../../../src/routes/endpoints/admin.js';
import { deploySyncRules, reprocessSyncRules, validateSyncRules } from '../../../src/routes/endpoints/sync-rules.js';
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
    version_label?: string;
  }): storage.PersistedSyncConfigContent {
    const id = options.id ?? 1;
    const syncConfigId = options.syncConfigId ?? String(id);
    const active = options.active ?? true;
    const state = active ? storage.SyncRuleState.ACTIVE : storage.SyncRuleState.PROCESSING;
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

  it.each([false, true])(
    'continues sync-config diagnostics after capability failure (table failure: %s)',
    async (tableFailure) => {
      const context = makeContext();
      const api = context.service_context.routerEngine.getAPI();
      vi.spyOn(context.service_context.routerEngine, 'getAPI').mockReturnValue(api);
      api.validateSourceCapabilities = async () => {
        throw new Error('Unsupported capability');
      };
      const tables = vi.spyOn(api, 'getDebugTablesInfo').mockImplementation(async () => {
        if (tableFailure) throw new Error('Table inspection failed');
        return [{ schema: 'public', pattern: 'items', wildcard: false, tables: [] }];
      });
      const response = await validateSyncRules.handler({
        context,
        request,
        params: {
          content: /* yaml */ `
            # Sync config fixture.
            config:
              edition: 3
            streams:
              items:
                query: SELECT * FROM items
          `
        }
      });
      const body = JSON.parse(response.data);
      expect(body.valid).toBe(false);
      expect(body.errors).toEqual(
        tableFailure ? ['Unsupported capability', 'Table inspection failed'] : ['Unsupported capability']
      );
      expect(tables).toHaveBeenCalledOnce();
      expect(body.source_tables).toHaveLength(tableFailure ? 0 : 1);
      expect(body.data_tables).toBeDefined();
    }
  );

  it('keeps advisory capability diagnostics visible without invalidating the config', async () => {
    const context = makeContext();
    const api = context.service_context.routerEngine.getAPI();
    vi.spyOn(context.service_context.routerEngine, 'getAPI').mockReturnValue(api);
    api.validateSourceCapabilities = async () => [{ level: 'warning', message: 'Source advisory' }];
    const response = await validateSyncRules.handler({
      context,
      request,
      params: {
        content: /* yaml */ `
          # Sync config fixture.
          config:
            edition: 3
          streams:
            items:
              query: SELECT * FROM items
        `
      }
    });
    expect(JSON.parse(response.data)).toMatchObject({ valid: true, warnings: ['Source advisory'] });
  });

  it.each([true, false])('deployment checks source capabilities before persistence (allowed: %s)', async (allowed) => {
    const updateSyncRules = vi.fn(async () => ({ replicationStreamName: 'new-slot' }));
    const context = makeContext({ updateSyncRules });
    context.service_context.configuration = {
      sync_rules: { present: false }
    } as typeof context.service_context.configuration;
    const api = context.service_context.routerEngine.getAPI();
    vi.spyOn(context.service_context.routerEngine, 'getAPI').mockReturnValue(api);
    api.validateSourceCapabilities = async (config) => {
      expect((config as PrecompiledSyncConfig).plan.sourceTableConfig?.orders?.mongodb_filter_expression).toEqual({
        $eq: ['$$doc.active', true]
      });
      return allowed
        ? [{ level: 'warning', message: 'Advisory source warning' }]
        : [{ level: 'fatal', message: 'MongoDB pre-filtering is not available for this connection.' }];
    };
    const result = deploySyncRules.handler({
      context,
      request,
      params: {
        content: /* yaml */ `
          # Sync config fixture.
          config:
            edition: 3
            source_table_options:
              orders:
                mongodb_filter_expression: { $eq: ['$$doc.active', true] }
          streams:
            orders:
              query: SELECT * FROM orders
        `
      }
    });
    if (allowed) {
      await expect(result).resolves.toEqual({ slot_name: 'new-slot' });
      expect(updateSyncRules).toHaveBeenCalledOnce();
    } else {
      await expect(result).rejects.toThrow();
      expect(updateSyncRules).not.toHaveBeenCalled();
    }
  });

  describe('validate', () => {
    it('uses the service parser and includes its diagnostics', async () => {
      const context = makeContext();
      const api = context.service_context.routerEngine.getAPI();
      vi.spyOn(context.service_context.routerEngine, 'getAPI').mockReturnValue(api);
      api.validateSourceCapabilities = async () => {
        throw new Error('Rejected by module validation.');
      };
      const response = await validate.handler({
        context,
        request,
        params: {
          sync_rules: /* yaml */ `
            # Sync config fixture.
            config:
              edition: 3
            streams: {}
          `
        }
      });
      expect(response.errors).toContainEqual(
        expect.objectContaining({
          level: 'fatal',
          message: 'Rejected by module validation.'
        })
      );
    });

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
    it('returns deploying config status', async () => {
      const active = makeSyncConfigContent({ id: 1, syncConfigId: 'active-config', version_label: 'v5' });
      const deploying = makeSyncConfigContent({ id: 2, syncConfigId: 'deploying-config', active: false });
      const getInstance = vi.fn(() => ({
        async getStatus() {
          return {
            snapshotDone: false,
            resumeLsn: null
          };
        }
      }));
      const activeBucketStorage = {
        getActiveSyncConfig: vi.fn(async () => ({
          content: active,
          replicationStream: {},
          storage: getInstance()
        })),
        getDeployingSyncConfig: vi.fn(async () => ({
          content: deploying,
          replicationStream: {},
          storage: getInstance()
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
    });
  });

  describe('reprocess', () => {
    it.each([reprocess, reprocessSyncRules])('blocks reprocessing on returned fatal diagnostics', async (route) => {
      const active = makeSyncConfigContent({ id: 7 });
      const updateSyncRules = vi.fn();
      const context = makeContext({
        getDeployingSyncConfig: vi.fn(async () => null),
        getActiveSyncConfig: vi.fn(async () => ({ content: active })),
        updateSyncRules
      });
      const api = context.service_context.routerEngine.getAPI();
      vi.spyOn(context.service_context.routerEngine, 'getAPI').mockReturnValue(api);
      api.validateSourceCapabilities = async () => [
        { level: 'warning', message: 'Advisory' },
        { level: 'fatal', message: 'Unsupported source' }
      ];
      await expect(route.handler({ context, params: {}, request })).rejects.toThrow('Unsupported source');
      expect(updateSyncRules).not.toHaveBeenCalled();
    });

    it('reprocesses the active sync config', async () => {
      const active = makeSyncConfigContent({ id: 7, syncConfigId: 'active-config', version_label: 'v6' });
      const updateSyncRules = vi.fn(async () => ({
        replicationStreamId: 8,
        replicationStreamName: 'new_slot',
        state: storage.SyncRuleState.PROCESSING,
        storageVersion: storage.LEGACY_STORAGE_VERSION,
        replicationJobId: '8',
        current_lock: null
      }));
      const activeBucketStorage = {
        getDeployingSyncConfig: vi.fn(async () => null),
        getActiveSyncConfig: vi.fn(async () => ({
          content: active,
          replicationStream: {},
          storage: {}
        })),
        getSyncConfigContent: vi.fn(),
        updateSyncRules
      };

      const response = await reprocess.handler({
        context: makeContext(activeBucketStorage),
        params: {},
        request
      });

      expect(activeBucketStorage.getActiveSyncConfig).toHaveBeenCalledTimes(1);
      expect(activeBucketStorage.getSyncConfigContent).not.toHaveBeenCalled();
      expect(updateSyncRules).toHaveBeenCalledTimes(1);
      expect(updateSyncRules).toHaveBeenCalledWith(
        expect.objectContaining({ version_label: 'v6', forceNewReplicationStream: true })
      );
      expect(response.connections[0].slot_name).toBe('new_slot');
    });

    it('rejects reprocess while a sync config is deploying', async () => {
      const activeBucketStorage = {
        getDeployingSyncConfig: vi.fn(async () => ({
          content: makeSyncConfigContent({ id: 2, active: false }),
          replicationStream: {},
          storage: {}
        })),
        getActiveSyncConfig: vi.fn(),
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
      expect(activeBucketStorage.getActiveSyncConfig).not.toHaveBeenCalled();
      expect(activeBucketStorage.updateSyncRules).not.toHaveBeenCalled();
    });

    it('logs that bucket_definitions are deprecated', async () => {
      const warnSpy = vi.spyOn(logger, 'warn');
      const activeBucketStorage = {
        getDeployingSyncConfig: vi.fn(async () => null),
        getActiveSyncConfig: vi.fn(async () => ({
          content: makeSyncConfigContent({}),
          replicationStream: {},
          storage: {}
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
