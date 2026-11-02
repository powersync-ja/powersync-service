import { BasicRouterRequest, Context, RouterEngine, storage, updateSyncRulesFromYaml } from '@/index.js';
import { diagnostics, reprocess } from '@/routes/endpoints/admin.js';
import { reprocessSyncRules } from '@/routes/endpoints/sync-rules.js';
import { SyncConfigEngine } from '@/system/SyncConfigEngine.js';
import { describe, expect, it, vi } from 'vitest';
import { mockServiceContext } from './mocks.js';

const VALID_CONFIG = /* yaml */ `
  # Stored config.
  config:
    edition: 3
  streams:
    todos:
      query: SELECT id FROM todos
`;
const INVALID_CONFIG = /* yaml */ `
  # Local rejected config.
  config:
    edition: 3
  streams:
    broken:
      query: SELECT FROM
`;
const REQUEST = { headers: {}, hostname: '', protocol: 'http' } as BasicRouterRequest;

function makeStoredConfig() {
  return {
    content: {
      replicationStreamName: 'stored_slot',
      version_label: 'stored',
      sync_rules_content: VALID_CONFIG,
      parsed: () => ({ syncConfigs: [updateSyncRulesFromYaml(VALID_CONFIG).config.parsed] }),
      getSyncConfigStatus: async () => null
    },
    storage: { getStatus: vi.fn(async () => ({ snapshotDone: true })) }
  } as unknown as storage.ResolvedSyncConfig;
}

async function makeContext(content: string, active = true, connected = true) {
  const service = mockServiceContext(null);
  service.configuration = {
    sync_rules: { present: true, content, version_label: 'local', exit_on_error: false }
  } as typeof service.configuration;
  const persisted = makeStoredConfig();
  (service.storageEngine as any).activeBucketStorage = {
    getActiveSyncConfig: async () => (active ? persisted : null),
    getDeployingSyncConfig: async () => persisted
  };
  const api = service.routerEngine.getAPI();
  service.routerEngine = new RouterEngine();
  service.routerEngine.registerAPI(api);
  service.syncConfigEngine = new SyncConfigEngine(service.configuration.sync_rules, () =>
    service.routerEngine.getAPI()
  );
  vi.spyOn(api, 'getConnectionStatus').mockResolvedValue({ connected, errors: [], id: 'test_id', uri: 'test' });
  return { context: { service_context: service } as Context, api, persisted };
}

describe('local invalid config diagnostics', () => {
  it.each([{ connected: true }, { connected: false }])(
    'overlays rejected config and preserves active diagnostics (connected=$connected)',
    async ({ connected }) => {
      const { context } = await makeContext(INVALID_CONFIG, true, connected);
      const result = await diagnostics.handler({ context, request: REQUEST, params: { sync_rules_content: true } });
      // Rejected configured YAML appears under deploying_sync_rules without hiding the active deployment.
      expect(result.active_sync_rules?.content).toBe(VALID_CONFIG);
      expect(result.active_sync_rules?.connections[0].slot_name).toBe('stored_slot');
      expect(result.deploying_sync_rules?.content).toBe(INVALID_CONFIG);
      expect(result.deploying_sync_rules?.version_label).toBe('local');
      expect(result.deploying_sync_rules?.connections).toEqual([]);
      expect(result.deploying_sync_rules?.errors.some((error) => error.level == 'fatal')).toBe(true);
    }
  );

  it('reports a rejected first config without content when not requested', async () => {
    const { context } = await makeContext(INVALID_CONFIG, false);
    const result = await diagnostics.handler({ context, request: REQUEST, params: {} });
    expect(result.active_sync_rules).toBeUndefined();
    expect(result.deploying_sync_rules?.content).toBeUndefined();
    expect(result.deploying_sync_rules?.errors.some((error) => error.level == 'fatal')).toBe(true);
  });

  it('reports local errors even when no source connection is configured', async () => {
    const { context, api } = await makeContext(INVALID_CONFIG, false);
    // Exercise the route's existing defensive fallback for a missing source adapter status.
    vi.mocked(api.getConnectionStatus).mockResolvedValue(
      undefined as unknown as Awaited<ReturnType<typeof api.getConnectionStatus>>
    );
    const result = await diagnostics.handler({ context, request: REQUEST, params: {} });
    expect(result.connections).toEqual([]);
    expect(result.deploying_sync_rules?.errors.some((error) => error.level == 'fatal')).toBe(true);
  });

  it('keeps stored pending diagnostics when the local config is valid', async () => {
    const { context } = await makeContext(VALID_CONFIG);
    const result = await diagnostics.handler({ context, request: REQUEST, params: { sync_rules_content: true } });
    expect(result.deploying_sync_rules?.content).toBe(VALID_CONFIG);
    expect(result.deploying_sync_rules?.connections[0].slot_name).toBe('stored_slot');
  });

  it.each([{ connected: true }, { connected: false }])(
    'reports rejected source capabilities on an API instance (connected=$connected)',
    async ({ connected }) => {
      const content = /* yaml */ `
        # Rejected local MongoDB deployment.
        config:
          edition: 3
          source_table_options:
            orders:
              mongodb_filter_expression: { $eq: ['$$doc.active', true] }
        streams:
          orders:
            query: SELECT * FROM orders
      `;
      const { context, api } = await makeContext(content, true, connected);
      // Only the API is running: no replicator has validated or persisted the local config.
      const fatal = {
        level: 'fatal' as const,
        message: 'MongoDB pre-filtering is not supported',
        location: { start_offset: 1, end_offset: 10 }
      };
      api.validateSourceCapabilities = vi.fn(async (config) => {
        return config.content === content ? [fatal] : [];
      });
      const result = await diagnostics.handler({ context, request: REQUEST, params: { sync_rules_content: true } });
      expect(result.active_sync_rules?.content).toBe(VALID_CONFIG);
      expect(result.deploying_sync_rules).toMatchObject({
        content,
        version_label: 'local',
        connections: [],
        errors: expect.arrayContaining([expect.objectContaining(fatal)])
      });
    }
  );

  it('reports unexpected source capability failures without persisted status or requested content', async () => {
    const { context, api } = await makeContext(VALID_CONFIG, false);
    api.validateSourceCapabilities = vi.fn(async () => {
      throw new Error('Capability validation failed');
    });
    const result = await diagnostics.handler({ context, request: REQUEST, params: {} });
    expect(result.deploying_sync_rules?.content).toBeUndefined();
    expect(result.deploying_sync_rules?.connections).toEqual([]);
    expect(result.deploying_sync_rules?.errors).toEqual([
      expect.objectContaining({ level: 'fatal', message: 'Capability validation failed' })
    ]);
  });

  it('keeps stored pending diagnostics when local source capabilities only report warnings', async () => {
    const { context, api } = await makeContext(VALID_CONFIG);
    api.validateSourceCapabilities = vi.fn(async () => [{ level: 'warning' as const, message: 'Advisory warning' }]);
    const result = await diagnostics.handler({ context, request: REQUEST, params: { sync_rules_content: true } });
    expect(result.deploying_sync_rules?.connections[0].slot_name).toBe('stored_slot');
  });
});

describe('manual reprocessing with rejected local config', () => {
  it.each(
    [reprocess, reprocessSyncRules].flatMap((route) =>
      [false, true].map((capabilityFailure) => ({ route, capabilityFailure }))
    )
  )(
    'blocks $route.path for local fatal errors (capability=$capabilityFailure)',
    async ({ route, capabilityFailure }) => {
      const { context, api } = await makeContext(capabilityFailure ? VALID_CONFIG : INVALID_CONFIG);
      const backend = context.service_context.storageEngine.activeBucketStorage;
      // Ensure no pending deployment exists, so invalid configured YAML is what blocks reprocessing.
      vi.spyOn(backend, 'getDeployingSyncConfig').mockResolvedValue(null);
      if (capabilityFailure) {
        api.validateSourceCapabilities = async () => [{ level: 'fatal', message: 'Unsupported executor' }];
      }
      const update = vi.fn();
      backend.updateSyncRules = update;
      await expect(route.handler({ context, request: REQUEST, params: {} })).rejects.toMatchObject({
        errorData: {
          status: 409,
          code: 'PSYNC_S4106',
          description: 'Cannot reprocess while the configured sync config is invalid'
        }
      });
      expect(update).not.toHaveBeenCalled();
    }
  );
});
