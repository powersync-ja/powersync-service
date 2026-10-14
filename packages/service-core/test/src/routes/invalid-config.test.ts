import { getInvalidLocalSyncConfigStatus } from '@/api/diagnostics.js';
import { BasicRouterRequest, Context, RouterEngine, storage, updateSyncRulesFromYaml } from '@/index.js';
import { diagnostics } from '@/routes/endpoints/admin.js';
import { loadSyncConfig } from '@/system/LoadedSyncConfig.js';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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
  service.loadedSyncConfig = await loadSyncConfig(service.configuration.sync_rules);
  vi.spyOn(api, 'getConnectionStatus').mockResolvedValue({ connected, errors: [], id: 'test_id', uri: 'test' });
  return { context: { service_context: service } as Context, api, persisted };
}

describe('local invalid config diagnostics', () => {
  it.each([INVALID_CONFIG, 'streams: ['])('reports fatal errors without persisted status', async (content) => {
    const loaded = await loadSyncConfig({ present: true, content, exit_on_error: false, version_label: 'local' });
    const result = getInvalidLocalSyncConfigStatus(loaded, true);
    expect(result?.content).toBe(content);
    expect(result?.version_label).toBe('local');
    expect(result?.connections).toEqual([]);
    expect(result?.errors).toEqual(expect.arrayContaining([expect.objectContaining({ level: 'fatal' })]));
  });

  it.each([true, false])(
    'overlays rejected config and preserves active diagnostics (connected=%s)',
    async (connected) => {
      const { context } = await makeContext(INVALID_CONFIG, true, connected);
      const result = await diagnostics.handler({ context, request: REQUEST, params: { sync_rules_content: true } });
      expect(result.active_sync_rules?.content).toBe(VALID_CONFIG);
      expect(result.active_sync_rules?.connections[0].slot_name).toBe('stored_slot');
      expect(result.deploying_sync_rules?.content).toBe(INVALID_CONFIG);
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

  it.each([VALID_CONFIG, INVALID_CONFIG])(
    'retains the initialized snapshot until a new context loads the same configuration',
    async (initial) => {
      const dir = await mkdtemp(join(tmpdir(), 'invalid-config-diagnostics-'));
      try {
        const path = join(dir, 'sync-config.yaml');
        const config = { present: true, path, exit_on_error: false };
        await writeFile(path, initial);
        const loaded = await loadSyncConfig(config);
        const [withoutContent, withContent] = await Promise.all([
          getInvalidLocalSyncConfigStatus(loaded, false),
          getInvalidLocalSyncConfigStatus(loaded, true)
        ]);
        expect(withoutContent?.content).toBeUndefined();
        expect(withContent?.content).toBe(initial == INVALID_CONFIG ? INVALID_CONFIG : undefined);
        const replacement = initial == VALID_CONFIG ? INVALID_CONFIG : VALID_CONFIG;
        await writeFile(path, replacement);
        const repeated = getInvalidLocalSyncConfigStatus(loaded, true);
        expect(repeated?.content).toBe(withContent?.content);
        expect(repeated?.errors.map(({ ts, ...error }) => error)).toEqual(
          withContent?.errors.map(({ ts, ...error }) => error)
        );
        expect(getInvalidLocalSyncConfigStatus(await loadSyncConfig(config), true)?.content).toBe(
          replacement == INVALID_CONFIG ? INVALID_CONFIG : undefined
        );
      } finally {
        await rm(dir, { recursive: true, force: true });
      }
    }
  );

  it('caches an unavailable file until restart', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'invalid-config-diagnostics-'));
    try {
      const path = join(dir, 'sync-config.yaml');
      const config = { present: true, path, exit_on_error: false };
      const loaded = await loadSyncConfig(config);
      expect(loaded.mode).toBe('unavailable');
      expect(getInvalidLocalSyncConfigStatus(loaded, true)).toBeUndefined();
      await writeFile(path, INVALID_CONFIG);
      expect(getInvalidLocalSyncConfigStatus(loaded, true)).toBeUndefined();
      expect(getInvalidLocalSyncConfigStatus(await loadSyncConfig(config), true)?.content).toBe(INVALID_CONFIG);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
