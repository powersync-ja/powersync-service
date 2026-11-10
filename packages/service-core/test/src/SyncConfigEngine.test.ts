import type { RouteAPI } from '@/api/RouteAPI.js';
import { ServiceContextContainer, ServiceContextMode } from '@/system/ServiceContext.js';
import { SyncConfigEngine, SyncConfigRejectionReason, SyncConfigResultStatus } from '@/system/SyncConfigEngine.js';
import { CompoundConfigCollector } from '@/util/config/compound-config-collector.js';
import * as syncConfigParser from '@powersync/service-sync-rules';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { mockServiceContext } from './routes/mocks.js';

const CONTENT = /* yaml */ `
  # sync config fixture.
  config:
    edition: 3
  streams:
    orders:
      query: SELECT * FROM orders
`;

function source(): RouteAPI {
  const adapter = mockServiceContext(null).routerEngine.getAPI();
  adapter.getParseSyncRulesOptions = () => ({
    defaultSchema: 'source_db',
    connections: { default: { type: 'mongodb' } }
  });
  adapter.validateSourceCapabilities = async () => [];
  return adapter;
}

describe('SyncConfigEngine', () => {
  it('constructs the shared engine before source adapter registration', async () => {
    const configuration = await new CompoundConfigCollector().collectConfig({
      config_base64: Buffer.from(
        JSON.stringify({ storage: { type: 'mongodb' }, sync_config: { content: CONTENT } })
      ).toString('base64')
    });
    const context = new ServiceContextContainer({ serviceMode: ServiceContextMode.API, configuration });
    const adapter = source();
    const parseOptions = vi.spyOn(adapter, 'getParseSyncRulesOptions');
    const engine = context.syncConfigEngine;
    // Source modules register after context construction; creating the engine must not resolve the adapter.
    context.routerEngine.registerAPI(adapter);
    expect(context.syncConfigEngine).toBe(engine);
    expect(parseOptions).not.toHaveBeenCalled();
    expect((await engine.getConfigured()).status).toBe(SyncConfigResultStatus.ACCEPTED);
    expect(parseOptions).toHaveBeenCalledOnce();
  });

  it('shares a single source-aware parse and capability result between concurrent consumers', async () => {
    const adapter = source();
    const capability = vi.spyOn(adapter, 'validateSourceCapabilities').mockImplementation(async (config) => {
      expect(config.getSourceTables()[0].schema).toBe('source_db');
      return [{ level: 'warning', message: 'Advisory' }];
    });
    const parse = vi.spyOn(syncConfigParser, 'parseSyncConfig');
    try {
      const settings = {
        present: true,
        exit_on_error: false,
        content: CONTENT
      };
      const service = new SyncConfigEngine(settings, () => adapter);
      // Startup and diagnostics can race on first use; both must share the same in-flight parse.
      const first = service.getConfigured();
      expect(service.getConfigured()).toBe(first);
      const [replication, diagnostics] = await Promise.all([first, service.getConfigured()]);
      expect(replication).toBe(diagnostics);
      expect(replication.status).toBe(SyncConfigResultStatus.ACCEPTED);
      expect(replication).toMatchObject({
        diagnostics: [expect.objectContaining({ level: 'warning', message: 'Advisory' })]
      });
      expect(capability).toHaveBeenCalledOnce();
      expect(parse).toHaveBeenCalledOnce();
      settings.content = 'streams: [';
      expect(await service.getConfigured()).toBe(replication);
    } finally {
      parse.mockRestore();
    }
  });

  it('retains capability errors and locations in the common rejection decision', async () => {
    const adapter = source();
    vi.spyOn(adapter, 'validateSourceCapabilities').mockResolvedValue([
      { level: 'fatal', message: 'Unsupported executor', location: { start_offset: 0, end_offset: 5 } }
    ]);
    const result = await new SyncConfigEngine({ present: false, exit_on_error: true }, () => adapter).validate(CONTENT);
    expect(result).toMatchObject({
      status: SyncConfigResultStatus.REJECTED,
      reason: SyncConfigRejectionReason.CAPABILITY,
      diagnostics: [expect.objectContaining({ level: 'fatal', location: { start_offset: 0, end_offset: 5 } })]
    });
    expect(result).not.toHaveProperty('compiled');
  });

  it.each([
    { content: 'streams: [', hasParsedConfig: true },
    { content: '# empty config\n', hasParsedConfig: false }
  ])(
    'checks capabilities for malformed input whenever a config was produced (parsed=$hasParsedConfig)',
    async ({ content, hasParsedConfig }) => {
      const adapter = source();
      const capability = vi.spyOn(adapter, 'validateSourceCapabilities');
      const result = await new SyncConfigEngine({ present: false, exit_on_error: false }, () => adapter).validate(
        content
      );
      expect(result).toMatchObject({
        status: SyncConfigResultStatus.REJECTED,
        reason: SyncConfigRejectionReason.PARSE
      });
      expect(result.diagnostics.some((diagnostic) => diagnostic.level === 'fatal')).toBe(true);
      // Partial parses still need capability diagnostics; a parser exception leaves no config to check.
      expect(capability).toHaveBeenCalledTimes(hasParsedConfig ? 1 : 0);
    }
  );

  it.each([{ capabilityCheckThrows: false }, { capabilityCheckThrows: true }])(
    'collects source-capability failures alongside fatal parser diagnostics (throws=$capabilityCheckThrows)',
    async ({ capabilityCheckThrows }) => {
      const adapter = source();
      const capability = vi.spyOn(adapter, 'validateSourceCapabilities').mockImplementation(async () => {
        if (capabilityCheckThrows) {
          throw new Error('Unsupported executor');
        }
        return [{ level: 'fatal', message: 'Unsupported executor' }];
      });
      const result = await new SyncConfigEngine({ present: false, exit_on_error: false }, () => adapter).validate(
        CONTENT.replace('SELECT * FROM orders', 'SELECT FROM')
      );
      expect(capability).toHaveBeenCalledOnce();
      expect(result).toMatchObject({
        status: SyncConfigResultStatus.REJECTED,
        reason: SyncConfigRejectionReason.PARSE
      });
      // Keep both failures for diagnostics, with parsing as the primary rejection reason.
      const fatal = result.diagnostics.filter((diagnostic) => diagnostic.level === 'fatal');
      expect(fatal.some((diagnostic) => diagnostic.message === 'Unsupported executor')).toBe(true);
      expect(fatal.some((diagnostic) => diagnostic.message !== 'Unsupported executor')).toBe(true);
      expect(result).not.toHaveProperty('compiled');
    }
  );

  it('validates submissions without changing the API-managed snapshot', async () => {
    const adapter = source();
    const parseOptions = vi.spyOn(adapter, 'getParseSyncRulesOptions');
    const getRouteAPI = vi.fn(() => adapter);
    const service = new SyncConfigEngine({ present: false, exit_on_error: false }, getRouteAPI);
    const configured = await service.getConfigured();
    expect(getRouteAPI).not.toHaveBeenCalled();
    expect(parseOptions).not.toHaveBeenCalled();
    expect(configured.status).toBe(SyncConfigResultStatus.ABSENT);
    // An API submission must not become this runner's configured snapshot.
    expect((await service.validate('streams: [')).status).toBe(SyncConfigResultStatus.REJECTED);
    expect((await service.validate(CONTENT)).status).toBe(SyncConfigResultStatus.ACCEPTED);
    expect(await service.getConfigured()).toBe(configured);
  });

  it.each([
    { inputKind: 'valid', initial: CONTENT },
    { inputKind: 'invalid query', initial: CONTENT.replace('SELECT * FROM orders', 'SELECT FROM') }
  ])('retains the first filesystem result until a new engine is created: $inputKind', async ({ initial }) => {
    const dir = await mkdtemp(join(tmpdir(), 'sync-config-snapshot-'));
    try {
      const path = join(dir, 'sync-config.yaml');
      const settings = { present: true, path, exit_on_error: false };
      const adapter = source();
      await writeFile(path, initial);
      const engine = new SyncConfigEngine(settings, () => adapter);
      const first = await engine.getConfigured();
      expect(first.status).toBe(
        initial === CONTENT ? SyncConfigResultStatus.ACCEPTED : SyncConfigResultStatus.REJECTED
      );
      const replacement = initial === CONTENT ? CONTENT.replace('SELECT * FROM orders', 'SELECT FROM') : CONTENT;
      // File edits take effect after restart, represented here by constructing a new engine.
      await writeFile(path, replacement);
      expect(await engine.getConfigured()).toBe(first);
      expect(await new SyncConfigEngine(settings, () => adapter).getConfigured()).toMatchObject({
        content: replacement,
        status: replacement === CONTENT ? SyncConfigResultStatus.ACCEPTED : SyncConfigResultStatus.REJECTED
      });
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('retains an unavailable file result until a new engine is created', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'sync-config-unavailable-'));
    try {
      const settings = { present: true, path: join(dir, 'sync-config.yaml'), exit_on_error: false };
      const adapter = source();
      const parseOptions = vi.spyOn(adapter, 'getParseSyncRulesOptions');
      const getRouteAPI = vi.fn(() => adapter);
      const engine = new SyncConfigEngine(settings, getRouteAPI);
      const first = await engine.getConfigured();
      expect(first).toMatchObject({ status: SyncConfigResultStatus.UNAVAILABLE, error: expect.any(Error) });
      // A file appearing later must not silently switch the running process to another snapshot.
      await writeFile(settings.path, CONTENT);
      expect(await engine.getConfigured()).toBe(first);
      expect(getRouteAPI).not.toHaveBeenCalled();
      expect(parseOptions).not.toHaveBeenCalled();
      expect((await new SyncConfigEngine(settings, () => adapter).getConfigured()).status).toBe(
        SyncConfigResultStatus.ACCEPTED
      );
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
