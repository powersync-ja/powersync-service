import {
  AbstractReplicationStream,
  AbstractReplicationStreamOptions
} from '@/replication/AbstractReplicationStream.js';
import { updateSyncRulesFromConfig } from '@/storage/BucketStorageFactory.js';
import { parsePersistedSyncConfigContent } from '@/storage/PersistedSyncConfigContent.js';
import type { SyncRulesBucketStorage } from '@/storage/SyncRulesBucketStorage.js';
import { ErrorCode } from '@powersync/lib-services-framework';
import { SqlSyncRules, SyncConfig, SyncConfigWithErrors, YamlError } from '@powersync/service-sync-rules';
import { describe, expect, test, vi } from 'vitest';

const CONFIG = /* yaml */ `
  # Sync config fixture.
  config:
    edition: 3
  streams:
    orders:
      query: SELECT * FROM orders
`;

class TestStream extends AbstractReplicationStream {
  constructor(
    options: AbstractReplicationStreamOptions,
    private readonly run: () => Promise<void>
  ) {
    super(options, { defaultSchema: 'public' });
  }

  protected doReplicate(): Promise<void> {
    return this.run();
  }
}

function createStream(
  syncConfigs: SyncConfigWithErrors[],
  assertion?: AbstractReplicationStreamOptions['assertSourceCapabilities']
) {
  const getParsedSyncConfigSet = vi.fn(() => ({ syncConfigs }));
  const reportError = vi.fn(async (_error: unknown) => {});
  const run = vi.fn(async () => {});
  const stream = new TestStream(
    {
      storage: { getParsedSyncConfigSet, reportError } as unknown as SyncRulesBucketStorage,
      assertSourceCapabilities: assertion
    },
    run
  );
  return { stream, run, reportError, getParsedSyncConfigSet };
}

function parsedConfig(): SyncConfigWithErrors {
  return SqlSyncRules.fromYaml(CONFIG, { defaultSchema: 'public' });
}

describe('replication stream validation', () => {
  test('checks every persisted config before source replication', async () => {
    const configs = [parsedConfig(), parsedConfig()];
    const calls: string[] = [];
    const assertion = vi.fn(async (_config: SyncConfig) => {
      calls.push('validate');
    });
    const { stream, run, reportError, getParsedSyncConfigSet } = createStream(configs, assertion);
    run.mockImplementation(async () => {
      calls.push('replicate');
    });
    await stream.replicate();
    expect(calls).toEqual(['validate', 'validate', 'replicate']);
    expect(assertion.mock.calls.map(([config]) => config)).toEqual(configs.map(({ config }) => config));
    expect(getParsedSyncConfigSet).toHaveBeenCalledWith({ defaultSchema: 'public' });
    expect(reportError).not.toHaveBeenCalled();
  });

  test('blocks a partially compiled persisted plan even without a capability callback', async () => {
    const invalid = SqlSyncRules.fromYaml(CONFIG.replace('SELECT * FROM orders', 'SELECT FROM'), {
      defaultSchema: 'public',
      throwOnError: false
    });
    expect(invalid.errors.some((error) => error.type === 'fatal')).toBe(true);
    const update = updateSyncRulesFromConfig(invalid);
    const restored = parsePersistedSyncConfigContent({
      content: CONFIG,
      compiledPlan: update.config.plan,
      storageVersion: 1,
      parseOptions: { defaultSchema: 'public' }
    });
    const { stream, run, reportError } = createStream([parsedConfig(), restored]);
    await expect(stream.replicate()).rejects.toMatchObject({ errorData: { code: ErrorCode.PSYNC_R0001 } });
    expect(run).not.toHaveBeenCalled();
    expect(reportError).toHaveBeenCalledOnce();
    expect(reportError.mock.calls[0][0]).toMatchObject({
      message: expect.stringContaining(restored.errors[0].message)
    });
  });

  test('allows warnings without a capability callback', async () => {
    const parsed = parsedConfig();
    const warning = new YamlError(new Error('Advisory warning'));
    warning.type = 'warning';
    parsed.errors.push(warning);
    const { stream, run, reportError } = createStream([parsed]);
    await stream.replicate();
    expect(run).toHaveBeenCalledOnce();
    expect(reportError).not.toHaveBeenCalled();
  });

  test('reports capability failure and revalidates when replication is retried', async () => {
    const failure = new Error('Source capabilities unavailable');
    const assertion = vi.fn(async () => {}).mockRejectedValueOnce(failure);
    const { stream, run, reportError } = createStream([parsedConfig()], assertion);
    await expect(stream.replicate()).rejects.toBe(failure);
    expect(run).not.toHaveBeenCalled();
    expect(reportError).toHaveBeenCalledExactlyOnceWith(failure);
    await stream.replicate();
    expect(assertion).toHaveBeenCalledTimes(2);
    expect(run).toHaveBeenCalledOnce();
  });

  test('reports source replication failures once', async () => {
    const failure = new Error('Source connection lost');
    const { stream, run, reportError } = createStream([parsedConfig()]);
    run.mockRejectedValue(failure);
    await expect(stream.replicate()).rejects.toBe(failure);
    expect(reportError).toHaveBeenCalledExactlyOnceWith(failure);
  });
});
