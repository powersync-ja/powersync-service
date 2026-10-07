import { ErrorCode, logger, ServiceError } from '@powersync/lib-services-framework';
import { MetricsEngine, storage } from '@powersync/service-core';
import { DEFAULT_HYDRATION_STATE, nodeSqlite, SqlSyncRules } from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { afterEach, expect, test, vi } from 'vitest';
import { ChangeStream } from '../../src/replication/ChangeStream.js';
import { MongoManager } from '../../src/replication/MongoManager.js';
import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '../../src/replication/MongoReplicationQueryProvider.js';
import { MongoSnapshotter } from '../../src/replication/MongoSnapshotter.js';
import { normalizeConnectionConfig } from '../../src/types/types.js';

const CONFIG = /* yaml */ `
  # Sync config fixture.
  config:
    edition: 3
    source_table_options:
      orders%:
        mongodb_filter_expression: { $eq: ['$$doc.active', true] }
      orders:
        mongodb_filter_expression: disabled
  streams:
    orders:
      query: SELECT * FROM orders
`;
const RAW_CONNECTION = { type: 'mongodb' as const, uri: 'mongodb://localhost/app' };
const CONNECTION = { ...RAW_CONNECTION, ...normalizeConnectionConfig(RAW_CONNECTION) };
afterEach(() => vi.restoreAllMocks());

test.each([
  { filtered: true, factory: false, blocked: true },
  { filtered: true, factory: true, blocked: false },
  { filtered: false, factory: false, blocked: false }
])('checks provider availability: %j', async ({ filtered, factory, blocked }) => {
  const yaml = filtered ? CONFIG : CONFIG.replace("{ $eq: ['$$doc.active', true] }", 'disabled');
  const parsed = SqlSyncRules.fromYaml(yaml, { defaultSchema: 'app' });
  const hydrated = parsed.config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
  const reportError = vi.fn(async (_error: unknown) => {});
  // Stop allowed cases at the first snapshot check, without connecting to a database.
  const checkSlot = vi
    .spyOn(MongoSnapshotter.prototype, 'checkSlot')
    .mockRejectedValue(new Error('snapshot check reached'));
  const source = new MongoManager(CONNECTION);
  const createProvider = vi.fn(() => DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER);
  try {
    const stream = new ChangeStream({
      connections: source,
      abort_signal: new AbortController().signal,
      metrics: {} as MetricsEngine,
      createReplicationQueryProvider: factory ? createProvider : undefined,
      storage: {
        replicationStreamId: 1,
        replicationStreamName: 'availability',
        getParsedSyncRules: () => hydrated,
        reportError,
        logger
      } as unknown as storage.SyncRulesBucketStorage
    });
    await expect(stream.replicate()).rejects.toThrow(
      blocked ? 'no query provider factory is registered' : 'snapshot check reached'
    );
    if (blocked) {
      expect(checkSlot).not.toHaveBeenCalled();
      const error = reportError.mock.calls[0]?.[0];
      expect(error).toBeInstanceOf(ServiceError);
      expect(error).toMatchObject({ errorData: { code: ErrorCode.PSYNC_S1348 } });
      expect(reportError).toHaveBeenCalledWith(
        expect.objectContaining({ message: expect.stringContaining('deploy a new sync config') })
      );
    } else {
      expect(checkSlot).toHaveBeenCalledOnce();
    }
    if (factory) expect(createProvider).toHaveBeenCalledWith(expect.objectContaining({ syncConfig: hydrated }));
  } finally {
    await source.client.close();
  }
});
