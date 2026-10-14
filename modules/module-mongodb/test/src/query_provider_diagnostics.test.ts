import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/index.js';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { describe, expect, test } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { describeWithStorage } from './util.js';

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
      query: SELECT _id AS id FROM orders
`;

describe('MongoDB missing-provider diagnostics', () => {
  describeWithStorage({ timeout: 30_000 }, ({ factory, storageVersion }) => {
    test('persists a missing-provider error for an active config after the provider is removed', async () => {
      {
        await using context = await openChangeStreamTestContext(factory, {
          storageVersion,
          streamOptions: { createReplicationQueryProvider: () => DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER }
        });
        // The concrete collection opts out, but the wildcard still requires provider support.
        await context.updateSyncRules(CONFIG);
        await context.db.createCollection('orders');
        await context.replicateSnapshot();
        await context.getCheckpoint();
      }
      {
        await using context = await openChangeStreamTestContext(factory, { storageVersion, doNotClear: true });
        await context.loadActiveSyncRules();
        const result = await context.startStreaming();
        expect(result.status).toBe('rejected');
        if (result.status != 'rejected') {
          throw new Error('Expected missing-provider rejection');
        }
        expect(result.reason).toBeInstanceOf(ServiceError);
        expect(result.reason).toMatchObject({ errorData: { code: ErrorCode.PSYNC_S1348 } });
      }
      // Reopen storage to prove diagnostics were persisted, rather than only reported in memory.
      await using reopened = await openChangeStreamTestContext(factory, { storageVersion, doNotClear: true });
      const active = await reopened.factory.getActiveSyncConfig();
      expect(active).not.toBeNull();
      const status = await active!.content.getSyncConfigStatus();
      expect(status?.last_fatal_error).toContain('no query provider factory is registered');
      expect(status?.last_fatal_error).toContain('deploy a new sync config');
    });
  });
});
