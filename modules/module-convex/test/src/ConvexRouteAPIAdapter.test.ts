import { SqlSyncRules } from '@powersync/service-sync-rules';
import { describe, expect, it, vi } from 'vitest';
import { ConvexRouteAPIAdapter } from '@module/api/ConvexRouteAPIAdapter.js';
import { ConvexLSN } from '@module/common/ConvexLSN.js';
import { normalizeConnectionConfig } from '@module/types/types.js';

function createAdapter() {
  const config = normalizeConnectionConfig({
    type: 'convex',
    deployment_url: 'https://example.convex.cloud',
    deploy_key: 'test-key'
  });

  const adapter = new ConvexRouteAPIAdapter({
    ...config,
    type: 'convex',
    deployment_url: 'https://example.convex.cloud',
    deploy_key: 'test-key'
  });

  (adapter as any).connectionManager.client = {
    getJsonSchemas: async () => ({
      tables: [
        {
          tableName: 'users',
          schema: {
            properties: {
              _id: { type: 'string' },
              age: { type: 'integer' }
            }
          }
        }
      ],
      raw: {}
    }),
    getHeadCursor: async () => '123',
    createWriteCheckpointMarker: async () => undefined
  };

  return adapter;
}

describe('ConvexRouteAPIAdapter', () => {
  it('returns connection schema from Convex json schema', async () => {
    const adapter = createAdapter();

    const schema = await adapter.getConnectionSchema();
    expect(schema[0]?.name).toBe('convex');
    expect(schema[0]?.tables[0]?.name).toBe('users');

    await adapter.shutdown();
  });

  it('builds debug table info for matching patterns', async () => {
    const adapter = createAdapter();

    const syncRules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  test:
    data:
      - SELECT _id AS id FROM users
`,
      {
        defaultSchema: 'convex'
      }
    );

    const result = await adapter.getDebugTablesInfo(syncRules.getSourceTables(), syncRules);
    expect(result[0]?.table?.name).toBe('users');

    await adapter.shutdown();
  });

  it('creates replication head from the global snapshot cursor', async () => {
    const adapter = createAdapter();
    const getHeadCursor = vi.fn(async (_options?: any) => '123');
    const createWriteCheckpointMarker = vi.fn(async (_options?: any) => undefined);
    (adapter as any).connectionManager.client.getHeadCursor = getHeadCursor;
    (adapter as any).connectionManager.client.createWriteCheckpointMarker = createWriteCheckpointMarker;

    const result = await adapter.createReplicationHead(async (head) => head);
    expect(result).toBe(ConvexLSN.fromCursor('123').comparable);
    expect(getHeadCursor).toHaveBeenCalledTimes(1);
    expect(getHeadCursor).toHaveBeenCalledWith();
    expect(createWriteCheckpointMarker).toHaveBeenCalledTimes(1);
    expect(createWriteCheckpointMarker).toHaveBeenCalledWith();

    await adapter.shutdown();
  });
});
