import { ConvexApiClient } from '@module/client/ConvexApiClient.js';
import { ConvexListSnapshotResult, RawJsonSchemaResponse } from '@module/client/ConvexAPITypes.js';
import { CONVEX_CHECKPOINT_TABLE } from '@module/common/ConvexCheckpoints.js';
import { normalizeConnectionConfig } from '@module/types/types.js';
import { JSONBig } from '@powersync/service-jsonbig';
import { afterEach, describe, expect, it, vi } from 'vitest';

const baseConfig = normalizeConnectionConfig({
  type: 'convex',
  deployment_url: 'https://example.convex.cloud',
  deploy_key: 'test-key'
});
const SNAPSHOT_CURSOR = 1770335566197683000n;

describe('ConvexApiClient', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('sends Convex authorization header and format=json', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          users: { type: 'table', properties: { _id: { type: 'string' } } }
        } satisfies RawJsonSchemaResponse),
        { status: 200 }
      )
    );

    const client = new ConvexApiClient(baseConfig);
    const result = await client.getJsonSchemas();

    expect(result.tables.map((table) => table.tableName)).toEqual(['users']);

    const [url, init] = fetchSpy.mock.calls[0]!;
    expect(String(url)).toContain('/api/json_schemas');
    expect(String(url)).toContain('format=json');
    expect((init?.headers as Record<string, string>).Authorization).toBe('Convex test-key');
  });

  it('preserves high-precision numeric snapshot values', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        '{"values":[],"snapshot":1770335566197682922,"cursor":"{\\"tablet\\":\\"X0yj4Cm7GfuikfsSBm9QCQ\\",\\"id\\":\\"j5700000000000000000000000001qv0\\"}","hasMore":true}',
        { status: 200 }
      )
    );

    const client = new ConvexApiClient(baseConfig);
    const page = await client.listSnapshot({ tableName: 'lists' });

    expect(page.snapshot).toBe(1770335566197682922n);
    expect(page.cursor).toContain('"tablet":"X0yj4Cm7GfuikfsSBm9QCQ"');
    expect(page.hasMore).toBe(true);
  });

  it('sends table_name as snake_case query parameter in list_snapshot', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSONBig.stringify({
          snapshot: SNAPSHOT_CURSOR,
          cursor: null,
          hasMore: false,
          values: []
        } satisfies ConvexListSnapshotResult),
        { status: 200 }
      )
    );

    const client = new ConvexApiClient(baseConfig);
    await client.listSnapshot({ tableName: 'lists', snapshot: SNAPSHOT_CURSOR.toString() });

    const url = String(fetchSpy.mock.calls[0]![0]);
    expect(url).toContain('table_name=lists');
    expect(url).not.toContain('tableName=lists');
  });

  it('marks network failures as retryable', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('fetch failed: ECONNRESET'));

    const client = new ConvexApiClient(baseConfig);

    await expect(client.getJsonSchemas()).rejects.toMatchObject({
      retryable: true
    });
  });

  it('creates write checkpoint markers via mutation', async () => {
    const fetchSpy = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(JSON.stringify({ status: 'success' }), { status: 200 }));

    const client = new ConvexApiClient(baseConfig);
    await client.createWriteCheckpointMarker();

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0]!;
    expect(String(url)).toContain('/api/mutation');
    expect(init?.method).toBe('POST');
    expect((init?.headers as Record<string, string>).Authorization).toBe('Convex test-key');

    const body = JSON.parse(String(init?.body));
    expect(body.path).toBe(`${CONVEX_CHECKPOINT_TABLE}:createCheckpoint`);
    expect(body.args).toEqual({});
    expect(body.format).toBe('json');
  });

  it('propagates checkpoint write errors directly (no fallback)', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
      new Response(JSON.stringify({ code: 'SomeError' }), { status: 400 })
    );

    const client = new ConvexApiClient(baseConfig);
    await expect(client.createWriteCheckpointMarker()).rejects.toMatchObject({
      status: 400,
      retryable: false
    });
  });

  it('checks the hostname policy before creating checkpoint markers', async () => {
    const fetchSpy = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(JSON.stringify({ status: 'success' }), { status: 200 }));
    const lookup = vi.fn((_hostname: string, _options: any, callback: (error: Error) => void) => {
      callback(new Error('blocked by reject_ip_ranges'));
    }) as unknown as import('node:net').LookupFunction;

    const client = new ConvexApiClient({
      ...baseConfig,
      lookup
    });

    await expect(client.createWriteCheckpointMarker()).rejects.toThrow('blocked by reject_ip_ranges');
    expect(lookup).toHaveBeenCalledTimes(1);
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});
