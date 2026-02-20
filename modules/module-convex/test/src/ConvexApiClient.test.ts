import { afterEach, describe, expect, it, vi } from 'vitest';
import { ConvexApiClient } from '@module/client/ConvexApiClient.js';
import { CONVEX_CHECKPOINT_TABLE } from '@module/common/ConvexCheckpoints.js';
import { normalizeConnectionConfig } from '@module/types/types.js';

const baseConfig = normalizeConnectionConfig({
  type: 'convex',
  deployment_url: 'https://example.convex.cloud',
  deploy_key: 'test-key',
  request_timeout_ms: 5000
});

describe('ConvexApiClient', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('sends Convex authorization header and format=json', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          tables: {
            users: { properties: { _id: { type: 'string' } } }
          }
        }),
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

  it('falls back to /api/streaming_export path on 404', async () => {
    const fetchSpy = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response('{}', { status: 404 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            snapshot: '100',
            cursor: '100',
            has_more: false,
            values: []
          }),
          { status: 200 }
        )
      );

    const client = new ConvexApiClient(baseConfig);
    const page = await client.listSnapshot({ tableName: 'users' });

    expect(page.snapshot).toBe('100');
    expect(fetchSpy.mock.calls.length).toBe(2);
    expect(String(fetchSpy.mock.calls[1]![0])).toContain('/api/streaming_export/list_snapshot');
  });

  it('reuses requested snapshot when response omits snapshot', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          cursor: 'next-page',
          has_more: true,
          values: []
        }),
        { status: 200 }
      )
    );

    const client = new ConvexApiClient(baseConfig);
    const page = await client.listSnapshot({
      snapshot: '1770335566197683',
      tableName: 'lists'
    });

    expect(page.snapshot).toBe('1770335566197683');
    expect(page.cursor).toBe('next-page');
  });

  it('fails when first list_snapshot page omits snapshot', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          cursor: 'next-page',
          has_more: true,
          values: []
        }),
        { status: 200 }
      )
    );

    const client = new ConvexApiClient(baseConfig);
    await expect(client.listSnapshot({ tableName: 'lists' })).rejects.toMatchObject({
      message: expect.stringContaining('missing snapshot'),
      retryable: false
    });
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

    expect(page.snapshot).toBe('1770335566197682922');
    expect(page.cursor).toContain('"tablet":"X0yj4Cm7GfuikfsSBm9QCQ"');
    expect(page.hasMore).toBe(true);
  });

  it('parses self-hosted top-level json_schemas table map', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          todos: {
            type: 'object',
            properties: {
              _id: { type: 'string' }
            }
          },
          lists: {
            type: 'object',
            properties: {
              _id: { type: 'string' },
              name: { type: 'string' }
            }
          }
        }),
        { status: 200 }
      )
    );

    const client = new ConvexApiClient(baseConfig);
    const result = await client.getJsonSchemas();

    expect(result.tables.map((table) => table.tableName)).toEqual(['lists', 'todos']);
  });

  it('sends table_name as snake_case query parameter in list_snapshot', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          snapshot: '100',
          cursor: null,
          has_more: false,
          values: []
        }),
        { status: 200 }
      )
    );

    const client = new ConvexApiClient(baseConfig);
    await client.listSnapshot({ tableName: 'lists', snapshot: '100' });

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
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ status: 'success' }), { status: 200 })
    );

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
      new Response(
        JSON.stringify({ code: 'SomeError' }),
        { status: 400 }
      )
    );

    const client = new ConvexApiClient(baseConfig);
    await expect(client.createWriteCheckpointMarker()).rejects.toMatchObject({
      status: 400,
      retryable: false
    });
  });
});
