import { ConvexApiClient } from '@module/client/ConvexApiClient.js';
import { ConvexListSnapshotResult, RawJsonSchemaResponse } from '@module/client/ConvexAPITypes.js';
import { CONVEX_CHECKPOINT_TABLE } from '@module/common/ConvexCheckpoints.js';
import { normalizeConnectionConfig } from '@module/types/types.js';
import { JSONBig } from '@powersync/service-jsonbig';
import nodeFetch from 'node-fetch';
import * as https from 'node:https';
import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('node-fetch', () => ({
  default: vi.fn()
}));

const baseConfig = normalizeConnectionConfig({
  type: 'convex',
  deployment_url: 'https://example.convex.cloud',
  deploy_key: 'test-key'
});
const SNAPSHOT_CURSOR = 1770335566197683000n;
const fetchMock = vi.mocked(nodeFetch);

describe('ConvexApiClient', () => {
  afterEach(() => {
    vi.useRealTimers();
    fetchMock.mockReset();
    vi.restoreAllMocks();
  });

  it('sends Convex authorization header and format=json', async () => {
    fetchMock.mockResolvedValue(
      new Response(
        JSON.stringify({
          users: { type: 'table', properties: { _id: { type: 'string' } } }
        } satisfies RawJsonSchemaResponse),
        { status: 200 }
      ) as any
    );

    const client = new ConvexApiClient(baseConfig);
    const result = await client.getJsonSchemas();

    expect(result.tables.map((table) => table.tableName)).toEqual(['users']);

    const [url, init] = fetchMock.mock.calls[0]!;
    expect(String(url)).toContain('/api/json_schemas');
    expect(String(url)).toContain('format=json');
    expect((init?.headers as Record<string, string>).Authorization).toBe('Convex test-key');
  });

  it('preserves high-precision numeric snapshot values', async () => {
    fetchMock.mockResolvedValue(
      new Response(
        '{"values":[],"snapshot":1770335566197682922,"cursor":"{\\"tablet\\":\\"X0yj4Cm7GfuikfsSBm9QCQ\\",\\"id\\":\\"j5700000000000000000000000001qv0\\"}","hasMore":true}',
        { status: 200 }
      ) as any
    );

    const client = new ConvexApiClient(baseConfig);
    const page = await client.listSnapshot({ tableName: 'lists' });

    expect(page.snapshot).toBe(1770335566197682922n);
    expect(page.cursor).toContain('"tablet":"X0yj4Cm7GfuikfsSBm9QCQ"');
    expect(page.hasMore).toBe(true);
  });

  it('sends table_name as snake_case query parameter in list_snapshot', async () => {
    fetchMock.mockResolvedValue(
      new Response(
        JSONBig.stringify({
          snapshot: SNAPSHOT_CURSOR,
          cursor: null,
          hasMore: false,
          values: []
        } satisfies ConvexListSnapshotResult),
        { status: 200 }
      ) as any
    );

    const client = new ConvexApiClient(baseConfig);
    await client.listSnapshot({ tableName: 'lists', snapshot: SNAPSHOT_CURSOR.toString() });

    const url = String(fetchMock.mock.calls[0]![0]);
    expect(url).toContain('table_name=lists');
    expect(url).not.toContain('tableName=lists');
  });

  it('marks network failures as retryable', async () => {
    fetchMock.mockRejectedValue(new Error('fetch failed: ECONNRESET'));

    const client = new ConvexApiClient(baseConfig);

    await expect(client.getJsonSchemas()).rejects.toMatchObject({
      retryable: true
    });
  });

  it('uses the configured request timeout', async () => {
    vi.useFakeTimers();
    fetchMock.mockImplementation(
      (_url, init) =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => {
            reject((init.signal as any).reason);
          });
        }) as any
    );

    const client = new ConvexApiClient({
      ...baseConfig,
      request_timeout_ms: 25
    });

    const request = expect(client.getJsonSchemas()).rejects.toMatchObject({
      message: expect.stringContaining('timed out after 25ms'),
      retryable: true
    });

    await vi.advanceTimersByTimeAsync(25);
    await request;
  });

  it('creates write checkpoint markers via mutation', async () => {
    fetchMock.mockResolvedValue(new Response(JSON.stringify({ status: 'success' }), { status: 200 }) as any);

    const client = new ConvexApiClient(baseConfig);
    await client.createWriteCheckpointMarker();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(String(url)).toContain('/api/mutation');
    expect(init?.method).toBe('POST');
    expect((init?.headers as Record<string, string>).Authorization).toBe('Convex test-key');

    const body = JSON.parse(String(init?.body));
    expect(body.path).toBe(`${CONVEX_CHECKPOINT_TABLE}:createCheckpoint`);
    expect(body.args).toEqual({});
    expect(body.format).toBe('json');
  });

  it('propagates checkpoint write errors directly (no fallback)', async () => {
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ code: 'SomeError' }), { status: 400 }) as any);

    const client = new ConvexApiClient(baseConfig);
    await expect(client.createWriteCheckpointMarker()).rejects.toMatchObject({
      status: 400,
      retryable: false
    });
  });

  it('uses an agent with the configured hostname policy for Convex API requests', async () => {
    fetchMock.mockResolvedValue(new Response(JSON.stringify({ status: 'success' }), { status: 200 }) as any);
    const lookup = vi.fn((_hostname: string, _options: any, callback: (error: Error) => void) => {
      callback(new Error('blocked by reject_ip_ranges'));
    }) as unknown as import('node:net').LookupFunction;

    const client = new ConvexApiClient({
      ...baseConfig,
      lookup
    });

    await client.createWriteCheckpointMarker();

    const init = fetchMock.mock.calls[0]![1] as RequestInit & { agent: https.Agent };
    expect(init.agent).toBeInstanceOf(https.Agent);
    expect(init.agent.options.lookup).toBe(lookup);
    expect(lookup).not.toHaveBeenCalled();

    await expect(
      new Promise<void>((resolve, reject) => {
        init.agent.options.lookup!('example.convex.cloud', {}, (error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      })
    ).rejects.toThrow('blocked by reject_ip_ranges');
  });
});
