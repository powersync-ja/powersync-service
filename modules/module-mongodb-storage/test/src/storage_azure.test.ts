import { type BlobDownloadResponseParsed } from '@azure/storage-blob';
import { AzureBlobObjectStorage } from '@module/storage/implementation/v3/object-storage/AzureBlobObjectStorage.js';
import { BucketDataObjectStorage } from '@module/storage/implementation/v3/object-storage/BucketDataObjectStorage.js';
import { MongoStorageConfig } from '@module/types/types.js';
import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { afterEach, describe, expect, test, vi } from 'vitest';

const metadata = { contentType: 'application/bson', contentEncoding: null };
const connectionString = 'UseDevelopmentStorage=true';
const createStorage = (options: { concurrencyLimit?: number; operationTimeoutMs?: number; prefix?: string } = {}) =>
  new AzureBlobObjectStorage({ container: 'powersync-object-storage-test', connectionString, ...options });

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('Azure Blob object storage', () => {
  test('decodes Azure configuration alongside existing S3 configuration', () => {
    for (const object_storage of [
      { type: 'azure', container: 'test', account_url: 'https://example.blob.core.windows.net' },
      { type: 'azure', container: 'test', connection_string: connectionString, operation_timeout_ms: 60000 },
      { type: 's3', bucket: 'test', region: 'us-east-1' }
    ] as const) {
      const config = MongoStorageConfig.decode({ type: 'mongodb', uri: 'mongodb://localhost/test', object_storage });
      expect(config.object_storage).toMatchObject(object_storage);
    }
  });

  test('requires one auth mode and validates options before making requests', () => {
    expect(() => new AzureBlobObjectStorage({ container: 'test' })).toThrow(/exactly one/);
    expect(
      () => new AzureBlobObjectStorage({ container: 'test', connectionString, accountUrl: 'https://example.com' })
    ).toThrow(/exactly one/);
    expect(() => new AzureBlobObjectStorage({ container: 'test', accountUrl: 'http://example.com' })).toThrow(/HTTPS/);
    expect(
      () => new AzureBlobObjectStorage({ container: 'test', accountUrl: 'https://example.com?sig=secret' })
    ).toThrow(/without credentials/);
    const identityStorage = new AzureBlobObjectStorage({
      container: 'test',
      accountUrl: 'https://example.blob.core.windows.net'
    });
    expect(identityStorage.container.url).toBe('https://example.blob.core.windows.net/test');
    for (const value of [0, -1, 1.5, NaN, Infinity]) {
      expect(() => createStorage({ concurrencyLimit: value })).toThrow(/positive integer/);
      expect(() => createStorage({ operationTimeoutMs: value })).toThrow(/integer between/);
    }
    expect(() => createStorage({ prefix: 'a/' })).toThrow(/must not end/);
    expect(() => createStorage({ prefix: '💾'.repeat(65) })).toThrow(/256 UTF-8 bytes/);
    expect(() => createStorage({ operationTimeoutMs: 2 ** 31 })).toThrow(/integer between/);
  });

  test('validates download lengths and preserves BSON content headers', async () => {
    const storage = createStorage();
    const blob = storage.container.getBlobClient('object');
    vi.spyOn(storage.container, 'getBlobClient').mockReturnValue(blob);
    const download = vi.spyOn(blob, 'download');
    for (const [length, bytes, valid] of [
      [3, [1, 2, 3], true],
      [4, [1, 2, 3], false],
      [2, [1, 2, 3], false],
      [undefined, [1], false]
    ] as const) {
      download.mockResolvedValue({
        contentLength: length,
        contentType: 'application/bson',
        readableStreamBody: Readable.from([Buffer.from(bytes)])
      } as unknown as BlobDownloadResponseParsed);
      if (valid) {
        await expect(storage.get('object')).resolves.toEqual({ data: new Uint8Array(bytes), metadata });
      } else {
        await expect(storage.get('object')).rejects.toMatchObject({ name: 'ObjectStorageError', retryable: false });
      }
    }
  });

  test.each([
    [{ statusCode: 503 }, true],
    [{ statusCode: 429 }, true],
    [{ code: 'ECONNRESET' }, true],
    [{ statusCode: 403 }, false],
    [{ statusCode: 404, code: 'BlobNotFound' }, false]
  ])('classifies failures for compaction retries: %j', async (error, retryable) => {
    const storage = createStorage();
    const blob = storage.container.getBlobClient('object');
    vi.spyOn(storage.container, 'getBlobClient').mockReturnValue(blob);
    vi.spyOn(blob, 'download').mockRejectedValue(error);
    await expect(storage.get('object')).rejects.toMatchObject({ name: 'ObjectStorageError', cause: error, retryable });
  });

  test('bounds a stalled streaming body and releases its concurrency slot', async () => {
    vi.useFakeTimers();
    const storage = createStorage({ concurrencyLimit: 1, operationTimeoutMs: 50 });
    const blob = storage.container.getBlobClient('object');
    vi.spyOn(storage.container, 'getBlobClient').mockReturnValue(blob);
    const stream = new Readable({ read() {} });
    vi.spyOn(blob, 'download')
      .mockResolvedValueOnce({
        contentLength: 1,
        readableStreamBody: stream
      } as unknown as BlobDownloadResponseParsed)
      .mockRejectedValue(new Error('next operation reached Azure'));
    const running = storage.get('object');
    const assertion = expect(running).rejects.toMatchObject({
      name: 'ObjectStorageError',
      retryable: true,
      message: expect.stringContaining('timed out')
    });
    await vi.advanceTimersByTimeAsync(50);
    await assertion;
    expect(stream.destroyed).toBe(true);
    await expect(storage.get('next')).rejects.toMatchObject({ cause: new Error('next operation reached Azure') });
  });

  test('cancels queued and active requests while preserving the caller reason', async () => {
    const storage = createStorage({ concurrencyLimit: 1 });
    const blob = storage.container.getBlockBlobClient('object');
    vi.spyOn(storage.container, 'getBlockBlobClient').mockReturnValue(blob);
    let started!: () => void;
    const ready = new Promise<void>((resolve) => (started = resolve));
    const upload = vi.spyOn(blob, 'upload').mockImplementation(async (_body, _length, options) => {
      started();
      await new Promise((_resolve, reject) =>
        options!.abortSignal!.addEventListener('abort', () => reject(new Error('aborted')))
      );
      throw new Error('unreachable');
    });
    const active = new AbortController();
    const reason = new Error('replication stopped');
    const running = storage.put('object', new Uint8Array(), metadata, { signal: active.signal });
    const assertion = expect(running).rejects.toBe(reason);
    await ready;
    const queued = new AbortController();
    const waiting = storage.put('other', new Uint8Array(), metadata, { signal: queued.signal });
    const queuedAssertion = expect(waiting).rejects.toBe(reason);
    queued.abort(reason);
    await queuedAssertion;
    expect(upload).toHaveBeenCalledTimes(1);
    active.abort(reason);
    await assertion;
  });

  test('checks batch subresponses, treats missing blobs idempotently, and forwards cancellation', async () => {
    const storage = createStorage();
    const batchClient = storage.container.getBlobBatchClient();
    vi.spyOn(storage.container, 'getBlobBatchClient').mockReturnValue(batchClient);
    const submit = vi.spyOn(batchClient, 'submitBatch');
    const response = (status: number, errorCode?: string) =>
      ({ subResponses: [{ status, errorCode }] }) as Awaited<ReturnType<typeof batchClient.submitBatch>>;
    submit.mockResolvedValue(response(404, 'BlobNotFound'));
    await expect(storage.delete(['missing'])).resolves.toBeUndefined();
    expect(submit.mock.calls[0][1]?.abortSignal).toBeDefined();
    submit.mockResolvedValue(response(503, 'ServerBusy'));
    await expect(storage.delete(['object'])).rejects.toMatchObject({ retryable: true });
    submit.mockResolvedValue(response(403, 'AuthorizationFailure'));
    await expect(storage.delete(['object'])).rejects.toMatchObject({ retryable: false });
    submit.mockResolvedValue(response(404, 'ContainerNotFound'));
    await expect(storage.delete(['object'])).rejects.toMatchObject({ retryable: false });
    submit.mockResolvedValue({ subResponses: [] } as unknown as Awaited<ReturnType<typeof batchClient.submitBatch>>);
    await expect(storage.delete(['object'])).rejects.toMatchObject({ retryable: true });
  });
});

describe.skipIf(!process.env.AZURITE_CONNECTION_STRING)('Azure Blob emulator integration', () => {
  test('round-trips BSON and deletes across pages without crossing prefix boundaries', async () => {
    const prefix = `adapter-${randomUUID()}`;
    const storage = new AzureBlobObjectStorage({
      container: 'powersync-object-storage-test',
      connectionString: process.env.AZURITE_CONNECTION_STRING!,
      prefix
    });
    const sibling = new AzureBlobObjectStorage({
      container: 'powersync-object-storage-test',
      connectionString: process.env.AZURITE_CONNECTION_STRING!,
      prefix: `${prefix}-sibling`
    });
    try {
      const chunks = new BucketDataObjectStorage(storage);
      await chunks.store('bucket-data/bson', [
        { o: 1n, op: 'REMOVE', table: 'items', row_id: '1', checksum: 1n, data: null }
      ]);
      const ops = await chunks.retrieve('bucket-data/bson', {});
      expect(ops).toMatchObject([{ row_id: '1', o: 1n, checksum: 1n }]);
      await sibling.put('bucket-data/keep', new Uint8Array([7]), metadata);
      await Promise.all(
        Array.from({ length: 260 }, (_, i) => storage.put(`bucket-data/${i}`, new Uint8Array([i % 256]), metadata))
      );
      const paths: string[] = [];
      for await (const path of storage.list('bucket-data/')) paths.push(path);
      expect(paths).toHaveLength(261);
      expect(paths).toContain('bucket-data/bson');
      await expect(storage.deletePrefix('bucket-data/')).resolves.toEqual({ objectCount: 261 });
      await expect(storage.delete(['bucket-data/missing'])).resolves.toBeUndefined();
      await expect(storage.get('bucket-data/bson')).rejects.toMatchObject({ retryable: false });
      await expect(sibling.get('bucket-data/keep')).resolves.toMatchObject({ data: new Uint8Array([7]) });
      await expect(storage.deletePrefix('bucket-data/')).resolves.toEqual({ objectCount: 0 });
    } finally {
      await storage.deletePrefix('');
      await sibling.deletePrefix('');
    }
  });
});
