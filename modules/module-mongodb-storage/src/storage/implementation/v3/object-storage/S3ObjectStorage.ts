import {
  DeleteObjectsCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client
} from '@aws-sdk/client-s3';
import { acquireSemaphoreAbortable } from '@powersync/service-core';
import { Semaphore, SemaphoreInterface } from 'async-mutex';
import type { ObjectStorage, ObjectStoragePutMetadata } from './ObjectStorage.js';

const S3_OPERATION_CONCURRENCY = 16;

export interface S3ObjectStorageOptions {
  bucket: string;
  region: string;
  prefix?: string;
  endpoint?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
}

export class S3ObjectStorage implements ObjectStorage {
  private client: S3Client;
  private bucket: string;
  private prefix: string;
  private readonly operationSemaphore: SemaphoreInterface = new Semaphore(S3_OPERATION_CONCURRENCY);

  constructor(options: S3ObjectStorageOptions) {
    this.bucket = options.bucket;
    this.prefix = options.prefix ?? '';
    this.client = new S3Client({
      region: options.region,
      endpoint: options.endpoint,
      forcePathStyle: !!options.endpoint,
      credentials:
        options.accessKeyId && options.secretAccessKey
          ? { accessKeyId: options.accessKeyId, secretAccessKey: options.secretAccessKey }
          : undefined
    });
  }

  async put(path: string, data: Uint8Array, metadata: ObjectStoragePutMetadata): Promise<void> {
    const fullPath = this.prefix ? `${this.prefix}/${path}` : path;
    await using _ = await this.withOperation();
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: fullPath,
        Body: data,
        ContentType: metadata.contentType,
        ContentEncoding: metadata.contentEncoding ?? undefined
      })
    );
  }

  async get(path: string, signal?: AbortSignal): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }> {
    await using _ = await this.withOperation(signal);
    const fullPath = this.prefix ? `${this.prefix}/${path}` : path;
    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: fullPath
        }),
        { abortSignal: signal }
      );
      const chunks: Uint8Array[] = [];
      const stream = response.Body as AsyncIterable<Uint8Array>;
      for await (const chunk of stream) {
        signal?.throwIfAborted();
        chunks.push(chunk);
      }
      return {
        data: mergeChunks(chunks),
        metadata: {
          contentType: response.ContentType ?? 'application/octet-stream',
          contentEncoding: response.ContentEncoding ?? null
        }
      };
    } catch (err: any) {
      if (err.name === 'NoSuchKey' || err.Code === 'NoSuchKey') {
        throw new Error(`S3 object not found: ${fullPath}`);
      }
      throw err;
    }
  }

  async *list(prefix: string, signal?: AbortSignal): AsyncIterable<string> {
    const fullPrefix = this.prefix ? `${this.prefix}/${prefix}` : prefix;
    let continuationToken: string | undefined;

    do {
      signal?.throwIfAborted();
      const response = await (async () => {
        await using _ = await this.withOperation(signal);
        return this.client.send(
          new ListObjectsV2Command({
            Bucket: this.bucket,
            Prefix: fullPrefix,
            ContinuationToken: continuationToken
          }),
          { abortSignal: signal }
        );
      })();
      for (const object of response.Contents ?? []) {
        if (object.Key == null) {
          continue;
        }
        yield this.prefix ? object.Key.slice(this.prefix.length + 1) : object.Key;
      }
      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
      if (response.IsTruncated && continuationToken == null) {
        throw new Error(`S3 listing for ${fullPrefix} was truncated without a continuation token`);
      }
    } while (continuationToken != null);
  }

  async delete(paths: string[]): Promise<void> {
    if (paths.length === 0) return;
    const fullPaths = paths.map((p) => ({ Key: this.prefix ? `${this.prefix}/${p}` : p }));
    await using _ = await this.withOperation();
    const response = await this.client.send(
      new DeleteObjectsCommand({
        Bucket: this.bucket,
        Delete: { Objects: fullPaths, Quiet: true }
      })
    );
    if (response.Errors?.length) {
      throw new Error(
        `Failed to delete S3 objects: ${response.Errors.map((error) => `${error.Key}: ${error.Code}`).join(', ')}`
      );
    }
  }

  private async withOperation(signal?: AbortSignal): Promise<AsyncDisposable> {
    signal?.throwIfAborted();
    const acquired = signal
      ? await acquireSemaphoreAbortable(this.operationSemaphore, signal)
      : await this.operationSemaphore.acquire();
    if (acquired === 'aborted') {
      signal!.throwIfAborted();
      throw new Error('S3 operation aborted while waiting for a concurrency slot');
    }

    const [, release] = acquired;
    if (signal?.aborted) {
      release();
      signal.throwIfAborted();
    }
    return {
      [Symbol.asyncDispose]: async () => release()
    };
  }
}

function mergeChunks(chunks: Uint8Array[]): Uint8Array {
  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}
