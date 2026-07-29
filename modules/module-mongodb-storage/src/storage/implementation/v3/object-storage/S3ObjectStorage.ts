import {
  DeleteObjectsCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
  PutObjectCommand,
  S3Client
} from '@aws-sdk/client-s3';
import { acquireSemaphoreAbortable, isAbortError } from '@powersync/service-core';
import { isThrottlingError, isTransientError } from '@smithy/core/retry';
import { Semaphore, SemaphoreInterface } from 'async-mutex';
import { ObjectStorageError, type ObjectStorage, type ObjectStoragePutMetadata } from './ObjectStorage.js';

const DEFAULT_S3_OPERATION_CONCURRENCY = 16;
const S3_DELETE_PREFIX_BATCH_SIZE = 1000;
/**
 * Slightly smaller than DEFAULT_S3_OPERATION_CONCURRENCY.
 */
const S3_DELETE_PREFIX_CONCURRENCY = 12;
const MAX_S3_PREFIX_BYTES = 256;
const SAFE_S3_KEY_BYTES = 896;

export interface S3ObjectStorageOptions {
  bucket: string;
  region: string;
  prefix?: string;
  endpoint?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  concurrencyLimit?: number;
}

export class S3ObjectStorage implements ObjectStorage {
  /**
   * Public for tests only.
   */
  public readonly client: S3Client;
  private bucket: string;
  private prefix: string;
  private readonly operationSemaphore: SemaphoreInterface;

  constructor(options: S3ObjectStorageOptions) {
    const concurrencyLimit = options.concurrencyLimit ?? DEFAULT_S3_OPERATION_CONCURRENCY;
    if (!Number.isInteger(concurrencyLimit) || concurrencyLimit <= 0) {
      throw new Error('S3 object storage concurrencyLimit must be a positive integer');
    }
    if ((options.accessKeyId == null) !== (options.secretAccessKey == null)) {
      throw new Error('S3 object storage accessKeyId and secretAccessKey must be configured together');
    }

    const prefix = options.prefix ?? '';
    if (Buffer.byteLength(prefix, 'utf8') > MAX_S3_PREFIX_BYTES) {
      throw new Error(`S3 object storage prefix must be at most ${MAX_S3_PREFIX_BYTES} UTF-8 bytes`);
    }
    if (prefix.endsWith('/')) {
      throw new Error('S3 object storage prefix must not end with "/"');
    }

    this.bucket = options.bucket;
    this.prefix = prefix;
    this.operationSemaphore = new Semaphore(concurrencyLimit);
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
    const fullPath = this.fullPath(path);
    await using _ = await this.withOperation();
    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: fullPath,
          Body: data,
          ContentType: metadata.contentType,
          ContentEncoding: metadata.contentEncoding ?? undefined
        })
      );
    } catch (error) {
      throw s3OperationError('upload', fullPath, error);
    }
  }

  async get(
    path: string,
    options?: { signal?: AbortSignal }
  ): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }> {
    const fullPath = this.fullPath(path);
    const signal = options?.signal;
    await using _ = await this.withOperation(signal);
    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: fullPath
        }),
        { abortSignal: signal }
      );
      const contentLength = response.ContentLength;
      if (contentLength == null) {
        throw new Error(`S3 download response for ${fullPath} is missing ContentLength`);
      }
      if (!Number.isSafeInteger(contentLength) || contentLength < 0) {
        throw new Error(`S3 download response for ${fullPath} has invalid ContentLength ${contentLength}`);
      }

      const data = new Uint8Array(contentLength);
      let offset = 0;
      const stream = response.Body as AsyncIterable<Uint8Array>;
      for await (const chunk of stream) {
        signal?.throwIfAborted();
        const nextOffset = offset + chunk.byteLength;
        if (nextOffset > contentLength) {
          throw new Error(
            `S3 download ContentLength mismatch for ${fullPath}: expected ${contentLength} bytes, received at least ${nextOffset}`
          );
        }
        data.set(chunk, offset);
        offset = nextOffset;
      }
      if (offset !== contentLength) {
        throw new Error(
          `S3 download ContentLength mismatch for ${fullPath}: expected ${contentLength} bytes, received ${offset}`
        );
      }

      return {
        data,
        metadata: {
          contentType: response.ContentType ?? 'application/octet-stream',
          contentEncoding: response.ContentEncoding ?? null
        }
      };
    } catch (err: any) {
      if (err.name === 'NoSuchKey' || err.Code === 'NoSuchKey') {
        throw new ObjectStorageError(`S3 object not found: ${fullPath}`, { cause: err, retryable: false });
      }
      throw s3OperationError('download', fullPath, err);
    }
  }

  async *list(prefix: string, options?: { signal?: AbortSignal }): AsyncIterable<string> {
    const fullPrefix = this.fullPath(prefix);
    let continuationToken: string | undefined;
    const signal = options?.signal;

    do {
      signal?.throwIfAborted();
      const response = await this.listPage(fullPrefix, continuationToken, signal);
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
    const fullPaths = paths.map((path) => ({ Key: this.fullPath(path) }));
    await this.deleteFullPaths(fullPaths);
  }

  async deletePrefix(prefix: string, options?: { signal?: AbortSignal }): Promise<{ objectCount: number }> {
    const fullPrefix = this.fullPath(prefix);
    const signal = options?.signal;
    let continuationToken: string | undefined;
    let objectCount = 0;
    const deleting = new Set<Promise<void>>();
    let deleteError: { error: unknown } | undefined;

    const scheduleDelete = (fullPaths: { Key: string }[]) => {
      let tracked: Promise<void>;
      tracked = this.deleteFullPaths(fullPaths, signal)
        .catch((error) => {
          deleteError ??= { error };
        })
        .finally(() => deleting.delete(tracked));
      deleting.add(tracked);
    };
    const throwDeleteError = () => {
      if (deleteError != null) {
        throw deleteError.error;
      }
    };

    let operationError: { error: unknown } | undefined;
    try {
      do {
        signal?.throwIfAborted();
        const response = await this.listPage(fullPrefix, continuationToken, signal);
        throwDeleteError();
        const fullPaths: { Key: string }[] = [];
        for (const object of response.Contents ?? []) {
          if (object.Key != null) {
            fullPaths.push({ Key: object.Key });
          }
        }
        objectCount += fullPaths.length;
        if (fullPaths.length !== 0) {
          while (deleting.size >= S3_DELETE_PREFIX_CONCURRENCY) {
            await Promise.race(deleting);
            throwDeleteError();
          }
          scheduleDelete(fullPaths);
        }
        continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
        if (response.IsTruncated && continuationToken == null) {
          throw new Error(`S3 listing for ${fullPrefix} was truncated without a continuation token`);
        }
      } while (continuationToken != null);
    } catch (error) {
      operationError = { error };
    }

    await Promise.all(deleting);
    if (operationError != null) {
      throw operationError.error;
    }
    throwDeleteError();
    return { objectCount };
  }

  private async listPage(
    fullPrefix: string,
    continuationToken: string | undefined,
    signal?: AbortSignal
  ): Promise<ListObjectsV2CommandOutput> {
    try {
      await using _ = await this.withOperation(signal);
      return await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          Prefix: fullPrefix,
          ContinuationToken: continuationToken,
          MaxKeys: S3_DELETE_PREFIX_BATCH_SIZE
        }),
        { abortSignal: signal }
      );
    } catch (error) {
      throw s3OperationError('list', fullPrefix, error);
    }
  }

  private async deleteFullPaths(fullPaths: { Key: string }[], signal?: AbortSignal): Promise<void> {
    if (fullPaths.length === 0) return;
    await using _ = await this.withOperation(signal);
    let response;
    try {
      response = await this.client.send(
        new DeleteObjectsCommand({
          Bucket: this.bucket,
          Delete: { Objects: fullPaths, Quiet: true }
        }),
        { abortSignal: signal }
      );
    } catch (error) {
      throw s3OperationError('delete', `${fullPaths.length} objects`, error);
    }
    if (response.Errors?.length) {
      const errors = response.Errors.map((error) => `${error.Key}: ${error.Code}`).join(', ');
      throw new ObjectStorageError(`Failed to delete S3 objects: ${errors}`, {
        cause: response.Errors,
        retryable: response.Errors.some((error) => isTransientS3Error({ name: error.Code }))
      });
    }
  }

  private fullPath(path: string): string {
    const fullPath = this.prefix ? `${this.prefix}/${path}` : path;
    const size = Buffer.byteLength(fullPath, 'utf8');
    if (size > SAFE_S3_KEY_BYTES) {
      throw new Error(`S3 object key is ${size} bytes, exceeding the safe limit of ${SAFE_S3_KEY_BYTES} bytes`);
    }
    return fullPath;
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

function s3OperationError(operation: string, target: string, error: unknown): Error {
  if (error instanceof ObjectStorageError) {
    return error;
  }
  // Preserve abort errors so callers can distinguish cancellation from an S3
  // failure and avoid retrying work after shutdown.
  if (isAbortError(error)) {
    return error as Error;
  }
  const detail = error instanceof Error ? `: ${error.message}` : '';
  return new ObjectStorageError(`S3 ${operation} failed for ${target}${detail}`, {
    cause: error,
    retryable: isTransientS3Error(error)
  });
}

function isTransientS3Error(error: unknown): boolean {
  if (typeof error != 'object' || error == null) {
    return false;
  }
  if (isAbortError(error)) {
    return false;
  }

  const sdkError = error as Parameters<typeof isTransientError>[0];
  // Use the same classification as the AWS SDK retry middleware. The SDK
  // returns the final error after its own attempt/quota limits are exhausted;
  // this tells the compactor whether restarting the larger bucket operation is
  // still appropriate.
  // One exception is clock skew errors: We don't treat that as retryable by us,
  // although the SDK may internally retry them.
  return isThrottlingError(sdkError) || isTransientError(sdkError);
}
