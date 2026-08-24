import {
  DeleteObjectsCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
  PutObjectCommand,
  S3Client
} from '@aws-sdk/client-s3';
import { logger } from '@powersync/lib-services-framework';
import { acquireSemaphoreAbortable, isAbortError } from '@powersync/service-core';
import { loadConfigsForDefaultMode, type DefaultsMode, type ResolvedDefaultsMode } from '@smithy/core/client';
import { isThrottlingError, isTransientError } from '@smithy/core/retry';
import { NodeHttpHandler } from '@smithy/node-http-handler';
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

/**
 * Timeouts are derived from the AWS defaults mode, rather than being individually configurable.
 *
 * The mode - AWS_DEFAULTS_MODE, defaults_mode in the AWS shared config file, or the defaults_mode
 * storage option - describes the expected latency between this service and the object storage
 * endpoint. We use the connection timeout it defines as the baseline for the other timeouts:
 *
 * | mode                   | connection | request | operation | queue |
 * | ---------------------- | ---------- | ------- | --------- | ----- |
 * | in-region              |       1.1s |    5.5s |       11s |   44s |
 * | standard, cross-region |       3.1s |   15.5s |       31s |  124s |
 * | mobile                 |        30s |    150s |      300s | 1200s |
 *
 * The multipliers are sized for the objects we store: bucket data chunks target 1MB, and are
 * bounded by the 16MB BSON document limit. Even at the upper bound, the request timeout only has to
 * cover a transfer of a few MB/s, and the operation timeout leaves room for a second attempt.
 *
 * `legacy`, the mode used when nothing is configured, does not define any timeouts, and is treated
 * as `standard` here. Running without timeouts is not an option: a stalled request holds one of the
 * limited operation slots until the process restarts, and enough of those block all reads.
 *
 * `auto` is also treated as `standard` - see resolveConfiguredDefaultsMode() below.
 */
const BASELINE_CONNECTION_TIMEOUT_MS = 3_100;
const REQUEST_TIMEOUT_FACTOR = 5;
const OPERATION_TIMEOUT_FACTOR = 2;
/**
 * How long an operation may wait for a concurrency slot, as a multiple of the operation timeout.
 *
 * Every slot holder now releases within the operation timeout, so the queue always drains: this is
 * a backpressure limit rather than a deadlock guard. Waiting this long means at least
 * `factor * concurrencyLimit` operations ahead of us each took their full deadline - 64 at the
 * default concurrency. Real operations complete in a fraction of the deadline, so the number of
 * queued operations this actually tolerates is far higher, well above the ~2000 uploads that the
 * largest possible replication flush (MAX_TRANSACTION_DOC_COUNT) can enqueue at once.
 */
const QUEUE_TIMEOUT_FACTOR = 4;

export interface S3TimeoutProfile {
  /**
   * Per attempt: establishing the connection, including waiting for a socket from the pool.
   */
  connectionTimeoutMs: number;
  /**
   * Per attempt: from starting the request until the response headers are received. For uploads
   * this includes sending the body.
   */
  requestTimeoutMs: number;
  /**
   * The complete operation, including SDK retries and streaming the response body.
   *
   * The SDK timeouts above stop applying once the response headers are received, so this is what
   * bounds how long a single operation can occupy a concurrency slot.
   */
  operationTimeoutMs: number;
  /**
   * Waiting for a concurrency slot, before the operation itself starts.
   */
  queueTimeoutMs: number;
}

export function resolveTimeoutProfile(mode: ResolvedDefaultsMode): S3TimeoutProfile {
  // Only connectionTimeout is currently vended by the SDK, but requestTimeout is part of the same
  // contract - use it as the hint if a future version starts defining it.
  const { connectionTimeout, requestTimeout } = loadConfigsForDefaultMode(mode);
  const connectionTimeoutMs = connectionTimeout ?? BASELINE_CONNECTION_TIMEOUT_MS;
  const requestTimeoutMs = requestTimeout ?? connectionTimeoutMs * REQUEST_TIMEOUT_FACTOR;
  const operationTimeoutMs = requestTimeoutMs * OPERATION_TIMEOUT_FACTOR;
  return {
    connectionTimeoutMs,
    requestTimeoutMs,
    operationTimeoutMs,
    queueTimeoutMs: operationTimeoutMs * QUEUE_TIMEOUT_FACTOR
  };
}

const AWS_DEFAULTS_MODE_ENV = 'AWS_DEFAULTS_MODE';
const RESOLVED_DEFAULTS_MODES: ResolvedDefaultsMode[] = ['standard', 'in-region', 'cross-region', 'mobile', 'legacy'];

/**
 * Resolve the configured AWS defaults mode: the explicit option, then the AWS_DEFAULTS_MODE
 * environment variable, then `legacy`, which is also the AWS default.
 *
 * Unlike the SDK's own lookup, this does not read defaults_mode from the AWS shared config file.
 * That lookup is asynchronous, and the mode is only used to pick timeouts, which we want available
 * synchronously so that operations never have to wait on configuration.
 *
 * `auto` is substituted with `standard`. It asks the SDK to detect whether it is running in the
 * same region as the endpoint, which means querying the EC2 instance metadata service on startup:
 *
 * - The detection only succeeds on EC2-family compute with instance metadata reachable. Anywhere
 *   else - including EC2 with IMDS disabled or behind a low hop limit - it silently falls back to
 *   `standard`, so the timeouts differ by deployment with no indication of why.
 * - It requires a resolvable region, and fails the client outright when there is none.
 * - Timeouts that depend on where the process happens to run are hard to reason about when
 *   diagnosing a stall.
 *
 * Deployments that want the tighter in-region timeouts should ask for them explicitly.
 */
function resolveConfiguredDefaultsMode(configured?: DefaultsMode): ResolvedDefaultsMode {
  // An empty environment variable is treated as unset, matching the SDK.
  const mode = (configured || process.env[AWS_DEFAULTS_MODE_ENV] || 'legacy').toLowerCase();
  if (mode === 'auto') {
    logger.warn(
      `Ignoring AWS defaults mode "auto" for object storage, using "standard" instead. ` +
        `Configure storage.object_storage.defaults_mode to select a mode explicitly.`
    );
    return 'standard';
  }
  const resolved = RESOLVED_DEFAULTS_MODES.find((candidate) => candidate === mode);
  if (resolved == null) {
    throw new Error(
      `Invalid AWS defaults mode ${JSON.stringify(mode)}, expected one of ${RESOLVED_DEFAULTS_MODES.join(', ')}, auto`
    );
  }
  return resolved;
}

/**
 * An S3 operation holding a concurrency slot, released when the operation is disposed.
 */
interface S3Operation extends AsyncDisposable {
  /**
   * The caller's signal, combined with the operation deadline.
   *
   * Use this for the request itself, so that neither a stalled request nor a stalled response body
   * can hold on to the slot indefinitely.
   */
  signal: AbortSignal;
}

export interface S3ObjectStorageOptions {
  bucket: string;
  region?: string;
  prefix?: string;
  endpoint?: string;
  forcePathStyle?: boolean;
  accessKeyId?: string;
  secretAccessKey?: string;
  concurrencyLimit?: number;
  /**
   * AWS defaults mode, used as the baseline for the request timeouts.
   *
   * Defaults to the AWS_DEFAULTS_MODE environment variable. `auto` is treated as `standard`.
   */
  defaultsMode?: DefaultsMode;
}

export class S3ObjectStorage implements ObjectStorage {
  /**
   * Public for tests only.
   */
  public readonly client: S3Client;
  private bucket: string;
  private prefix: string;
  private readonly operationSemaphore: SemaphoreInterface;
  private readonly timeouts: S3TimeoutProfile;

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

    // Resolve the mode once, and share it with the client so that its own defaults agree with ours.
    const defaultsMode = resolveConfiguredDefaultsMode(options.defaultsMode);
    this.timeouts = resolveTimeoutProfile(defaultsMode);

    this.client = new S3Client({
      region: options.region,
      endpoint: options.endpoint,
      forcePathStyle: options.forcePathStyle,
      defaultsMode,
      requestHandler: new NodeHttpHandler({
        connectionTimeout: this.timeouts.connectionTimeoutMs,
        requestTimeout: this.timeouts.requestTimeoutMs,
        // Without this, exceeding requestTimeout only logs a warning.
        throwOnRequestTimeout: true
      }),
      credentials:
        options.accessKeyId && options.secretAccessKey
          ? { accessKeyId: options.accessKeyId, secretAccessKey: options.secretAccessKey }
          : undefined
    });
  }

  async put(path: string, data: Uint8Array, metadata: ObjectStoragePutMetadata): Promise<void> {
    const fullPath = this.fullPath(path);
    await using operation = await this.withOperation();
    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: fullPath,
          Body: data,
          ContentType: metadata.contentType,
          ContentEncoding: metadata.contentEncoding ?? undefined
        }),
        { abortSignal: operation.signal }
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
    await using operation = await this.withOperation(options?.signal);
    // Includes the operation deadline, which also covers streaming the response body below.
    const signal = operation.signal;
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
        signal.throwIfAborted();
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
      await using operation = await this.withOperation(signal);
      return await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.bucket,
          Prefix: fullPrefix,
          ContinuationToken: continuationToken,
          MaxKeys: S3_DELETE_PREFIX_BATCH_SIZE
        }),
        { abortSignal: operation.signal }
      );
    } catch (error) {
      throw s3OperationError('list', fullPrefix, error);
    }
  }

  private async deleteFullPaths(fullPaths: { Key: string }[], signal?: AbortSignal): Promise<void> {
    if (fullPaths.length === 0) return;
    await using operation = await this.withOperation(signal);
    let response;
    try {
      response = await this.client.send(
        new DeleteObjectsCommand({
          Bucket: this.bucket,
          Delete: { Objects: fullPaths, Quiet: true }
        }),
        { abortSignal: operation.signal }
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

  private async withOperation(signal?: AbortSignal): Promise<S3Operation> {
    signal?.throwIfAborted();
    const { operationTimeoutMs, queueTimeoutMs } = this.timeouts;

    // Waiting for a slot is bounded even when the caller has no signal of its own, which is the
    // case for uploads and deletes.
    const queueDeadline = timeoutSignal(queueTimeoutMs, `S3 operation waited ${queueTimeoutMs} ms for a slot`);
    let acquired;
    try {
      acquired = await acquireSemaphoreAbortable(
        this.operationSemaphore,
        signal ? AbortSignal.any([signal, queueDeadline.signal]) : queueDeadline.signal
      );
    } finally {
      queueDeadline.cancel();
    }
    if (acquired === 'aborted') {
      // A caller abort takes precedence: it is a cancellation, not an S3 failure.
      signal?.throwIfAborted();
      throw new ObjectStorageError(`Timed out waiting for an S3 operation slot`, {
        cause: queueDeadline.signal.reason,
        retryable: true
      });
    }

    const [, release] = acquired;
    if (signal?.aborted) {
      release();
      signal.throwIfAborted();
    }
    // This deadline only starts once the operation holds a slot - queueing is covered above.
    const deadline = timeoutSignal(operationTimeoutMs, `S3 operation did not complete within ${operationTimeoutMs} ms`);
    return {
      signal: signal ? AbortSignal.any([signal, deadline.signal]) : deadline.signal,
      [Symbol.asyncDispose]: async () => {
        deadline.cancel();
        release();
      }
    };
  }
}

/**
 * An abort signal that fires after the given timeout, using a timer that does not hold the process
 * open. TimeoutError is what isTransientS3Error() classifies as retryable, matching the SDK's own
 * request timeouts.
 */
function timeoutSignal(timeoutMs: number, message: string): { signal: AbortSignal; cancel: () => void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new DOMException(message, 'TimeoutError')), timeoutMs);
  timer.unref();
  return { signal: controller.signal, cancel: () => clearTimeout(timer) };
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
