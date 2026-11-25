import { DefaultAzureCredential } from '@azure/identity';
import {
  BlobServiceClient,
  type ContainerClient,
  type ContainerListBlobFlatSegmentResponse
} from '@azure/storage-blob';
import { acquireSemaphoreAbortable, isAbortError } from '@powersync/service-core';
import { Semaphore } from 'async-mutex';
import {
  ObjectStorageError,
  type ObjectStorage,
  type ObjectStorageOperationOptions,
  type ObjectStoragePutMetadata
} from './ObjectStorage.js';

// Only dynamically import this module: Azure SDKs should only load for Azure deployments.

const DEFAULT_CONCURRENCY = 64;
const DEFAULT_OPERATION_TIMEOUT_MS = 60_000;
const AZURE_REQUEST_TIMEOUT_MS = 20_000;
const DELETE_BATCH_SIZE = 256;
const MAX_PREFIX_BYTES = 256;
const SAFE_KEY_BYTES = 896;

export interface AzureBlobObjectStorageOptions {
  container: string;
  /** Exactly one of connectionString and accountUrl must be configured. */
  connectionString?: string;
  /** HTTPS account endpoint; authenticates through DefaultAzureCredential. */
  accountUrl?: string;
  prefix?: string;
  concurrencyLimit?: number;
  operationTimeoutMs?: number;
}

export class AzureBlobObjectStorage implements ObjectStorage {
  /** Public for adapter tests only. The container must already exist. */
  readonly container: ContainerClient;
  private readonly prefix: string;
  private readonly semaphore: Semaphore;
  private readonly operationTimeoutMs: number;

  constructor(options: AzureBlobObjectStorageOptions) {
    if ((options.connectionString != null) === (options.accountUrl != null)) {
      throw new Error('Azure object storage requires exactly one of connectionString and accountUrl');
    }
    const concurrency = options.concurrencyLimit ?? DEFAULT_CONCURRENCY;
    if (!Number.isSafeInteger(concurrency) || concurrency <= 0) {
      throw new Error('Azure object storage concurrencyLimit must be a positive integer');
    }
    this.operationTimeoutMs = options.operationTimeoutMs ?? DEFAULT_OPERATION_TIMEOUT_MS;
    if (
      !Number.isSafeInteger(this.operationTimeoutMs) ||
      this.operationTimeoutMs <= 0 ||
      this.operationTimeoutMs > 2 ** 31 - 1
    ) {
      throw new Error('Azure object storage operationTimeoutMs must be an integer between 1 and 2147483647');
    }
    this.prefix = options.prefix ?? '';
    // Keep names portable between our providers. Prefixes are namespace boundaries,
    // not directory resources: every operation adds the separating slash itself.
    if (Buffer.byteLength(this.prefix, 'utf8') > MAX_PREFIX_BYTES || this.prefix.endsWith('/')) {
      throw new Error('Azure object storage prefix must be at most 256 UTF-8 bytes and must not end with "/"');
    }
    this.semaphore = new Semaphore(concurrency);

    // Use Azure's exponential retry policy, with up to three attempts. Its per-try
    // timeout alone does not bound retries or streaming response bodies, so run()
    // also supplies a deadline for the complete operation. No AWS latency modes apply.
    const clientOptions = {
      retryOptions: { maxTries: 3, tryTimeoutInMs: Math.min(AZURE_REQUEST_TIMEOUT_MS, this.operationTimeoutMs) }
    };
    let client: BlobServiceClient;
    if (options.connectionString != null) {
      client = BlobServiceClient.fromConnectionString(options.connectionString, clientOptions);
    } else {
      const url = new URL(options.accountUrl!);
      if (url.protocol !== 'https:' || url.search || url.hash || url.username || url.password) {
        throw new Error(
          'Azure object storage accountUrl must be an HTTPS endpoint without credentials, query or fragment'
        );
      }
      // Supports workload identity, managed identity, environment credentials and
      // local Azure CLI credentials. AZURE_CLIENT_ID selects a user-assigned identity.
      client = new BlobServiceClient(url.href, new DefaultAzureCredential(), clientOptions);
    }
    this.container = client.getContainerClient(options.container);
  }

  async put(
    path: string,
    data: Uint8Array,
    metadata: ObjectStoragePutMetadata,
    options?: ObjectStorageOperationOptions
  ): Promise<void> {
    const blob = this.container.getBlockBlobClient(this.fullPath(path));
    await this.run(`upload for ${path}`, options?.signal, async (abortSignal) => {
      await blob.upload(data, data.byteLength, {
        abortSignal,
        blobHTTPHeaders: {
          blobContentType: metadata.contentType,
          blobContentEncoding: metadata.contentEncoding ?? undefined
        }
      });
    });
  }

  async get(
    path: string,
    options?: ObjectStorageOperationOptions
  ): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }> {
    const blob = this.container.getBlobClient(this.fullPath(path));
    return this.run(`download for ${path}`, options?.signal, async (abortSignal) => {
      const response = await blob.download(0, undefined, { abortSignal });
      const length = response.contentLength;
      const stream = response.readableStreamBody;
      if (length == null || !Number.isSafeInteger(length) || length < 0 || stream == null) {
        stream?.destroy();
        throw new Error('Azure download response has invalid ContentLength or is missing its body');
      }
      // Preallocate once and validate the complete body, just as the BSON reader
      // expects. The deadline remains active after response headers arrive.
      const abort = () => stream.destroy(abortSignal.reason);
      abortSignal.addEventListener('abort', abort, { once: true });
      try {
        abortSignal.throwIfAborted();
        const data = new Uint8Array(length);
        let offset = 0;
        for await (const chunk of stream) {
          abortSignal.throwIfAborted();
          if (!(chunk instanceof Uint8Array) || offset + chunk.byteLength > length) {
            throw new Error('Azure download ContentLength mismatch');
          }
          data.set(chunk, offset);
          offset += chunk.byteLength;
        }
        if (offset !== length) {
          throw new Error(`Azure download ContentLength mismatch: expected ${length} bytes, received ${offset}`);
        }
        return {
          data,
          metadata: {
            contentType: response.contentType ?? 'application/octet-stream',
            contentEncoding: response.contentEncoding || null
          }
        };
      } finally {
        abortSignal.removeEventListener('abort', abort);
        stream.destroy();
      }
    });
  }

  async *list(prefix: string, options?: ObjectStorageOperationOptions): AsyncIterable<string> {
    const fullPrefix = this.fullPath(prefix);
    let continuationToken: string | undefined;
    do {
      const page = await this.listPage(fullPrefix, continuationToken, options?.signal);
      for (const blob of page.segment.blobItems) {
        options?.signal?.throwIfAborted();
        yield this.prefix ? blob.name.slice(this.prefix.length + 1) : blob.name;
      }
      continuationToken = page.continuationToken;
    } while (continuationToken);
  }

  async delete(paths: string[], options?: ObjectStorageOperationOptions): Promise<void> {
    // Validate all names before performing any mutations.
    const blobs = paths.map((path) => this.container.getBlobClient(this.fullPath(path)));
    for (let offset = 0; offset < blobs.length; offset += DELETE_BATCH_SIZE) {
      const batchBlobs = blobs.slice(offset, offset + DELETE_BATCH_SIZE);
      await this.run('delete', options?.signal, async (abortSignal) => {
        const client = this.container.getBlobBatchClient();
        const batch = client.createBatch();
        for (const blob of batchBlobs) {
          abortSignal.throwIfAborted();
          await batch.deleteBlob(blob, { abortSignal });
        }
        // deleteBlobs() only forwards options to the subrequests. Use submitBatch
        // explicitly so cancellation also reaches the outer HTTP request.
        const response = await client.submitBatch(batch, { abortSignal });
        const failures = response.subResponses.filter(
          (result) => result.status >= 300 && !(result.status === 404 && result.errorCode === 'BlobNotFound')
        );
        if (response.subResponses.length !== batchBlobs.length) {
          throw new ObjectStorageError('Azure batch delete returned an incomplete response', {
            cause: response,
            retryable: true
          });
        }
        if (failures.length) {
          throw new ObjectStorageError(
            `Azure batch delete failed: ${failures.map((r) => `${r.status} ${r.errorCode ?? ''}`).join(', ')}`,
            { cause: failures, retryable: failures.every((r) => isTransientAzureError({ statusCode: r.status })) }
          );
        }
      });
    }
    options?.signal?.throwIfAborted();
  }

  async deletePrefix(prefix: string, options?: ObjectStorageOperationOptions): Promise<{ objectCount: number }> {
    let objectCount = 0;
    let batch: string[] = [];
    for await (const path of this.list(prefix, options)) {
      batch.push(path);
      if (batch.length === DELETE_BATCH_SIZE) {
        await this.delete(batch, options);
        objectCount += batch.length;
        batch = [];
      }
    }
    await this.delete(batch, options);
    return { objectCount: objectCount + batch.length };
  }

  private async listPage(
    prefix: string,
    continuationToken: string | undefined,
    signal?: AbortSignal
  ): Promise<ContainerListBlobFlatSegmentResponse> {
    return this.run('list', signal, async (abortSignal) => {
      const pages = this.container.listBlobsFlat({ prefix, abortSignal }).byPage({
        continuationToken,
        maxPageSize: DELETE_BATCH_SIZE
      });
      const page = await pages.next();
      if (page.done) {
        throw new Error('Azure listing returned no page');
      }
      return page.value;
    });
  }

  private fullPath(path: string): string {
    const name = this.prefix ? `${this.prefix}/${path}` : path;
    if (Buffer.byteLength(name, 'utf8') > SAFE_KEY_BYTES) {
      throw new Error('Azure object key exceeds the portable limit of 896 UTF-8 bytes');
    }
    return name;
  }

  private async run<T>(
    operation: string,
    signal: AbortSignal | undefined,
    action: (signal: AbortSignal) => Promise<T>
  ): Promise<T> {
    signal?.throwIfAborted();
    // Bound queueing independently: a request gets its full operation budget once
    // admitted, while shutdown can cancel either stage immediately.
    const queue = this.deadline(operation, 'waiting for a concurrency slot');
    let acquired;
    try {
      acquired = await acquireSemaphoreAbortable(
        this.semaphore,
        signal ? AbortSignal.any([signal, queue.signal]) : queue.signal
      );
    } finally {
      queue.cancel();
    }
    if (acquired === 'aborted') {
      signal?.throwIfAborted();
      throw queue.signal.reason;
    }
    const [, release] = acquired;
    const deadline = this.deadline(operation, 'performing the operation');
    const combined = signal ? AbortSignal.any([signal, deadline.signal]) : deadline.signal;
    try {
      combined.throwIfAborted();
      const result = await action(combined);
      combined.throwIfAborted();
      return result;
    } catch (cause) {
      if (combined.aborted) throw combined.reason;
      if (cause instanceof ObjectStorageError || isAbortError(cause)) throw cause;
      // Do not include endpoint URLs or SDK messages: connection strings may carry SAS credentials.
      throw new ObjectStorageError(`Azure ${operation} failed`, { cause, retryable: isTransientAzureError(cause) });
    } finally {
      deadline.cancel();
      release();
    }
  }

  private deadline(operation: string, stage: string): { signal: AbortSignal; cancel: () => void } {
    const controller = new AbortController();
    const timer = setTimeout(() => {
      controller.abort(
        new ObjectStorageError(`Azure ${operation} timed out ${stage} after ${this.operationTimeoutMs} ms`, {
          cause: new DOMException('Azure object storage deadline exceeded', 'TimeoutError'),
          retryable: true
        })
      );
    }, this.operationTimeoutMs);
    timer.unref();
    return { signal: controller.signal, cancel: () => clearTimeout(timer) };
  }
}

function isTransientAzureError(error: unknown): boolean {
  if (typeof error !== 'object' || error == null || isAbortError(error)) return false;
  const { statusCode, code } = error as { statusCode?: number; code?: string };
  return (
    statusCode === 408 ||
    statusCode === 429 ||
    (statusCode != null && statusCode >= 500) ||
    ['ETIMEDOUT', 'ESOCKETTIMEDOUT', 'ECONNRESET', 'ECONNREFUSED', 'EAI_AGAIN', 'REQUEST_SEND_ERROR'].includes(
      code ?? ''
    )
  );
}
