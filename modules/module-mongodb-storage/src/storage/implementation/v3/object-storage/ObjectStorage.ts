export interface ObjectStoragePutMetadata {
  contentType: string;
  contentEncoding: string | null;
}

export class ObjectStorageError extends Error {
  readonly retryable: boolean;

  constructor(message: string, options: { cause: unknown; retryable: boolean }) {
    super(message, { cause: options.cause });
    this.name = 'ObjectStorageError';
    this.retryable = options.retryable;
  }
}

export function isRetryableObjectStorageError(error: unknown): error is ObjectStorageError {
  return error instanceof ObjectStorageError && error.retryable;
}

export interface ObjectStorageOperationOptions {
  /**
   * Cancels the operation, including while it waits for a concurrency slot.
   *
   * Operations are bounded by their own timeouts regardless, but a signal releases the slot as
   * soon as the caller no longer needs the result.
   */
  signal?: AbortSignal;
}

export interface ObjectStorage {
  put(
    path: string,
    data: Uint8Array,
    metadata: ObjectStoragePutMetadata,
    options?: ObjectStorageOperationOptions
  ): Promise<void>;
  get(
    path: string,
    options?: ObjectStorageOperationOptions
  ): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }>;
  list(prefix: string, options?: ObjectStorageOperationOptions): AsyncIterable<string>;
  delete(paths: string[], options?: ObjectStorageOperationOptions): Promise<void>;
  deletePrefix(prefix: string, options?: ObjectStorageOperationOptions): Promise<{ objectCount: number }>;
}
