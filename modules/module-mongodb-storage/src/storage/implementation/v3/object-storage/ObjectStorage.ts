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

export interface ObjectStorage {
  put(path: string, data: Uint8Array, metadata: ObjectStoragePutMetadata): Promise<void>;
  get(
    path: string,
    options?: { signal?: AbortSignal }
  ): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }>;
  list(prefix: string, options?: { signal?: AbortSignal }): AsyncIterable<string>;
  delete(paths: string[]): Promise<void>;
  deletePrefix(prefix: string, options?: { signal?: AbortSignal }): Promise<{ objectCount: number }>;
}
