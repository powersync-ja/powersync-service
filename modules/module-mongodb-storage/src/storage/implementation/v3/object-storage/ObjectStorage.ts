export interface ObjectStoragePutMetadata {
  contentType: string;
  contentEncoding: string | null;
}

export interface ObjectStorage {
  put(path: string, data: Uint8Array, metadata?: ObjectStoragePutMetadata): Promise<void>;
  get(path: string, signal?: AbortSignal): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }>;
  list(prefix: string, signal?: AbortSignal): AsyncIterable<string>;
  delete(paths: string[]): Promise<void>;
}
