export interface ObjectStoragePutMetadata {
  contentType: string;
  contentEncoding: string | null;
}

export interface ObjectStorage {
  put(path: string, data: Uint8Array, metadata?: ObjectStoragePutMetadata): Promise<void>;
  get(path: string): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }>;
  delete(paths: string[]): Promise<void>;
}
