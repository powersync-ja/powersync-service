export interface ObjectStoragePutMetadata {
  contentType: string;
  contentEncoding: string;
}

export interface ObjectStorage {
  put(path: string, data: Buffer, metadata?: ObjectStoragePutMetadata): Promise<void>;
  get(path: string): Promise<Buffer>;
  delete(paths: string[]): Promise<void>;
}
