export interface ObjectStorage {
  put(path: string, data: Buffer): Promise<void>;
  get(path: string): Promise<Buffer>;
  delete(paths: string[]): Promise<void>;
}
