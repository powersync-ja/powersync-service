import { DeleteObjectsCommand, GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import type { ObjectStorage, ObjectStoragePutMetadata } from './ObjectStorage.js';

export interface S3ObjectStorageOptions {
  bucket: string;
  region: string;
  prefix?: string;
  endpoint?: string;
}

export class S3ObjectStorage implements ObjectStorage {
  private client: S3Client;
  private bucket: string;
  private prefix: string;

  constructor(options: S3ObjectStorageOptions) {
    this.bucket = options.bucket;
    this.prefix = options.prefix ?? '';
    this.client = new S3Client({
      region: options.region,
      endpoint: options.endpoint,
      forcePathStyle: !!options.endpoint
    });
  }

  async put(path: string, data: Buffer, metadata?: ObjectStoragePutMetadata): Promise<void> {
    const fullPath = this.prefix ? `${this.prefix}/${path}` : path;
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: fullPath,
        Body: data,
        ContentType: metadata?.contentType,
        ContentEncoding: metadata?.contentEncoding
      })
    );
  }

  async get(path: string): Promise<Buffer> {
    const fullPath = this.prefix ? `${this.prefix}/${path}` : path;
    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: fullPath
        })
      );
      const chunks: Buffer[] = [];
      const stream = response.Body as any;
      for await (const chunk of stream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }
      return Buffer.concat(chunks);
    } catch (err: any) {
      if (err.name === 'NoSuchKey' || err.Code === 'NoSuchKey') {
        throw new Error(`S3 object not found: ${fullPath}`);
      }
      throw err;
    }
  }

  async delete(paths: string[]): Promise<void> {
    if (paths.length === 0) return;
    const fullPaths = paths.map((p) => ({ Key: this.prefix ? `${this.prefix}/${p}` : p }));
    await this.client.send(
      new DeleteObjectsCommand({
        Bucket: this.bucket,
        Delete: { Objects: fullPaths, Quiet: true }
      })
    );
  }
}
