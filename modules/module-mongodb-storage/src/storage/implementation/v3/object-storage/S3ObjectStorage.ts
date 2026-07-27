import { DeleteObjectsCommand, GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import type { ObjectStorage, ObjectStoragePutMetadata } from './ObjectStorage.js';

export interface S3ObjectStorageOptions {
  bucket: string;
  region: string;
  prefix?: string;
  endpoint?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
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
      forcePathStyle: !!options.endpoint,
      credentials:
        options.accessKeyId && options.secretAccessKey
          ? { accessKeyId: options.accessKeyId, secretAccessKey: options.secretAccessKey }
          : undefined
    });
  }

  async put(path: string, data: Uint8Array, metadata: ObjectStoragePutMetadata): Promise<void> {
    const fullPath = this.prefix ? `${this.prefix}/${path}` : path;
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: fullPath,
        Body: data,
        ContentType: metadata.contentType,
        ContentEncoding: metadata.contentEncoding ?? undefined
      })
    );
  }

  async get(path: string): Promise<{ data: Uint8Array; metadata: ObjectStoragePutMetadata }> {
    const fullPath = this.prefix ? `${this.prefix}/${path}` : path;
    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: fullPath
        })
      );
      const chunks: Uint8Array[] = [];
      const stream = response.Body as AsyncIterable<Uint8Array>;
      for await (const chunk of stream) {
        chunks.push(chunk);
      }
      return {
        data: mergeChunks(chunks),
        metadata: {
          contentType: response.ContentType ?? 'application/octet-stream',
          contentEncoding: response.ContentEncoding ?? null
        }
      };
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

function mergeChunks(chunks: Uint8Array[]): Uint8Array {
  const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}
