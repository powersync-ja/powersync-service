import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { ObjectStorage } from '@module/storage/implementation/v3/object-storage/ObjectStorage.js';
import { S3ObjectStorage } from '@module/storage/implementation/v3/object-storage/S3ObjectStorage.js';
import { MemoryObjectStorage } from './MemoryObjectStorage.js';

export interface S3TestFactoryOptions {
  url: string;
  isCI: boolean;
  inlineThresholdBytes?: number;
}

/**
 * Creates an ObjectStorage instance for S3 tests.
 * Set MINIO_ENDPOINT to switch all S3 tests from MemoryObjectStorage
 * to a real MinIO/S3 endpoint.
 *   MINIO_ENDPOINT=http://localhost:9000
 */
export function createS3TestStorageSuite(options: S3TestFactoryOptions) {
  const minioEndpoint = process.env.MINIO_ENDPOINT;
  const objectStorage: ObjectStorage = minioEndpoint
    ? new S3ObjectStorage({
        bucket: 'powersync-s3-test',
        region: 'us-east-1',
        endpoint: minioEndpoint,
        accessKeyId: process.env.MINIO_ACCESS_KEY ?? 'minioadmin',
        secretAccessKey: process.env.MINIO_SECRET_KEY ?? 'minioadmin'
      })
    : new MemoryObjectStorage();

  return {
    objectStorage,
    factoryGen: mongoTestStorageFactoryGenerator({
      url: options.url,
      isCI: options.isCI,
      internalOptions: {
        objectStorage,
        inlineThresholdBytes: options.inlineThresholdBytes ?? 0
      }
    })
  };
}
