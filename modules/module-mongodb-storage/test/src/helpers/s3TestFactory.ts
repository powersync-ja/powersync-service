import { ObjectStorage } from '@module/storage/implementation/v3/object-storage/ObjectStorage.js';
import { S3ObjectStorage } from '@module/storage/implementation/v3/object-storage/S3ObjectStorage.js';
import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { randomUUID } from 'node:crypto';
import { MemoryObjectStorage } from './MemoryObjectStorage.js';

const rustfsStorages: S3ObjectStorage[] = [];

export interface S3TestFactoryOptions {
  url: string;
  isCI: boolean;
  inlineThresholdBytes?: number;
}

function createTestStorageSuite(options: S3TestFactoryOptions, objectStorage: ObjectStorage) {
  return {
    objectStorage,
    factoryGen: mongoTestStorageFactoryGenerator({
      url: options.url,
      isCI: options.isCI,
      objectStorage,
      inlineThresholdBytes: options.inlineThresholdBytes ?? 0
    })
  };
}

/**
 * Creates an ObjectStorage instance for S3 tests.
 * Set RUSTFS_ENDPOINT to switch all S3 tests from MemoryObjectStorage
 * to a real RustFS/S3 endpoint.
 *   RUSTFS_ENDPOINT=http://localhost:9000
 */
export function createS3TestStorageSuite(options: S3TestFactoryOptions) {
  const rustfsEndpoint = process.env.RUSTFS_ENDPOINT;
  let objectStorage: ObjectStorage;
  if (rustfsEndpoint) {
    const s3 = new S3ObjectStorage({
      bucket: 'powersync-s3-test',
      region: 'us-east-1',
      prefix: `test-${process.pid}-${randomUUID()}`,
      endpoint: rustfsEndpoint,
      forcePathStyle: true,
      accessKeyId: process.env.RUSTFS_ACCESS_KEY ?? 'rustfsadmin',
      secretAccessKey: process.env.RUSTFS_SECRET_KEY ?? 'rustfsadmin'
    });
    rustfsStorages.push(s3);
    objectStorage = s3;
  } else {
    objectStorage = new MemoryObjectStorage();
  }

  return createTestStorageSuite(options, objectStorage);
}

/** Remove all objects created by RustFS-backed suites in the current test. */
export async function cleanupS3TestStorage(): Promise<void> {
  for (const storage of rustfsStorages.splice(0)) {
    await storage.deletePrefix('bucket-data/');
  }
}

/** Creates an explicitly memory-backed suite for tests that inspect stored objects. */
export function createMemoryS3TestStorageSuite(options: S3TestFactoryOptions) {
  const objectStorage = new MemoryObjectStorage();
  return {
    ...createTestStorageSuite(options, objectStorage),
    objectStorage
  };
}
