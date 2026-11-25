import { AzureBlobObjectStorage } from '@module/storage/implementation/v3/object-storage/AzureBlobObjectStorage.js';
import { ObjectStorage } from '@module/storage/implementation/v3/object-storage/ObjectStorage.js';
import { S3ObjectStorage } from '@module/storage/implementation/v3/object-storage/S3ObjectStorage.js';
import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { randomUUID } from 'node:crypto';
import { MemoryObjectStorage } from './MemoryObjectStorage.js';

const remoteStorages: ObjectStorage[] = [];

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
 * Creates an ObjectStorage instance for the shared object-storage integration tests.
 * Set RUSTFS_ENDPOINT to switch all S3 tests from MemoryObjectStorage
 * to a real RustFS/S3 endpoint.
 *   RUSTFS_ENDPOINT=http://localhost:9000
 * Set AZURITE_CONNECTION_STRING to run the same tests against Azure Blob Storage instead.
 */
export function createS3TestStorageSuite(options: S3TestFactoryOptions) {
  const rustfsEndpoint = process.env.RUSTFS_ENDPOINT;
  const azureConnectionString = process.env.AZURITE_CONNECTION_STRING;
  if (rustfsEndpoint && azureConnectionString) {
    throw new Error('Select only one of RUSTFS_ENDPOINT and AZURITE_CONNECTION_STRING');
  }
  let objectStorage: ObjectStorage;
  if (azureConnectionString) {
    objectStorage = new AzureBlobObjectStorage({
      container: 'powersync-object-storage-test',
      connectionString: azureConnectionString,
      prefix: `test-${process.pid}-${randomUUID()}`
    });
    remoteStorages.push(objectStorage);
  } else if (rustfsEndpoint) {
    const s3 = new S3ObjectStorage({
      bucket: 'powersync-s3-test',
      region: 'us-east-1',
      prefix: `test-${process.pid}-${randomUUID()}`,
      endpoint: rustfsEndpoint,
      forcePathStyle: true,
      accessKeyId: process.env.RUSTFS_ACCESS_KEY ?? 'rustfsadmin',
      secretAccessKey: process.env.RUSTFS_SECRET_KEY ?? 'rustfsadmin'
    });
    remoteStorages.push(s3);
    objectStorage = s3;
  } else {
    objectStorage = new MemoryObjectStorage();
  }

  return createTestStorageSuite(options, objectStorage);
}

/** Remove all objects created by remote-storage suites in the current test. */
export async function cleanupS3TestStorage(): Promise<void> {
  for (const storage of remoteStorages.splice(0)) {
    await storage.deletePrefix('bucket-data/');
  }
}

/** Provision the emulator's test container before tests create storage suites. */
export async function initializeAzureTestStorage(): Promise<void> {
  if (process.env.AZURITE_CONNECTION_STRING) {
    const storage = new AzureBlobObjectStorage({
      container: 'powersync-object-storage-test',
      connectionString: process.env.AZURITE_CONNECTION_STRING
    });
    await storage.container.createIfNotExists();
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
