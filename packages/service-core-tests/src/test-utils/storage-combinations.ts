import {
  STORAGE_VERSION_1,
  STORAGE_VERSION_2,
  STORAGE_VERSION_4,
  TestStorageConfig,
  TestStorageFactory
} from '@powersync/service-core';
import { describe, TestOptions } from 'vitest';

const MONGO_STORAGE_VERSIONS = [STORAGE_VERSION_1, STORAGE_VERSION_2, STORAGE_VERSION_4];
const POSTGRES_STORAGE_VERSIONS = [STORAGE_VERSION_1, STORAGE_VERSION_2];

export interface StorageVersionTestContext {
  factory: TestStorageFactory;
  storageVersion: number;
}

export interface StorageCombinationTestConfig {
  mongodb?: TestStorageConfig;
  postgres?: TestStorageConfig;
}

export type StorageCombinationTestOptions = TestOptions & {
  /** Omit to test all supported versions, or use 'current' for each backend's default. */
  storageVersions?: number[] | 'current';
};

export function describeStorageCombinations(
  config: StorageCombinationTestConfig,
  options: StorageCombinationTestOptions,
  fn: (context: StorageVersionTestContext) => void
) {
  const describeFactory = (
    storageName: string,
    storage: TestStorageConfig,
    supportedStorageVersions: number[],
    currentStorageVersion: number
  ) => {
    const requestedVersions = options.storageVersions === 'current' ? [currentStorageVersion] : options.storageVersions;
    const storageVersions = (requestedVersions ?? supportedStorageVersions).filter((version) =>
      supportedStorageVersions.includes(version)
    );
    describe(`${storageName} storage`, options, function () {
      for (const storageVersion of storageVersions) {
        describe(`storage v${storageVersion}`, function () {
          fn({
            factory: storage.factory,
            storageVersion
          });
        });
      }
    });
  };

  if (config.mongodb) {
    describeFactory('mongodb', config.mongodb, MONGO_STORAGE_VERSIONS, STORAGE_VERSION_4);
  }

  if (config.postgres) {
    describeFactory('postgres', config.postgres, POSTGRES_STORAGE_VERSIONS, STORAGE_VERSION_2);
  }
}
