import * as types from '@module/types/types.js';
import { api } from '@testing-convex/_generated/api.js';
import { ConvexHttpClient } from 'convex/browser';

import { SUPPORTED_STORAGE_VERSIONS, TestStorageConfig, TestStorageFactory } from '@powersync/service-core';
import { describe, TestOptions } from 'vitest';
import { env } from '../env.js';

export type TestConvexConnection = {
  client: ConvexHttpClient;
  api: typeof api;
};

export const TEST_URI = env.CONVEX_URL;

export const INITIALIZED_MONGO_STORAGE_FACTORY: TestStorageConfig = {
  tableIdStrings: false,
  factory: async (options) => {
    const mongo_storage = await import('@powersync/service-module-mongodb-storage');
    const config = mongo_storage.test_utils.mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI
    });
    return config.factory(options);
  }
};

export const INITIALIZED_POSTGRES_STORAGE_FACTORY: TestStorageConfig = {
  tableIdStrings: true,
  factory: async (options) => {
    const postgres_storage = await import('@powersync/service-module-postgres-storage');
    const config = postgres_storage.test_utils.postgresTestSetup({
      url: env.PG_STORAGE_TEST_URL
    });
    return config.factory(options);
  }
};

const TEST_STORAGE_VERSIONS = SUPPORTED_STORAGE_VERSIONS;

export interface StorageVersionTestContext {
  factory: TestStorageFactory;
  storageVersion: number;
}

export function describeWithStorage(
  options: TestOptions & { storageVersions?: number[] },
  fn: (context: StorageVersionTestContext) => void
) {
  const storageVersions = options.storageVersions ?? TEST_STORAGE_VERSIONS;
  const describeFactory = (storageName: string, config: TestStorageConfig) => {
    describe(`${storageName} storage`, options, function () {
      for (const storageVersion of storageVersions) {
        describe(`storage v${storageVersion}`, function () {
          fn({
            factory: config.factory,
            storageVersion
          });
        });
      }
    });
  };

  if (env.TEST_MONGO_STORAGE) {
    describeFactory('mongodb', INITIALIZED_MONGO_STORAGE_FACTORY);
  }

  if (env.TEST_POSTGRES_STORAGE) {
    describeFactory('postgres', INITIALIZED_POSTGRES_STORAGE_FACTORY);
  }
}

export const RAW_TEST_CONNECTION_OPTIONS: types.ConvexConnectionConfig = {
  type: 'convex',
  deploy_key: env.CONVEX_DEPLOY_KEY,
  deployment_url: env.CONVEX_URL
} as const;

export const TEST_CONNECTION_OPTIONS = types.resolveConvexConnectionConfig(RAW_TEST_CONNECTION_OPTIONS);

export function connectConvex(): TestConvexConnection {
  return {
    client: new ConvexHttpClient(env.CONVEX_URL),
    api
  };
}

export async function clearTestDb(connection: TestConvexConnection) {
  const { api, client } = connection;

  // Delete all lists
  let deletedCount = 0;
  console.info(`Clearing Convex DB`);
  do {
    deletedCount = await client.mutation(api.lists.deleteBatch, {});
    console.info(`Cleared ${deletedCount} lists`);
  } while (deletedCount > 0);

  deletedCount = 0;
  do {
    deletedCount = await client.mutation(api.todos.deleteBatch, {});
    console.info(`Cleared ${deletedCount} todos`);
  } while (deletedCount > 0);
}
