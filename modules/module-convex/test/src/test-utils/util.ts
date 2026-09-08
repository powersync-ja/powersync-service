import * as types from '@module/types/types.js';
import { api } from '@testing-convex/_generated/api.js';
import { ConvexHttpClient } from 'convex/browser';

import { TestStorageConfig, TestStorageFactory } from '@powersync/service-core';
import { describeStorageCombinations } from '@powersync/service-core-tests';
import { TestOptions } from 'vitest';
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

export interface StorageVersionTestContext {
  factory: TestStorageFactory;
  storageVersion: number;
}

export function describeWithStorage(
  options: TestOptions & { storageVersions?: number[] },
  fn: (context: StorageVersionTestContext) => void
) {
  describeStorageCombinations(
    {
      mongodb: env.TEST_MONGO_STORAGE ? INITIALIZED_MONGO_STORAGE_FACTORY : undefined,
      postgres: env.TEST_POSTGRES_STORAGE ? INITIALIZED_POSTGRES_STORAGE_FACTORY : undefined
    },
    options,
    fn
  );
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
