import * as types from '@module/types/types.js';
import { api } from '@testing-convex/_generated/api.js';
import { ConvexHttpClient } from 'convex/browser';

import { SUPPORTED_STORAGE_VERSIONS, TestStorageConfig, TestStorageFactory } from '@powersync/service-core';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';
import { describe, TestOptions } from 'vitest';
import { env } from '../env.js';

export type TestConvexConnection = {
  client: ConvexHttpClient;
  api: typeof api;
};

export const TEST_URI = env.CONVEX_URL;

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongo_storage.test_utils.mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export const INITIALIZED_POSTGRES_STORAGE_FACTORY = postgres_storage.test_utils.postgresTestSetup({
  url: env.PG_STORAGE_TEST_URL
});

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

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig(RAW_TEST_CONNECTION_OPTIONS);

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
  do {
    deletedCount = await client.mutation(api.lists.deleteBatch, {});
  } while (deletedCount > 0);

  deletedCount = 0;
  do {
    deletedCount = await client.mutation(api.todos.deleteBatch, {});
  } while (deletedCount > 0);
}

// export async function getClientCheckpoint(
//   db: pgwire.PgClient,
//   storageFactory: BucketStorageFactory,
//   options?: { timeout?: number }
// ): Promise<InternalOpId> {
//   const start = Date.now();

//   const api = new PostgresRouteAPIAdapter(db);
//   const lsn = await api.createReplicationHead(async (lsn) => lsn);

//   // This old API needs a persisted checkpoint id.
//   // Since we don't use LSNs anymore, the only way to get that is to wait.

//   const timeout = options?.timeout ?? 50_000;

//   logger.info(`Waiting for LSN checkpoint: ${lsn}`);
//   while (Date.now() - start < timeout) {
//     const storage = await storageFactory.getActiveStorage();
//     const cp = await storage?.getCheckpoint();

//     if (cp?.lsn != null && cp.lsn >= lsn) {
//       logger.info(`Got write checkpoint: ${lsn} : ${cp.checkpoint}`);
//       return cp.checkpoint;
//     }

//     await new Promise((resolve) => setTimeout(resolve, 5));
//   }

//   throw new Error('Timeout while waiting for checkpoint');
// }
