import { mongo } from '@powersync/lib-service-mongodb';
import { framework, PowerSyncMigrationManager, TestStorageOptions } from '@powersync/service-core';
import { MongoMigrationAgent } from '../migrations/MongoMigrationAgent.js';
import { MongoBucketStorage, MongoBucketStorageOptions } from '../storage/MongoBucketStorage.js';
import { MongoReportStorage } from '../storage/MongoReportStorage.js';
import { PowerSyncMongo } from '../storage/implementation/db.js';

export type MongoTestStorageOptions = {
  url: string;
  isCI: boolean;
  clientOptions?: mongo.MongoClientOptions;
  runMigrations?: boolean;
  internalOptions?: MongoBucketStorageOptions;
} & Omit<MongoBucketStorageOptions, 'replicationStreamNamePrefix'>;

export function mongoTestStorageFactoryGenerator(factoryOptions: MongoTestStorageOptions) {
  return {
    factory: async (options?: TestStorageOptions) => {
      if (factoryOptions.runMigrations) {
        await runMongoMigrations(factoryOptions.url);
      }

      const db = connectMongoForTests(factoryOptions.url, factoryOptions.isCI, factoryOptions.clientOptions);

      // None of the tests insert data into this collection, so it was never created
      if (!(await db.db.listCollections({ name: db.bucket_parameters.collectionName }).hasNext())) {
        await db.db.createCollection('bucket_parameters');
      }

      // Full migrations are not currently run for tests, so we manually create this
      await db.createCheckpointEventsCollection();

      if (!options?.doNotClear) {
        await db.clear();
      }

      return new MongoBucketStorage(db, {
        replicationStreamNamePrefix: 'test_',
        checksumOptions: factoryOptions.checksumOptions,
        supportsMultipleSyncConfigs: factoryOptions.supportsMultipleSyncConfigs
      });
    },
    tableIdStrings: false
  };
}

export function mongoTestReportStorageFactoryGenerator(factoryOptions: MongoTestStorageOptions) {
  return async (options?: TestStorageOptions) => {
    if (factoryOptions.runMigrations) {
      await runMongoMigrations(factoryOptions.url);
    }

    const db = connectMongoForTests(factoryOptions.url, factoryOptions.isCI, factoryOptions.clientOptions);

    await db.createConnectionReportingCollection();

    if (!options?.doNotClear) {
      await db.clear();
    }

    return new MongoReportStorage(db);
  };
}

export const connectMongoForTests = (url: string, isCI: boolean, options: mongo.MongoClientOptions = {}) => {
  // Short timeout for tests, to fail fast when the server is not available.
  // Slightly longer timeouts for CI, to avoid arbitrary test failures
  const client = new mongo.MongoClient(url, {
    connectTimeoutMS: isCI ? 15_000 : 5_000,
    socketTimeoutMS: isCI ? 15_000 : 5_000,
    serverSelectionTimeoutMS: isCI ? 15_000 : 2_500,
    ...options
  });
  return new PowerSyncMongo(client);
};

async function runMongoMigrations(url: string) {
  await using migrationManager: PowerSyncMigrationManager = new framework.migrations.MigrationManager();
  await using migrationAgent = new MongoMigrationAgent({ type: 'mongodb', uri: url });

  migrationManager.registerMigrationAgent(migrationAgent);
  await migrationManager.migrate({
    direction: framework.migrations.Direction.Up
  });
}
