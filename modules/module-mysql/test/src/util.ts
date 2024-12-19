import * as types from '@module/types/types.js';
import { getMySQLVersion, isVersionAtLeast } from '@module/utils/mysql-utils.js';
import { BucketStorageFactory, Metrics } from '@powersync/service-core';
import * as mongo_module from '@powersync/service-module-mongodb';
import mysqlPromise from 'mysql2/promise';
import { env } from './env.js';

export const TEST_URI = env.MYSQL_TEST_URI;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mysql',
  uri: TEST_URI
});

// The metrics need to be initialized before they can be used
await Metrics.initialise({
  disable_telemetry_sharing: true,
  powersync_instance_id: 'test',
  internal_metrics_endpoint: 'unused.for.tests.com'
});
Metrics.getInstance().resetCounters();

export type StorageFactory = () => Promise<BucketStorageFactory>;

export const INITIALIZED_MONGO_STORAGE_FACTORY: StorageFactory = async () => {
  const db = await connectMongo();

  // None of the tests insert data into this collection, so it was never created
  if (!(await db.db.listCollections({ name: db.bucket_parameters.collectionName }).hasNext())) {
    await db.db.createCollection('bucket_parameters');
  }

  await db.clear();

  return new mongo_module.storage.MongoBucketStorage(db, { slot_name_prefix: 'test_' });
};

export async function connectMongo() {
  // Short timeout for tests, to fail fast when the server is not available.
  // Slightly longer timeouts for CI, to avoid arbitrary test failures
  const client =  mongo_module.storage.createMongoClient(env.MONGO_TEST_URL, {
    connectTimeoutMS: env.CI ? 15_000 : 5_000,
    socketTimeoutMS: env.CI ? 15_000 : 5_000,
    serverSelectionTimeoutMS: env.CI ? 15_000 : 2_500
  });
  return new mongo_module.storage.PowerSyncMongo(client);
}

export async function clearTestDb(connection: mysqlPromise.Connection) {
  const version = await getMySQLVersion(connection);
  if (isVersionAtLeast(version, '8.4.0')) {
    await connection.query('RESET BINARY LOGS AND GTIDS');
  } else {
    await connection.query('RESET MASTER');
  }

  const [result] = await connection.query<mysqlPromise.RowDataPacket[]>(
    `SELECT TABLE_NAME FROM information_schema.tables
     WHERE TABLE_SCHEMA = '${TEST_CONNECTION_OPTIONS.database}'`
  );
  for (let row of result) {
    const name = row.TABLE_NAME;
    if (name.startsWith('test_')) {
      await connection.query(`DROP TABLE ${name}`);
    }
  }
}
