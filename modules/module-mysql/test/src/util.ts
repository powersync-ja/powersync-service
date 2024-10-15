import * as types from '@module/types/types.js';
import { BucketStorageFactory, Metrics, MongoBucketStorage } from '@powersync/service-core';
import { env } from './env.js';
import mysqlPromise from 'mysql2/promise';
import { connectMongo } from '@core-tests/util.js';
import { getMySQLVersion } from '@module/common/check-source-configuration.js';
import { gte } from 'semver';
import { RowDataPacket } from 'mysql2';

// The metrics need to be initialized before they can be used
await Metrics.initialise({
  disable_telemetry_sharing: true,
  powersync_instance_id: 'test',
  internal_metrics_endpoint: 'unused.for.tests.com'
});
Metrics.getInstance().resetCounters();

export const TEST_URI = env.MYSQL_TEST_URI;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mysql',
  uri: TEST_URI
});

export type StorageFactory = () => Promise<BucketStorageFactory>;

export const INITIALIZED_MONGO_STORAGE_FACTORY: StorageFactory = async () => {
  const db = await connectMongo();

  // None of the tests insert data into this collection, so it was never created
  if (!(await db.db.listCollections({ name: db.bucket_parameters.collectionName }).hasNext())) {
    await db.db.createCollection('bucket_parameters');
  }

  await db.clear();

  return new MongoBucketStorage(db, { slot_name_prefix: 'test_' });
};

export async function clearAndRecreateTestDb(connection: mysqlPromise.Connection) {
  const version = await getMySQLVersion(connection);
  if (gte(version, '8.4.0')) {
    await connection.query('RESET BINARY LOGS AND GTIDS');
  } else {
    await connection.query('RESET MASTER');
  }

  // await connection.query(`DROP DATABASE IF EXISTS ${TEST_CONNECTION_OPTIONS.database}`);
  //
  // await connection.query(`CREATE DATABASE IF NOT EXISTS ${TEST_CONNECTION_OPTIONS.database}`);

  const [result] = await connection.query<RowDataPacket[]>(
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
