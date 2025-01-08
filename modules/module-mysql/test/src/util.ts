import * as types from '@module/types/types.js';
import { getMySQLVersion, isVersionAtLeast } from '@module/utils/mysql-utils.js';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import mysqlPromise from 'mysql2/promise';
import { env } from './env.js';

export const TEST_URI = env.MYSQL_TEST_URI;

export const TEST_CONNECTION_OPTIONS = types.normalizeConnectionConfig({
  type: 'mysql',
  uri: TEST_URI
});

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongo_storage.MongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

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
