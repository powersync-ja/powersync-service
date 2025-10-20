import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  MSSQL_TEST_URI: utils.type.string.default(`mssql://sa:321strong_ROOT_password@localhost:1433/powersync`),
  MONGO_TEST_URL: utils.type.string.default('mongodb://localhost:27017/powersync_test'),
  CI: utils.type.boolean.default('false'),
  PG_STORAGE_TEST_URL: utils.type.string.default('postgres://postgres:postgres@localhost:5431/powersync_storage_test'),
  TEST_MONGO_STORAGE: utils.type.boolean.default('true'),
  TEST_POSTGRES_STORAGE: utils.type.boolean.default('true')
});
