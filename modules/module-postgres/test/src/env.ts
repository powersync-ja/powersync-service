import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  PG_TEST_URL: utils.type.string.default('postgres://postgres:postgres@localhost:5432/powersync_test'),
  PG_STORAGE_TEST_URL: utils.type.string.default('postgres://postgres:postgres@localhost:5431/powersync_storage_test'),
  MONGO_TEST_URL: utils.type.string.default('mongodb://localhost:27017/powersync_test'),
  CI: utils.type.boolean.default('false'),
  SLOW_TESTS: utils.type.boolean.default('false'),
  TEST_MONGO_STORAGE: utils.type.boolean.default('true'),
  TEST_POSTGRES_STORAGE: utils.type.boolean.default('true')
});
