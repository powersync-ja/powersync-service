import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  PG_TEST_URL: utils.type.string.default('postgres://postgres:postgres@localhost:5432/powersync_test'),
  MONGO_TEST_URL: utils.type.string.default('mongodb://localhost:27017/powersync_test'),
  CI: utils.type.boolean.default('false'),
  SLOW_TESTS: utils.type.boolean.default('false')
});
