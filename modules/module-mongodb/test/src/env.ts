import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  MONGO_TEST_URL: utils.type.string.default('mongodb://localhost:27017/powersync_test'),
  MONGO_TEST_DATA_URL: utils.type.string.default('mongodb://localhost:27017/powersync_test_data'),
  CI: utils.type.boolean.default('false'),
  SLOW_TESTS: utils.type.boolean.default('false')
});
