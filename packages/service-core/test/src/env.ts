import * as framework from '@powersync/service-framework';

export const env = framework.utils.collectEnvironmentVariables({
  MONGO_TEST_URL: framework.utils.type.string.default('mongodb://localhost:27017/powersync_test'),
  PG_TEST_URL: framework.utils.type.string.default('postgres://postgres:postgres@localhost:5432/powersync_test'),
  CI: framework.utils.type.boolean.default('false'),
  SLOW_TESTS: framework.utils.type.boolean.default('false')
});
