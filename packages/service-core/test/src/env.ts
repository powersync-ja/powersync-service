import * as micro from '@journeyapps-platform/micro';

export const env = micro.utils.collectEnvironmentVariables({
  MONGO_TEST_URL: micro.utils.type.string.default('mongodb://localhost:27017/powersync_test'),
  PG_TEST_URL: micro.utils.type.string.default('postgres://postgres:postgres@localhost:5432/powersync_test'),
  CI: micro.utils.type.boolean.default('false'),
  SLOW_TESTS: micro.utils.type.boolean.default('false')
});
