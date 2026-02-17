import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  CONVEX_URL: utils.type.string.default('http://127.0.0.1:3210'),
  CONVEX_DEPLOY_KEY: utils.type.string.default(''),
  MONGO_TEST_URL: utils.type.string.default('mongodb://127.0.0.1:27017/powersync_test?directConnection=true'),
  CI: utils.type.boolean.default('false'),
  SLOW_TESTS: utils.type.boolean.default('false')
});
