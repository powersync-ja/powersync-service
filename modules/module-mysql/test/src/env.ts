import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  MYSQL_TEST_URI: utils.type.string.default('mysql://root:mypassword@localhost:3306/mydatabase'),
  MONGO_TEST_URL: utils.type.string.default('mongodb://localhost:27017/powersync_test'),
  CI: utils.type.boolean.default('false'),
  SLOW_TESTS: utils.type.boolean.default('false')
});
