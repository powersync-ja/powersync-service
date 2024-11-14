import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  MYSQL_TEST_URI: utils.type.string.default('mysql://root:mypassword@localhost:3306/mydatabase'),
  CI: utils.type.boolean.default('false'),
  SLOW_TESTS: utils.type.boolean.default('false')
});
