import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  MIKROORM_MYSQL_STORAGE_TEST_URI: utils.type.string.default(
    process.env.MYSQL_TEST_URI ?? 'mysql://repl_user:good_password@localhost:3306/powersync'
  )
});
