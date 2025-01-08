import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  PG_STORAGE_TEST_URL: utils.type.string.default('postgres://postgres:postgres@localhost:5431/powersync_storage_test'),
  CI: utils.type.boolean.default('false')
});
