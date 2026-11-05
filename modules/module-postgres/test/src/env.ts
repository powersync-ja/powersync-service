import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  PG_TEST_URL: utils.type.string.default('postgres://postgres:postgres@localhost:5432/powersync_test'),
  PG_STORAGE_TEST_URL: utils.type.string.default('postgres://postgres:postgres@localhost:5432/powersync_storage_test'),
  MONGO_TEST_URL: utils.type.string.default('mongodb://localhost:27017/powersync_test'),
  CI: utils.type.boolean.default('false'),
  SLOW_TESTS: utils.type.boolean.default('false'),
  TEST_MONGO_STORAGE: utils.type.boolean.default('true'),
  TEST_POSTGRES_STORAGE: utils.type.boolean.default('true'),
  /**
   * Postgres image (e.g. postgres:17) for replication slot failover tests, which start their own
   * primary / standby containers with testcontainers. These tests are skipped when not set.
   */
  PG_FAILOVER_TEST_IMAGE: utils.type.string.optional()
});
