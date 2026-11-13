import { utils } from '@powersync/lib-services-framework';

export const env = utils.collectEnvironmentVariables({
  BENCHMARK_POSTGRES_SOURCE_URL: utils.type.string.optional(),
  BENCHMARK_POSTGRES_STORAGE_URL: utils.type.string.optional(),
  BENCHMARK_MONGODB_SOURCE_URL: utils.type.string.optional(),
  BENCHMARK_MONGODB_STORAGE_URL: utils.type.string.optional(),
  CI: utils.type.boolean.default('false')
});

export const fixtureEnvironment: Readonly<Record<string, string | undefined>> = {
  ...env,
  CI: String(env.CI)
};
