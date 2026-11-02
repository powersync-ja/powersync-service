import { defineConfig, mergeConfig } from 'vitest/config';
import { serviceIntegrationTestConfig } from '../test_config';

export default mergeConfig(
  serviceIntegrationTestConfig(__dirname),
  defineConfig({
    test: {
      include: ['benchmarks/*.benchmark.ts'],
      pool: 'forks',
      execArgv: ['--expose-gc'],
      maxWorkers: 1,
      fileParallelism: false
    }
  })
);
