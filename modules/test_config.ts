import * as path from 'node:path';

import { defineConfig } from 'vitest/config';

export function serviceIntegrationTestConfig(rootDir: string, options: { hasSetup: boolean } = { hasSetup: true }) {
  return defineConfig({
    resolve: {
      alias: {
        '@module': path.resolve(rootDir, 'src'),
        '@core-tests': path.resolve(rootDir, '../../packages/service-core/test/src'),
        '@': path.resolve(rootDir, '../../packages/service-core/src')
      }
    },
    test: {
      setupFiles: options.hasSetup ? './test/src/setup.ts' : undefined,
      pool: 'threads',
      maxWorkers: 1
    }
  });
}
