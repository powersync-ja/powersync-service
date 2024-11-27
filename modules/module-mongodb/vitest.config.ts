import tsconfigPaths from 'vite-tsconfig-paths';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [tsconfigPaths()],
  test: {
    setupFiles: './test/src/setup.ts',
    poolOptions: {
      threads: {
        singleThread: true
      }
    },
    pool: 'threads'
  }
});
