import { defineConfig } from 'vitest/config';

export default defineConfig({
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
