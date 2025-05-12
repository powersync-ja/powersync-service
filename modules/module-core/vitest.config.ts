import tsconfigPaths from 'vite-tsconfig-paths';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [tsconfigPaths()],
  test: {
    poolOptions: {
      threads: {
        singleThread: true
      }
    },
    pool: 'threads'
  }
});
