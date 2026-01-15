import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [],
  test: {
    setupFiles: ['test/matchers.ts'],
    coverage: {
      provider: 'v8'
    }
  }
});
