import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    setupFiles: ['test/coverage.ts'],
    coverage: { provider: 'v8', include: ['src/**/*.ts'], reporter: ['text', 'json-summary'] }
  }
});
