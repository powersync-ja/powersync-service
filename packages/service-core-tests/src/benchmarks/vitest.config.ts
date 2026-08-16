import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['src/benchmarks/benchmarks/**/*.bench.ts'],
    pool: 'threads',
    fileParallelism: false,
    maxWorkers: 1,
    tags: [{ name: 'storage' }, { name: 'quick' }, { name: 'postgres-storage' }, { name: 'mongodb-storage' }]
  }
});
