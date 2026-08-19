import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['src/benchmarks/benchmarks/**/*.bench.ts'],
    pool: 'threads',
    fileParallelism: false,
    maxWorkers: 1,
    tags: [
      { name: 'storage' },
      { name: 'replication' },
      { name: 'quick' },
      { name: 'baseline' },
      { name: 'snapshot' },
      { name: 'streaming' },
      { name: 'synthetic-source' },
      { name: 'mongodb-source' },
      { name: 'postgres-storage' },
      { name: 'mongodb-storage' },
      { name: 'storage-v2' }
    ]
  }
});
