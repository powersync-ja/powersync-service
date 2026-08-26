import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['src/benchmarks/**/*.bench.ts'],
    pool: 'threads',
    fileParallelism: false,
    maxWorkers: 1,
    tags: [
      { name: 'storage' },
      { name: 'replication' },
      { name: 'api' },
      { name: 'combined' },
      { name: 'quick' },
      { name: 'baseline' },
      { name: 'multi-buckets' },
      { name: 'buckets-10' },
      { name: 'snapshot' },
      { name: 'streaming' },
      { name: 'initial' },
      { name: 'http' },
      { name: 'ndjson' },
      { name: 'mongodb-source' },
      { name: 'postgres-source' },
      { name: 'postgres-storage' },
      { name: 'mongodb-storage' },
      { name: 'storage-v2' }
    ]
  }
});
