import { defineConfig } from 'vitest/config';

const tagsFilter = process.env.BENCHMARK_TAGS_FILTER?.trim();

export default defineConfig(() => ({
  test: {
    globalSetup: ['./src/setup/benchmark-global-setup.ts'],
    setupFiles: ['./src/setup/benchmark-env-setup.ts'],
    tagsFilter: [tagsFilter || '!profile:manual'],
    include: ['src/benchmarks/**/*.bench.ts'],
    pool: 'threads',
    fileParallelism: false,
    maxWorkers: 1,
    tags: [
      { name: 'profile:quick' },
      { name: 'profile:standard' },
      { name: 'profile:manual' },
      { name: 'engine:sync-streams' },
      { name: 'engine:sync-rules' },
      { name: 'layout:single-table' },
      { name: 'layout:multi-table' },
      { name: 'version:1' },
      { name: 'version:2' },
      { name: 'version:3' },
      { name: 'version:4' },
      { name: 'source:postgres' },
      { name: 'source:mongodb' },
      { name: 'storage:postgres' },
      { name: 'storage:mongodb' },
      { name: 'phase:snapshot' },
      { name: 'phase:streaming' },
      { name: 'layer:api' },
      { name: 'layer:combined' },
      { name: 'layer:replication' },
      { name: 'layer:storage' },
      { name: 'shape:single-bucket' },
      { name: 'shape:10-buckets' }
    ]
  }
}));
