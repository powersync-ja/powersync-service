import type { BenchmarkDataDefinition } from '../types/BenchmarkDataDefinition.js';
import type { BenchmarkProfile } from '../types/BenchmarkScenario.js';
import type { BenchmarkSyncDefinition } from '../types/BenchmarkSyncDefinition.js';
import { categorySync, multiTableData, singleTableData } from './scenario-presets.js';

export interface BenchmarkDataset {
  readonly name: string;
  readonly data: BenchmarkDataDefinition;
  readonly sync: BenchmarkSyncDefinition;
  readonly profile?: BenchmarkProfile;
  /** Override the suite's test timeout in milliseconds, including all iterations. */
  readonly timeout_ms?: number;
}

export const BASELINE_DATASETS: readonly BenchmarkDataset[] = [
  {
    name: 'Single table, 1k rows, one bucket',
    data: singleTableData({ rowCount: 1_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 1 })
  },
  {
    name: 'Two tables, 1k rows each, overlapping Sync Stream membership',
    ...multiTableData({ rowCount: 1_000, rowBytes: 512 })
  },
  {
    name: 'Single table, 1k rows, 10 buckets',
    data: singleTableData({ rowCount: 1_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 10 })
  },
  {
    name: 'Single table, 10k rows, one bucket',
    data: singleTableData({ rowCount: 10_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 1 }),
    profile: 'quick',
    timeout_ms: 300_000
  },
  {
    name: 'Single table, 100k rows, one bucket',
    data: singleTableData({ rowCount: 100_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 1 }),
    profile: 'standard',
    timeout_ms: 600_000
  },
  {
    name: 'Single table, 10k rows, 10 buckets',
    data: singleTableData({ rowCount: 10_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 10 }),
    profile: 'quick',
    timeout_ms: 300_000
  },
  {
    name: 'Single table, 100k rows, 10 buckets',
    data: singleTableData({ rowCount: 100_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 10 }),
    profile: 'standard',
    timeout_ms: 600_000
  },
  {
    name: 'Single table, 1k rows, 100 buckets',
    data: singleTableData({ rowCount: 1_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 100 }),
    profile: 'quick'
  },
  {
    name: 'Single table, 10k rows, 100 buckets',
    data: singleTableData({ rowCount: 10_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 100 }),
    profile: 'quick',
    timeout_ms: 300_000
  },
  {
    name: 'Single table, 100k rows, 100 buckets',
    data: singleTableData({ rowCount: 100_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 100 }),
    profile: 'standard',
    timeout_ms: 600_000
  }
];
