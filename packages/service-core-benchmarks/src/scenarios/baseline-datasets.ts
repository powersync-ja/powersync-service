import type { BenchmarkDataDefinition } from '../types/BenchmarkDataDefinition.js';
import type { BenchmarkSyncDefinition } from '../types/BenchmarkSyncDefinition.js';
import { categorySync, multiTableData, singleTableData } from './scenario-presets.js';

export interface BenchmarkDataset {
  readonly name: string;
  readonly data: BenchmarkDataDefinition;
  readonly sync: BenchmarkSyncDefinition;
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
  }
];
