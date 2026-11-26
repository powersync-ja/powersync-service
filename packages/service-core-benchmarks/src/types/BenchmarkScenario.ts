import type { BenchmarkDataDefinition } from './BenchmarkDataDefinition.js';
import type { BenchmarkSyncDefinition } from './BenchmarkSyncDefinition.js';

export type BenchmarkLayer = 'storage' | 'replication' | 'api' | 'combined';
export type BenchmarkProfile = 'quick' | 'standard' | 'manual';
/**
 * How bucket storage evaluates source rows.
 *
 * `rust` evaluates rows with the Rust addon where supported (MongoDB sources with MongoDB storage v3+).
 */
export type BenchmarkEvaluator = 'javascript' | 'rust';

export interface BenchmarkSourceTable {
  readonly schema: string;
  readonly table: string;
}

export interface BenchmarkScenario {
  id: string;
  description: string;
  layer: BenchmarkLayer;
  profile: BenchmarkProfile;
  readonly storage: {
    readonly implementation: 'storage:postgres' | 'storage:mongodb';
    readonly version: number;
    /** Defaults to `javascript`. */
    readonly evaluator?: BenchmarkEvaluator;
  };
  tags: string[];
  prerequisites: string[];
  timeout_ms: number;
  warmup_iterations: number;
  measured_iterations: number;
  readonly data: BenchmarkDataDefinition;
  readonly sync: BenchmarkSyncDefinition;
  sync_parameters: Record<string, unknown>;
  expected_bucket_count: number;
  expected_bucket_operation_count: number;
}
