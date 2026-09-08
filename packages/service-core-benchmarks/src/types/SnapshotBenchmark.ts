import type { Sequence } from '../utils/generated-sequence.js';
export interface SnapshotBenchmarkItem {
  readonly [column: string]: unknown;
  readonly id: string;
  readonly owner_id: string;
  readonly category: string;
  readonly version: number;
  readonly updated_at: string;
  readonly payload: string;
  readonly is_target: number;
}

export interface SnapshotBenchmarkTarget {
  readonly markerId: string;
  readonly nativePosition: string | null;
}

export interface SnapshotBenchmarkManifest {
  readonly snapshotRows: Sequence<SnapshotBenchmarkItem>;
  readonly target: SnapshotBenchmarkTarget;
  readonly sourceLogicalBytes: number;
  readonly payloadBytes: number;
  readonly expectedPutCount: number;
}
