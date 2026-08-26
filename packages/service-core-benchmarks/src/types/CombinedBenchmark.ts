import { ApiClientConfiguration } from './ApiBenchmark.js';
import { BenchmarkIterationRuntime } from './BenchmarkRunOptions.js';
import { BenchmarkScenario } from './BenchmarkScenario.js';
import { ReplicationBenchmarkProducerId, ReplicationPositionComparison } from './ReplicationBenchmark.js';
import { SnapshotBenchmarkManifest, SnapshotBenchmarkTarget } from './SnapshotBenchmark.js';
import { StorageBenchmarkImplementationId } from './StorageBenchmark.js';

export interface CombinedBenchmarkWorkload {
  readonly snapshot_row_count: number;
  readonly payload_bytes: number;
}

export interface CombinedBenchmarkScenario
  extends BenchmarkScenario<CombinedBenchmarkWorkload>,
    ApiClientConfiguration {
  readonly layer: 'combined';
  readonly producer: ReplicationBenchmarkProducerId;
  readonly storage: {
    readonly implementation: StorageBenchmarkImplementationId;
    readonly version: number;
  };
}

export interface CombinedBenchmarkRunContext<Resource> {
  readonly resource: Resource;
}

export interface CombinedBenchmarkIterationContext<Resource> {
  readonly runtime: BenchmarkIterationRuntime;
  readonly resource: Resource;
  readonly manifest: SnapshotBenchmarkManifest;
}

export interface CombinedBenchmarkIterationSetup {
  readonly iterationId: string;
  readonly scenario: CombinedBenchmarkScenario;
  readonly manifest: SnapshotBenchmarkManifest;
}

export interface CombinedBenchmarkObservation {
  readonly target: SnapshotBenchmarkTarget;
  /** Exact bucket-storage checkpoint id exposed to clients as last_op_id. */
  readonly storageCheckpoint: string;
  /** Source position associated with the storage checkpoint. */
  readonly checkpoint: string;
  /** Parent-process monotonic time at which checkpoint visibility was observed. */
  readonly checkpointVisibleAtNs: string;
  /** Child-process monotonic time retained for diagnostics, never cross-clock duration calculations. */
  readonly childCheckpointVisibleAtNs: string;
  readonly operations: readonly { readonly op: string; readonly object_id?: string; readonly data?: string | null }[];
  readonly snapshotDone: boolean;
  readonly bucketCount: number;
}

export interface CombinedReleaseObservation {
  /** Parent-process monotonic time recorded immediately before sending the release command. */
  readonly parentReleasedAtNs: string;
  /** Child-process monotonic time retained for diagnostics only. */
  readonly childReleasedAtNs: string;
  /** Snapshot visibility fields are measured on the child clock. */
  readonly snapshot?: { readonly position: string; readonly visibleAtNs: string };
}

export interface CombinedBenchmarkIterationResource {
  readonly endpoint: string;
  readonly token: string;
  readonly target: SnapshotBenchmarkTarget;
  releaseReplication(waitForSnapshot?: boolean): Promise<CombinedReleaseObservation>;
  observeCheckpoint(): Promise<CombinedBenchmarkObservation>;
  comparePosition(checkpoint: string, target: SnapshotBenchmarkTarget): ReplicationPositionComparison;
  dispose(): Promise<void>;
}

export interface CombinedBenchmarkRunResource {
  readonly environment: object;
  createIteration(setup: CombinedBenchmarkIterationSetup): Promise<CombinedBenchmarkIterationResource>;
  dispose(): Promise<void>;
}

export interface CombinedBenchmarkImplementation {
  readonly sourceId: ReplicationBenchmarkProducerId;
  readonly storageId: StorageBenchmarkImplementationId;
  readonly storageVersion: number;
  open(signal: AbortSignal, runId: string): Promise<CombinedBenchmarkRunResource>;
}
