import { BenchmarkIterationRuntime } from './BenchmarkRunOptions.js';
import { BenchmarkScenario } from './BenchmarkScenario.js';
import { StorageBenchmarkImplementationId } from './StorageBenchmark.js';

export type ReplicationBenchmarkProducerId = 'postgres-source' | 'mongodb-source';

export type ReplicationBenchmarkPhase = 'snapshot' | 'streaming' | 'catch-up';

export interface ReplicationBenchmarkWorkload {
  readonly snapshot_row_count: number;
  readonly streaming_mutation_count: number;
  readonly transaction_count: number;
  readonly payload_bytes: number;
}

export interface ReplicationBenchmarkScenario extends BenchmarkScenario<ReplicationBenchmarkWorkload> {
  readonly layer: 'replication';
  readonly producer: ReplicationBenchmarkProducerId;
  readonly phase: ReplicationBenchmarkPhase;
  readonly storage: {
    readonly implementation: StorageBenchmarkImplementationId;
    readonly version: number;
  };
  readonly checkpoint_policy: 'target-visible';
  readonly core_verification: false;
}

export interface ReplicationBenchmarkItem {
  readonly [column: string]: string | number;
  readonly id: string;
  readonly owner_id: string;
  readonly category: string;
  readonly version: number;
  readonly updated_at: string;
  readonly payload: string;
  readonly is_target: number;
}

export interface ReplicationBenchmarkMutation {
  readonly tag: 'insert';
  readonly row: ReplicationBenchmarkItem;
}

export interface ReplicationBenchmarkTransaction {
  readonly id: string;
  readonly position: string;
  readonly mutations: readonly ReplicationBenchmarkMutation[];
}

export interface ReplicationBenchmarkTarget {
  readonly markerId: string;
  readonly nativePosition: string | null;
  readonly committedAtNs?: string;
}

export interface ReplicationBenchmarkManifest {
  readonly snapshotRows: readonly ReplicationBenchmarkItem[];
  readonly transactions: readonly ReplicationBenchmarkTransaction[];
  readonly target: ReplicationBenchmarkTarget;
  readonly sourceLogicalBytes: number;
  readonly payloadBytes: number;
  readonly expectedPutCount: number;
}

export interface ReplicationSourceCapabilities {
  readonly positionKind: 'wal-lsn' | 'mongo-lsn' | 'gtid' | 'cdc-lsn' | 'convex-cursor';
  readonly positionsComparable: boolean;
  readonly atomicity: 'transaction' | 'ordered-batch' | 'single-mutation' | 'topology-dependent';
  readonly keepalive: 'native' | 'marker' | 'polling';
}

export interface ReplicationPositionComparison {
  readonly comparable: boolean;
  readonly reached: boolean;
  readonly details?: object;
}

export interface ReplicationBenchmarkSourceAdapter {
  readonly id: ReplicationBenchmarkProducerId;
  readonly capabilities: ReplicationSourceCapabilities;
  createSchema(iterationId: string): Promise<void>;
  populateSnapshot(manifest: ReplicationBenchmarkManifest): Promise<ReplicationBenchmarkTarget>;
  prepareTransactions(manifest: ReplicationBenchmarkManifest): Promise<void>;
  commitTransaction(transaction: ReplicationBenchmarkTransaction): Promise<ReplicationBenchmarkTarget>;
  keepalive(): Promise<ReplicationBenchmarkTarget>;
  comparePosition(checkpoint: string, target: ReplicationBenchmarkTarget): ReplicationPositionComparison;
  collectMetadata(): Promise<object>;
  cleanup(): Promise<void>;
}

export interface ReplicationBenchmarkRunContext<Resource> {
  readonly resource: Resource;
}

export interface ReplicationBenchmarkIterationContext<Resource> {
  readonly runtime: BenchmarkIterationRuntime;
  readonly resource: Resource;
  readonly manifest: ReplicationBenchmarkManifest;
}

export interface ReplicationBenchmarkObservation {
  readonly target: ReplicationBenchmarkTarget;
  readonly checkpoint: string;
  readonly checkpointVisibleAtNs: string;
  readonly replicationReleasedAtNs?: string;
  readonly operations: readonly { op: string; object_id?: string; data?: string | null }[];
  readonly snapshotDone: boolean;
  readonly keepalives: number;
  readonly retries: number;
  readonly restarts: number;
}

export interface ReplicationBenchmarkIterationSetup {
  readonly iterationId: string;
  readonly scenario: ReplicationBenchmarkScenario;
  readonly manifest: ReplicationBenchmarkManifest;
}

export interface ReplicationReleaseObservation {
  readonly releasedAtNs: string;
  readonly snapshot?: { readonly position: string; readonly visibleAtNs: string };
  readonly target?: ReplicationBenchmarkTarget;
}

export interface ReplicationBenchmarkIterationResource {
  releaseReplication(waitForSnapshot?: boolean): Promise<ReplicationReleaseObservation>;
  commitTransaction(transactionId: string): Promise<ReplicationBenchmarkTarget>;
  keepalive(): Promise<ReplicationBenchmarkTarget>;
  observeCheckpoint(options: {
    target: ReplicationBenchmarkTarget;
    releasedAtNs?: string;
  }): Promise<ReplicationBenchmarkObservation>;
  comparePosition(checkpoint: string, target: ReplicationBenchmarkTarget): ReplicationPositionComparison;
  dispose(): Promise<void>;
}

export interface ReplicationBenchmarkRunResource {
  readonly environment: object;
  createIteration(setup: ReplicationBenchmarkIterationSetup): Promise<ReplicationBenchmarkIterationResource>;
  dispose(): Promise<void>;
}

export interface ReplicationBenchmarkImplementation {
  readonly sourceId: ReplicationBenchmarkProducerId;
  readonly storageId: StorageBenchmarkImplementationId;
  readonly storageVersion: number;
  open(signal: AbortSignal, runId: string): Promise<ReplicationBenchmarkRunResource>;
}
