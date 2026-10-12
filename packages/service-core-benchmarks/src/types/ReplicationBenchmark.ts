import type { BenchmarkTransaction } from '../generators/benchmark-data.js';
import type { OperationEvidence } from '../utils/operation-evidence.js';
import type { BenchmarkDataDefinition } from './BenchmarkDataDefinition.js';
import { BenchmarkIterationRuntime } from './BenchmarkRunOptions.js';
import { BenchmarkScenario, BenchmarkSourceTable } from './BenchmarkScenario.js';
import { SnapshotBenchmarkItem, SnapshotBenchmarkManifest, SnapshotBenchmarkTarget } from './SnapshotBenchmark.js';
import { StorageBenchmarkImplementationId } from './StorageBenchmark.js';

export type ReplicationBenchmarkProducerId = 'source:postgres' | 'source:mongodb';

export type ReplicationBenchmarkPhase = 'snapshot' | 'streaming' | 'catch-up';

export interface ReplicationBenchmarkScenario extends BenchmarkScenario {
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

export interface ReplicationBenchmarkItem extends SnapshotBenchmarkItem {}

export type ReplicationBenchmarkTransaction = BenchmarkTransaction;

export interface ReplicationBenchmarkTarget extends SnapshotBenchmarkTarget {
  readonly committedAtNs?: string;
}

export type ReplicationBenchmarkManifest = SnapshotBenchmarkManifest;

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

export interface ReplicationBenchmarkSourceTable extends BenchmarkSourceTable {}

export interface ReplicationBenchmarkSourceAdapter {
  readonly id: ReplicationBenchmarkProducerId;
  readonly capabilities: ReplicationSourceCapabilities;
  readonly sourceTable: ReplicationBenchmarkSourceTable;
  setReplicationStreamName(name: string): void;
  createSchema(iterationId: string, data: BenchmarkDataDefinition, signal?: AbortSignal): Promise<void>;
  populateSnapshot(manifest: SnapshotBenchmarkManifest): Promise<ReplicationBenchmarkTarget>;
  prepareTransactions(manifest: ReplicationBenchmarkManifest): Promise<void>;
  commitTransaction(transaction: ReplicationBenchmarkTransaction): Promise<ReplicationBenchmarkTarget>;
  keepalive(): Promise<ReplicationBenchmarkTarget>;
  comparePosition(checkpoint: string, target: ReplicationBenchmarkTarget): ReplicationPositionComparison;
  collectMetadata(): Promise<object>;
  cleanup(): Promise<void>;
}

export interface ReplicationBenchmarkRunContext {
  readonly resource: ReplicationBenchmarkRunResource;
}

export interface ReplicationBenchmarkIterationContext {
  readonly runtime: BenchmarkIterationRuntime;
  readonly resource: ReplicationBenchmarkIterationResource;
  readonly manifest: ReplicationBenchmarkManifest;
}

export interface ReplicationBenchmarkObservation {
  readonly target: ReplicationBenchmarkTarget;
  readonly checkpoint: string;
  readonly checkpointVisibleAtNs: string;
  readonly replicationReleasedAtNs?: string;
  readonly evidence: OperationEvidence;
  readonly snapshotDone: boolean;
  readonly bucketCount: number;
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
