import { storage } from '@powersync/service-core';
import type { BenchmarkDataManifest } from '../generators/benchmark-data.js';
import type { OperationEvidence } from '../utils/operation-evidence.js';
import { BenchmarkIterationRuntime } from './BenchmarkRunOptions.js';
import { BenchmarkScenario } from './BenchmarkScenario.js';

export type StorageBenchmarkImplementationId = 'storage:postgres' | 'storage:mongodb';

export interface StorageBenchmarkScenario extends BenchmarkScenario {
  readonly layer: 'storage';
  readonly storage: {
    readonly implementation: StorageBenchmarkImplementationId;
    readonly version: number;
  };
  readonly mode: 'write' | 'read' | 'write-read';
  readonly primary_boundary: 'storage_write_read' | 'storage_read' | 'storage_write';
  readonly flush_policy: 'automatic';
}

export interface StorageBenchmarkItem {
  readonly [column: string]: string | number;
  readonly id: string;
  readonly owner_id: string;
  readonly category: string;
  readonly version: number;
  readonly updated_at: string;
  readonly payload: string;
}

export interface StorageBenchmarkCase {
  readonly scenario: StorageBenchmarkScenario;
  readonly implementation: StorageBenchmarkImplementation;
  readonly expectedStorage: StorageBenchmarkScenario['storage'];
}

export type StorageBenchmarkManifest = BenchmarkDataManifest;

export interface StorageBenchmarkRunResource {
  readonly factory: storage.BucketStorageFactory;
  readonly tableIdStrings: boolean;
  readonly environment: object;
  dispose(): Promise<void>;
}

export interface StorageBenchmarkImplementation {
  readonly id: StorageBenchmarkImplementationId;
  open(signal: AbortSignal): Promise<StorageBenchmarkRunResource>;
}

export interface PostgresStorageBenchmarkImplementationOptions {
  readonly url: string;
}

export interface MongoStorageBenchmarkImplementationOptions {
  readonly url: string;
  readonly isCI: boolean;
}

export interface StorageBenchmarkRunContext {
  readonly resource: StorageBenchmarkRunResource;
}

export interface StorageBenchmarkFlushCounter {
  count: number;
}

export interface StorageBenchmarkIterationContext {
  readonly runtime: BenchmarkIterationRuntime;
  readonly replicationStream: storage.PersistedReplicationStream;
  readonly replicationLock: storage.ReplicationLock;
  readonly storage: storage.SyncRulesBucketStorage;
  readonly syncRulesContent: storage.PersistedSyncConfigContent;
  readonly writer: storage.BucketStorageBatch;
  readonly sourceTables: ReadonlyMap<string, storage.SourceTable>;
  readonly manifest: StorageBenchmarkManifest;
  readonly flushes: StorageBenchmarkFlushCounter;
  readonly targetPosition: string;
  readonly seedCommit?: storage.CheckpointResult;
}

export interface StorageBenchmarkObservation {
  readonly commit: storage.CheckpointResult;
  readonly read?: StorageBenchmarkReadObservation;
}

export interface StorageBenchmarkReadObservation {
  readonly checkpoint: storage.ReplicationCheckpoint;
  readonly buckets: readonly import('@powersync/service-sync-rules').ResolvedBucket[];
  readonly checksums: import('@powersync/service-core').ChecksumMap;
  readonly evidence: OperationEvidence;
}
