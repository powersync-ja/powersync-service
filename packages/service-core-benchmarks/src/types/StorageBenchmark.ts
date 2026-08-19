import { storage } from '@powersync/service-core';
import { BenchmarkIterationRuntime } from './BenchmarkRunOptions.js';
import { BenchmarkScenario } from './BenchmarkScenario.js';

export type StorageBenchmarkImplementationId = 'postgres-storage' | 'mongodb-storage';

export interface StorageBenchmarkWorkload {
  readonly row_count: number;
  readonly payload_bytes: number;
}

export interface StorageBenchmarkScenario extends BenchmarkScenario<StorageBenchmarkWorkload> {
  readonly layer: 'storage';
  readonly storage: {
    readonly implementation: StorageBenchmarkImplementationId;
    readonly version: number;
  };
  readonly mode: 'write';
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

export interface StorageBenchmarkManifest {
  readonly rows: readonly StorageBenchmarkItem[];
  readonly sourceLogicalBytes: number;
  readonly payloadBytes: number;
}

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
  readonly storage: storage.SyncRulesBucketStorage;
  readonly syncRulesContent: storage.PersistedSyncConfigContent;
  readonly writer: storage.BucketStorageBatch;
  readonly sourceTable: storage.SourceTable;
  readonly manifest: StorageBenchmarkManifest;
  readonly flushes: StorageBenchmarkFlushCounter;
  readonly targetPosition: string;
}

export interface StorageBenchmarkObservation {
  readonly commit: storage.CheckpointResult;
}
