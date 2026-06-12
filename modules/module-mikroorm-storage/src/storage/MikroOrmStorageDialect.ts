import type { EntityClass, EntityManager, EntityName } from '@mikro-orm/core';
import type { BucketDataRequest } from '@powersync/service-core';
import type {
  BucketData,
  BucketParameters,
  CurrentData,
  Instance,
  SourceTable,
  SyncRules,
  WriteCheckpoint
} from '../entities/entities-index.js';

/**
 * Complete database-specific adapter consumed by the common MikroORM storage classes.
 */
export interface MikroOrmStorageDialect {
  /** Public storage type identifier, e.g. `mikroorm:sqlite`. */
  readonly type: string;
  /** Flat entity class list passed to MikroORM initialization and migration tooling. */
  readonly entityClasses: EntityName<any>[];
  /** Stored bucket operations, typically read by bucket and operation id. */
  readonly bucketDataEntity: EntityClass<BucketData>;
  /** Materialized parameter lookups for bucket parameter queries. */
  readonly bucketParametersEntity: EntityClass<BucketParameters>;
  /** Latest known source row state used by replication batching and compaction paths. */
  readonly currentDataEntity: EntityClass<CurrentData>;
  /** Singleton service instance identity row. */
  readonly instanceEntity: EntityClass<Instance>;
  /** Source table replication metadata and snapshot progress. */
  readonly sourceTableEntity: EntityClass<SourceTable>;
  /** Sync rule versions and replication stream state. */
  readonly syncRulesEntity: EntityClass<SyncRules>;
  /** User write checkpoint rows. */
  readonly writeCheckpointEntity: EntityClass<WriteCheckpoint>;
  /**
   * Stream bucket operation rows for sync reads.
   *
   * Hot bucket reads need to avoid buffering a whole result set before chunking. SQL drivers can implement this with
   * MikroORM query builder streaming, while non-SQL drivers can provide their closest cursor equivalent.
   */
  streamBucketDataRows(options: MikroOrmBucketDataStreamOptions): AsyncIterable<BucketData>;
  /** Create a checkpoint watcher suitable for this database's notification capabilities. */
  createCheckpointWatcher(): MikroOrmCheckpointWatcher;
}

export interface MikroOrmBucketDataStreamOptions {
  readonly em: EntityManager;
  readonly groupId: number;
  readonly checkpoint: bigint;
  readonly dataBuckets: BucketDataRequest[];
  readonly limit: number;
}

/**
 * Minimal checkpoint notification abstraction.
 *
 * SQLite can only notify listeners inside the current process, while future drivers can map this onto database-level
 * notifications that work across service instances.
 */
export interface MikroOrmCheckpointWatcher {
  /** Wake local watchers after this process writes a relevant checkpoint. */
  notify(): void;
  /** Yield whenever a checkpoint change should be re-read. */
  watch(signal: AbortSignal): AsyncIterable<void>;
}

/**
 * Process-local checkpoint watcher for drivers without a database notification implementation.
 *
 * This is sufficient for single-process runners and tests. Drivers that support cross-process notifications should
 * replace this with a watcher backed by the database or an external event channel.
 */
export class InProcessMikroOrmCheckpointWatcher implements MikroOrmCheckpointWatcher {
  private readonly listeners = new Set<() => void>();

  notify(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }

  async *watch(signal: AbortSignal): AsyncIterable<void> {
    while (!signal.aborted) {
      yield await new Promise<void>((resolve) => {
        const listener = () => {
          this.listeners.delete(listener);
          resolve();
        };
        this.listeners.add(listener);
        signal.addEventListener(
          'abort',
          () => {
            this.listeners.delete(listener);
            resolve();
          },
          { once: true }
        );
      });
    }
  }
}
