import { SourceTable, storage } from '@powersync/service-core';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV1 } from './PersistedBatchV1.js';
import { SourceRecordStoreV1 } from './SourceRecordStoreV1.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';

export class MongoBucketBatchV1 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV1;

  private readonly store: SourceRecordStore;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.store = new SourceRecordStoreV1(this.db, this.group_id);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV1(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }

  protected async cleanupDroppedSourceTables(_tables: SourceTable[]) {
    // No-op for V1: source records live in a shared collection.
  }

  override async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<storage.CheckpointResult> {
    return super.commit(lsn, options);
  }

  override async keepalive(lsn: string): Promise<storage.CheckpointResult> {
    return super.keepalive(lsn);
  }

  override async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    return super.markAllSnapshotDone(no_checkpoint_before_lsn);
  }

  override async markTableSnapshotRequired(table: storage.SourceTable): Promise<void> {
    return super.markTableSnapshotRequired(table);
  }

  override async markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    return super.markTableSnapshotDone(tables, no_checkpoint_before_lsn);
  }
}
