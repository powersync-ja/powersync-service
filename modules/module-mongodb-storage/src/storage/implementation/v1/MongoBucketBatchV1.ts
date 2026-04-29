import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { SourceTable, storage } from '@powersync/service-core';
import { mongoTableId } from '../../../utils/util.js';
import { calculateCheckpointState } from '../CheckpointState.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { SyncRuleDocumentV1 } from '../models.js';
import { PersistedBatchV1 } from './PersistedBatchV1.js';
import { SourceRecordStoreV1 } from './SourceRecordStoreV1.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';

export class MongoBucketBatchV1 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV1;

  private readonly store: SourceRecordStore;
  private needsActivation = true;
  private lastWaitingLogThottled = 0;

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

  async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<storage.CheckpointResult> {
    const { createEmptyCheckpoints } = { ...storage.DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS, ...options };

    await this.flush(options);

    const now = new Date();

    await this.db.write_checkpoints.updateMany(
      {
        processed_at_lsn: null,
        'lsns.1': { $lte: lsn }
      },
      {
        $set: {
          processed_at_lsn: lsn
        }
      },
      {
        session: this.session
      }
    );

    const can_checkpoint = {
      $and: [
        { $eq: ['$snapshot_done', true] },
        {
          $or: [{ $eq: ['$last_checkpoint_lsn', null] }, { $lte: ['$last_checkpoint_lsn', { $literal: lsn }] }]
        },
        {
          $or: [{ $eq: ['$no_checkpoint_before', null] }, { $lte: ['$no_checkpoint_before', { $literal: lsn }] }]
        }
      ]
    };

    const new_keepalive_op = {
      $cond: [
        can_checkpoint,
        { $literal: null },
        {
          $toString: {
            $max: [{ $toLong: '$keepalive_op' }, { $literal: this.persisted_op }, 0n]
          }
        }
      ]
    };

    const new_last_checkpoint = {
      $cond: [
        can_checkpoint,
        {
          $max: ['$last_checkpoint', { $literal: this.persisted_op }, { $toLong: '$keepalive_op' }, 0n]
        },
        '$last_checkpoint'
      ]
    };

    const preUpdateDocument = (await this.db.sync_rules.findOneAndUpdate(
      { _id: this.group_id },
      [
        {
          $set: {
            _can_checkpoint: can_checkpoint,
            _not_empty: createEmptyCheckpoints
              ? true
              : {
                  $or: [
                    { $literal: createEmptyCheckpoints },
                    { $ne: ['$keepalive_op', new_keepalive_op] },
                    { $ne: ['$last_checkpoint', new_last_checkpoint] }
                  ]
                }
          }
        },
        {
          $set: {
            last_checkpoint_lsn: {
              $cond: [{ $and: ['$_can_checkpoint', '$_not_empty'] }, { $literal: lsn }, '$last_checkpoint_lsn']
            },
            last_checkpoint_ts: {
              $cond: [{ $and: ['$_can_checkpoint', '$_not_empty'] }, { $literal: now }, '$last_checkpoint_ts']
            },
            last_keepalive_ts: { $literal: now },
            last_fatal_error: { $literal: null },
            last_fatal_error_ts: { $literal: null },
            keepalive_op: new_keepalive_op,
            last_checkpoint: new_last_checkpoint,
            snapshot_lsn: {
              $cond: [{ $and: ['$_can_checkpoint', '$_not_empty'] }, { $literal: null }, '$snapshot_lsn']
            }
          }
        },
        {
          $unset: ['_can_checkpoint', '_not_empty']
        }
      ],
      {
        session: this.session,
        returnDocument: 'before',
        projection: {
          snapshot_done: 1,
          last_checkpoint_lsn: 1,
          no_checkpoint_before: 1,
          keepalive_op: 1,
          last_checkpoint: 1
        }
      }
    )) as SyncRuleDocumentV1;

    if (preUpdateDocument == null) {
      throw new ReplicationAssertionError(
        'Failed to update checkpoint - no matching sync_rules document for _id: ' + this.group_id
      );
    }

    const checkpointState = calculateCheckpointState({
      lsn,
      snapshotDone: preUpdateDocument.snapshot_done === true,
      lastCheckpointLsn: preUpdateDocument.last_checkpoint_lsn,
      noCheckpointBefore: preUpdateDocument.no_checkpoint_before,
      keepaliveOp: preUpdateDocument.keepalive_op == null ? null : BigInt(preUpdateDocument.keepalive_op),
      lastCheckpoint: preUpdateDocument.last_checkpoint,
      persistedOp: this.persisted_op,
      createEmptyCheckpoints
    });
    if (checkpointState.checkpointBlocked) {
      if (Date.now() - this.lastWaitingLogThottled > 5_000) {
        this.logger.info(
          `Waiting before creating checkpoint, currently at ${lsn} / ${preUpdateDocument.keepalive_op}. Current state: ${JSON.stringify(
            {
              snapshot_done: preUpdateDocument.snapshot_done,
              last_checkpoint_lsn: preUpdateDocument.last_checkpoint_lsn,
              no_checkpoint_before: preUpdateDocument.no_checkpoint_before
            }
          )}`
        );
        this.lastWaitingLogThottled = Date.now();
      }
    } else {
      if (checkpointState.checkpointCreated) {
        this.logger.debug(`Created checkpoint at ${lsn} / ${checkpointState.newLastCheckpoint}`);
      }
      await this.autoActivate(lsn);
      await this.db.notifyCheckpoint();
      this.persisted_op = null;
      this.last_checkpoint_lsn = lsn;
      if (checkpointState.newLastCheckpoint != null) {
        await this.sourceRecordStore.postCommitCleanup(checkpointState.newLastCheckpoint, this.logger);
      }
    }

    return {
      checkpointBlocked: checkpointState.checkpointBlocked,
      checkpointCreated: checkpointState.checkpointCreated
    };
  }

  async keepalive(lsn: string): Promise<storage.CheckpointResult> {
    return await this.commit(lsn, { createEmptyCheckpoints: true });
  }

  async setResumeLsn(lsn: string): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id
      },
      {
        $set: {
          snapshot_lsn: lsn
        }
      },
      { session: this.session }
    );
  }

  async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id
      },
      {
        $set: {
          snapshot_done: true,
          last_keepalive_ts: new Date()
        },
        $max: {
          no_checkpoint_before: no_checkpoint_before_lsn
        }
      },
      { session: this.session }
    );
  }

  async markTableSnapshotRequired(_table: storage.SourceTable): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id
      },
      {
        $set: {
          snapshot_done: false
        }
      },
      { session: this.session }
    );
  }

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    const session = this.session;
    const ids = tables.map((table) => mongoTableId(table.id));

    await this.withTransaction(async () => {
      await this.db.commonSourceTables(this.group_id).updateMany(
        { _id: { $in: ids } },
        {
          $set: {
            snapshot_done: true
          },
          $unset: {
            snapshot_status: 1
          }
        },
        { session }
      );

      if (no_checkpoint_before_lsn != null) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.group_id
          },
          {
            $set: {
              last_keepalive_ts: new Date()
            },
            $max: {
              no_checkpoint_before: no_checkpoint_before_lsn
            }
          },
          { session: this.session }
        );
      }
    });
    return tables.map((table) => {
      const copy = table.clone();
      copy.snapshotComplete = true;
      return copy;
    });
  }

  private async autoActivate(lsn: string): Promise<void> {
    if (!this.needsActivation) {
      return;
    }

    const session = this.session;
    let activated = false;
    await session.withTransaction(async () => {
      const doc = (await this.db.sync_rules.findOne({ _id: this.group_id }, { session })) as SyncRuleDocumentV1;
      if (doc && doc.state == storage.SyncRuleState.PROCESSING && doc.snapshot_done && doc.last_checkpoint != null) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.group_id
          },
          {
            $set: {
              state: storage.SyncRuleState.ACTIVE
            }
          },
          { session }
        );

        await this.db.sync_rules.updateMany(
          {
            _id: { $ne: this.group_id },
            state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
          },
          {
            $set: {
              state: storage.SyncRuleState.STOP
            }
          },
          { session }
        );
        activated = true;
      } else if (doc?.state != storage.SyncRuleState.PROCESSING) {
        this.needsActivation = false;
      }
    });
    if (activated) {
      this.logger.info(`Activated new sync rules at ${lsn}`);
      await this.db.notifyCheckpoint();
      this.needsActivation = false;
    }
  }
}
