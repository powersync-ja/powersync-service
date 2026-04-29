import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { calculateCheckpointState } from '../CheckpointState.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';
import { SourceRecordStoreV3 } from './SourceRecordStoreV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';
import { SyncRuleDocumentV3 } from './models.js';

export class MongoBucketBatchV3 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV3;

  private readonly store: SourceRecordStore;
  private readonly syncConfigId: bson.ObjectId;
  private needsActivationV3 = true;
  private lastWaitingLogThrottledV3 = 0;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    if (options.syncConfigId == null) {
      throw new ReplicationAssertionError('Missing sync config id for v3 batch');
    }
    this.syncConfigId = options.syncConfigId;
    this.store = new SourceRecordStoreV3(this.db, this.group_id, this.mapping);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }

  protected async cleanupDroppedSourceTables(sourceTables: storage.SourceTable[]) {
    for (const table of sourceTables) {
      await this.db
        .sourceRecordsV3(this.group_id, mongoTableId(table.id))
        .drop()
        .catch((error) => {
          if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
            return;
          }
          throw error;
        });
    }
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

    const preUpdateDocument = await this.db.sync_rules.findOne(
      {
        _id: this.group_id,
        'sync_configs._id': this.syncConfigId
      },
      {
        session: this.session,
        projection: {
          snapshot_lsn: 1,
          sync_configs: {
            $elemMatch: {
              _id: this.syncConfigId
            }
          }
        }
      }
    );

    const state = (preUpdateDocument as SyncRuleDocumentV3)?.sync_configs?.[0];
    if (state == null) {
      throw new ReplicationAssertionError(
        `Failed to update checkpoint - no matching sync_config for _id: ${this.group_id}/${this.syncConfigId.toHexString()}`
      );
    }

    const checkpointState = calculateCheckpointState({
      lsn,
      snapshotDone: state.snapshot_done === true,
      lastCheckpointLsn: state.last_checkpoint_lsn,
      noCheckpointBefore: state.no_checkpoint_before,
      keepaliveOp: state.keepalive_op == null ? null : BigInt(state.keepalive_op),
      lastCheckpoint: state.last_checkpoint,
      persistedOp: this.persisted_op,
      createEmptyCheckpoints
    });

    const updateSet: Record<string, any> = {
      last_keepalive_ts: now,
      last_fatal_error: null,
      last_fatal_error_ts: null,
      'sync_configs.$[config].keepalive_op': checkpointState.newKeepaliveOp,
      'sync_configs.$[config].last_checkpoint': checkpointState.newLastCheckpoint
    };
    if (checkpointState.checkpointCreated) {
      updateSet['sync_configs.$[config].last_checkpoint_lsn'] = lsn;
      updateSet['snapshot_lsn'] = null;
      updateSet['last_checkpoint_ts'] = now;
    }

    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id,
        'sync_configs._id': this.syncConfigId
      },
      {
        $set: updateSet
      },
      {
        session: this.session,
        arrayFilters: [{ 'config._id': this.syncConfigId }]
      }
    );

    if (checkpointState.checkpointBlocked) {
      if (Date.now() - this.lastWaitingLogThrottledV3 > 5_000) {
        this.logger.info(
          `Waiting before creating checkpoint, currently at ${lsn} / ${state.keepalive_op}. Current state: ${JSON.stringify(
            {
              snapshot_done: state.snapshot_done,
              last_checkpoint_lsn: state.last_checkpoint_lsn,
              no_checkpoint_before: state.no_checkpoint_before
            }
          )}`
        );
        this.lastWaitingLogThrottledV3 = Date.now();
      }
    } else {
      if (checkpointState.checkpointCreated) {
        this.logger.debug(`Created checkpoint at ${lsn} / ${checkpointState.newLastCheckpoint}`);
      }
      await this.autoActivateV3(lsn);
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
        _id: this.group_id,
        'sync_configs._id': this.syncConfigId
      },
      {
        $set: {
          snapshot_lsn: lsn
        }
      },
      { session: this.session }
    );
  }

  private async autoActivateV3(lsn: string): Promise<void> {
    if (!this.needsActivationV3) {
      return;
    }

    const session = this.session;
    let activated = false;
    await session.withTransaction(async () => {
      const doc = await this.db.sync_rules.findOne(
        {
          _id: this.group_id,
          'sync_configs._id': this.syncConfigId
        },
        {
          session,
          projection: {
            state: 1,
            sync_configs: {
              $elemMatch: {
                _id: this.syncConfigId
              }
            }
          }
        }
      );
      const state = (doc as SyncRuleDocumentV3)?.sync_configs?.[0];
      if (
        doc &&
        doc.state == storage.SyncRuleState.PROCESSING &&
        state?.state == storage.SyncRuleState.PROCESSING &&
        state.snapshot_done &&
        state.last_checkpoint != null
      ) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.group_id,
            'sync_configs._id': this.syncConfigId
          },
          {
            $set: {
              state: storage.SyncRuleState.ACTIVE,
              'sync_configs.$[config].state': storage.SyncRuleState.ACTIVE
            }
          },
          {
            session,
            arrayFilters: [{ 'config._id': this.syncConfigId }]
          }
        );

        await this.db.sync_rules.updateMany(
          {
            _id: { $ne: this.group_id },
            state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] },
            sync_configs: { $exists: true }
          },
          {
            $set: {
              state: storage.SyncRuleState.STOP,
              'sync_configs.$[].state': storage.SyncRuleState.STOP
            }
          },
          { session }
        );
        activated = true;
      } else if (doc?.state != storage.SyncRuleState.PROCESSING) {
        this.needsActivationV3 = false;
      }
    });
    if (activated) {
      this.logger.info(`Activated new sync rules at ${lsn}`);
      await this.db.notifyCheckpoint();
      this.needsActivationV3 = false;
    }
  }

  async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id,
        'sync_configs._id': this.syncConfigId
      },
      {
        $set: {
          'sync_configs.$[config].snapshot_done': true,
          last_keepalive_ts: new Date()
        },
        $max: {
          'sync_configs.$[config].no_checkpoint_before': no_checkpoint_before_lsn
        }
      },
      {
        session: this.session,
        arrayFilters: [{ 'config._id': this.syncConfigId }]
      }
    );
  }

  async markTableSnapshotRequired(_table: storage.SourceTable): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id,
        'sync_configs._id': this.syncConfigId
      },
      {
        $set: {
          'sync_configs.$[config].snapshot_done': false
        }
      },
      {
        session: this.session,
        arrayFilters: [{ 'config._id': this.syncConfigId }]
      }
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
            _id: this.group_id,
            'sync_configs._id': this.syncConfigId
          },
          {
            $set: {
              last_keepalive_ts: new Date()
            },
            $max: {
              'sync_configs.$[config].no_checkpoint_before': no_checkpoint_before_lsn
            }
          },
          {
            session: this.session,
            arrayFilters: [{ 'config._id': this.syncConfigId }]
          }
        );
      }
    });
    return tables.map((table) => {
      const copy = table.clone();
      copy.snapshotComplete = true;
      return copy;
    });
  }
}
