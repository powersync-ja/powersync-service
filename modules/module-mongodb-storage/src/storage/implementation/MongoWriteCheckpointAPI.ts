import { mongo } from '@powersync/lib-service-mongodb';
import * as framework from '@powersync/lib-services-framework';
import { GetCheckpointChangesOptions, InternalOpId, storage } from '@powersync/service-core';
import { VersionedPowerSyncMongo } from './db.js';

export type MongoCheckpointAPIOptions = {
  db: VersionedPowerSyncMongo;
  mode: storage.WriteCheckpointMode;
  sync_rules_id: number;
};

export class MongoWriteCheckpointAPI implements storage.WriteCheckpointAPI {
  readonly db: VersionedPowerSyncMongo;
  private _mode: storage.WriteCheckpointMode;

  constructor(options: MongoCheckpointAPIOptions) {
    this.db = options.db;
    this._mode = options.mode;
  }

  get writeCheckpointMode() {
    return this._mode;
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this._mode = mode;
  }

  async createManagedWriteCheckpoints(
    checkpoints: storage.ManagedWriteCheckpointOptions[]
  ): Promise<Map<string, bigint>> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.MANAGED) {
      throw new framework.ServiceAssertionError(
        `Attempting to create a managed Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const uniqueCheckpoints = [...new Map(checkpoints.map((checkpoint) => [checkpoint.user_id, checkpoint])).values()];
    if (uniqueCheckpoints.length == 0) {
      return new Map();
    }

    if (uniqueCheckpoints.length == 1) {
      // For the common case of a single checkpoint, we can do this in a single request.
      const { user_id } = uniqueCheckpoints[0];
      const doc = await this.db.write_checkpoints.findOneAndUpdate(
        {
          user_id
        },
        this.createManagedWriteCheckpointUpdate(uniqueCheckpoints[0]),
        { upsert: true, returnDocument: 'after' }
      );

      return new Map([[doc!.user_id, doc!.client_id]]);
    }

    // For more than one checkpoint, this gives a constant 2 requests
    await this.db.write_checkpoints.bulkWrite(
      uniqueCheckpoints.map((checkpoint) => ({
        updateOne: {
          filter: { user_id: checkpoint.user_id },
          update: this.createManagedWriteCheckpointUpdate(checkpoint),
          upsert: true
        }
      }))
    );

    const userIds = uniqueCheckpoints.map((checkpoint) => checkpoint.user_id);
    const docs = await this.db.write_checkpoints
      .find(
        {
          user_id: { $in: userIds }
        },
        {
          projection: {
            user_id: 1,
            client_id: 1
          }
        }
      )
      .toArray();

    return new Map(docs.map((doc) => [doc.user_id, doc.client_id]));
  }

  private createManagedWriteCheckpointUpdate(checkpoint: storage.ManagedWriteCheckpointOptions) {
    const { user_id, heads: lsns } = checkpoint;
    const checkpointRequestId = checkpoint.checkpoint_request_id ?? null;
    const hasSuppliedId = checkpointRequestId != null;
    // Generated managed checkpoints always increment. Supplied request ids are
    // monotonic: only a value greater than the stored client_id may update the
    // checkpoint id and heads. Stale or duplicate requests return the stored id.
    const shouldApplySuppliedId = {
      $and: [
        { $literal: hasSuppliedId },
        {
          $or: [{ $eq: ['$client_id', null] }, { $gt: [{ $literal: checkpointRequestId }, '$client_id'] }]
        }
      ]
    };
    const shouldUpdateCheckpoint = hasSuppliedId ? shouldApplySuppliedId : { $literal: true };
    const suppliedOrCurrentId = {
      $cond: [shouldApplySuppliedId, { $literal: checkpointRequestId }, '$client_id']
    };
    const generatedId = {
      $add: [{ $ifNull: ['$client_id', 0n] }, 1n]
    };
    const nextClientId = {
      $cond: [{ $literal: hasSuppliedId }, suppliedOrCurrentId, generatedId]
    };
    const nextIsCheckpointRequest = hasSuppliedId
      ? { $cond: [shouldApplySuppliedId, true, '$isCheckpointRequest'] }
      : false;

    return [
      {
        $set: {
          user_id,
          client_id: nextClientId,
          lsns: {
            $cond: [shouldUpdateCheckpoint, { $literal: lsns }, '$lsns']
          },
          processed_at_lsn: {
            $cond: [shouldUpdateCheckpoint, null, '$processed_at_lsn']
          },
          isCheckpointRequest: nextIsCheckpointRequest
        }
      }
    ];
  }

  async lastWriteCheckpoint(filters: storage.LastWriteCheckpointFilters): Promise<bigint | null> {
    switch (this.writeCheckpointMode) {
      case storage.WriteCheckpointMode.CUSTOM:
        if (false == 'sync_rules_id' in filters) {
          throw new framework.ServiceAssertionError(
            `Replication stream ID is required for custom Write Checkpoint filtering`
          );
        }
        return this.lastCustomWriteCheckpoint(filters);
      case storage.WriteCheckpointMode.MANAGED:
        if (false == 'heads' in filters) {
          throw new framework.ServiceAssertionError(
            `Replication HEAD is required for managed Write Checkpoint filtering`
          );
        }
        return this.lastManagedWriteCheckpoint(filters);
    }
  }

  async getWriteCheckpointChanges(options: GetCheckpointChangesOptions) {
    switch (this.writeCheckpointMode) {
      case storage.WriteCheckpointMode.CUSTOM:
        return this.getCustomWriteCheckpointChanges(options);
      case storage.WriteCheckpointMode.MANAGED:
        return this.getManagedWriteCheckpointChanges(options);
    }
  }

  protected async lastCustomWriteCheckpoint(filters: storage.CustomWriteCheckpointFilters) {
    const { user_id, sync_rules_id } = filters;
    const lastWriteCheckpoint = await this.db.custom_write_checkpoints.findOne({
      user_id,
      sync_rules_id
    });
    return lastWriteCheckpoint?.checkpoint ?? null;
  }

  protected async lastManagedWriteCheckpoint(filters: storage.ManagedWriteCheckpointFilters) {
    const { user_id, heads } = filters;
    // TODO: support multiple heads when we need to support multiple connections
    const lsn = heads['1'];
    if (lsn == null) {
      // Can happen if we haven't replicated anything yet.
      return null;
    }
    const lastWriteCheckpoint = await this.db.write_checkpoints.findOne({
      user_id: user_id,
      'lsns.1': { $lte: lsn }
    });
    return lastWriteCheckpoint?.client_id ?? null;
  }

  private async getManagedWriteCheckpointChanges(options: GetCheckpointChangesOptions) {
    const limit = 1000;
    const changes = await this.db.write_checkpoints
      .find(
        {
          processed_at_lsn: { $gt: options.lastCheckpoint.lsn, $lte: options.nextCheckpoint.lsn }
        },
        {
          limit: limit + 1,
          // batchSize is 1 more than limit to auto-close the cursor.
          // See https://github.com/mongodb/node-mongodb-native/pull/4580
          batchSize: limit + 2,
          singleBatch: true
        }
      )
      .toArray();
    const invalidate = changes.length > limit;

    const updatedWriteCheckpoints = new Map<string, bigint>();
    if (!invalidate) {
      for (let c of changes) {
        updatedWriteCheckpoints.set(c.user_id, c.client_id);
      }
    }

    return {
      invalidateWriteCheckpoints: invalidate,
      updatedWriteCheckpoints
    };
  }

  private async getCustomWriteCheckpointChanges(options: GetCheckpointChangesOptions) {
    const limit = 1000;
    const changes = await this.db.custom_write_checkpoints
      .find(
        {
          op_id: { $gt: options.lastCheckpoint.checkpoint, $lte: options.nextCheckpoint.checkpoint }
        },
        {
          limit: limit + 1,
          // batchSize is 1 more than limit to auto-close the cursor.
          // See https://github.com/mongodb/node-mongodb-native/pull/4580
          batchSize: limit + 2,
          singleBatch: true
        }
      )
      .toArray();
    const invalidate = changes.length > limit;

    const updatedWriteCheckpoints = new Map<string, bigint>();
    if (!invalidate) {
      for (let c of changes) {
        updatedWriteCheckpoints.set(c.user_id, c.checkpoint);
      }
    }

    return {
      invalidateWriteCheckpoints: invalidate,
      updatedWriteCheckpoints
    };
  }
}

export async function batchCreateCustomWriteCheckpoints(
  db: VersionedPowerSyncMongo,
  session: mongo.ClientSession,
  checkpoints: storage.CustomWriteCheckpointOptions[],
  opId: InternalOpId
): Promise<void> {
  if (checkpoints.length == 0) {
    return;
  }

  await db.custom_write_checkpoints.bulkWrite(
    checkpoints.map((checkpointOptions) => ({
      updateOne: {
        filter: { user_id: checkpointOptions.user_id, sync_rules_id: checkpointOptions.sync_rules_id },
        update: {
          $set: {
            checkpoint: checkpointOptions.checkpoint,
            sync_rules_id: checkpointOptions.sync_rules_id,
            op_id: opId
          }
        },
        upsert: true
      }
    })),
    { session }
  );
}
