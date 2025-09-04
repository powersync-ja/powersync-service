import { mongo } from '@powersync/lib-service-mongodb';
import * as framework from '@powersync/lib-services-framework';
import { GetCheckpointChangesOptions, InternalOpId, storage } from '@powersync/service-core';
import { PowerSyncMongo } from './db.js';

export type MongoCheckpointAPIOptions = {
  db: PowerSyncMongo;
  mode: storage.WriteCheckpointMode;
  sync_rules_id: number;
};

export class MongoWriteCheckpointAPI implements storage.WriteCheckpointAPI {
  readonly db: PowerSyncMongo;
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

  async createManagedWriteCheckpoint(checkpoint: storage.ManagedWriteCheckpointOptions): Promise<bigint> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.MANAGED) {
      throw new framework.ServiceAssertionError(
        `Attempting to create a managed Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const { user_id, heads: lsns } = checkpoint;
    const doc = await this.db.write_checkpoints.findOneAndUpdate(
      {
        user_id: user_id
      },
      {
        $set: {
          lsns,
          processed_at_lsn: null
        },
        $inc: {
          client_id: 1n
        }
      },
      { upsert: true, returnDocument: 'after' }
    );
    return doc!.client_id;
  }

  async lastWriteCheckpoint(filters: storage.LastWriteCheckpointFilters): Promise<bigint | null> {
    switch (this.writeCheckpointMode) {
      case storage.WriteCheckpointMode.CUSTOM:
        if (false == 'sync_rules_id' in filters) {
          throw new framework.ServiceAssertionError(`Sync rules ID is required for custom Write Checkpoint filtering`);
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
          batchSize: limit + 1,
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
          batchSize: limit + 1,
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
  db: PowerSyncMongo,
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
