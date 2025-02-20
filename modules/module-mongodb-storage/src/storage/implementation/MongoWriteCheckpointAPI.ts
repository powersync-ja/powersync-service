import * as framework from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { PowerSyncMongo } from './db.js';

export type MongoCheckpointAPIOptions = {
  db: PowerSyncMongo;
  mode: storage.WriteCheckpointMode;
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

  async batchCreateCustomWriteCheckpoints(checkpoints: storage.CustomWriteCheckpointOptions[]): Promise<void> {
    return batchCreateCustomWriteCheckpoints(this.db, checkpoints);
  }

  async createCustomWriteCheckpoint(options: storage.CustomWriteCheckpointOptions): Promise<bigint> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.CUSTOM) {
      throw new framework.errors.ValidationError(
        `Creating a custom Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const { checkpoint, user_id, sync_rules_id } = options;
    const doc = await this.db.custom_write_checkpoints.findOneAndUpdate(
      {
        user_id: user_id,
        sync_rules_id
      },
      {
        $set: {
          checkpoint
        }
      },
      { upsert: true, returnDocument: 'after' }
    );
    return doc!.checkpoint;
  }

  async createManagedWriteCheckpoint(checkpoint: storage.ManagedWriteCheckpointOptions): Promise<bigint> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.MANAGED) {
      throw new framework.errors.ValidationError(
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
          lsns
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
          throw new framework.errors.ValidationError(`Sync rules ID is required for custom Write Checkpoint filtering`);
        }
        return this.lastCustomWriteCheckpoint(filters);
      case storage.WriteCheckpointMode.MANAGED:
        if (false == 'heads' in filters) {
          throw new framework.errors.ValidationError(
            `Replication HEAD is required for managed Write Checkpoint filtering`
          );
        }
        return this.lastManagedWriteCheckpoint(filters);
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
}

export async function batchCreateCustomWriteCheckpoints(
  db: PowerSyncMongo,
  checkpoints: storage.CustomWriteCheckpointOptions[]
): Promise<void> {
  if (!checkpoints.length) {
    return;
  }

  await db.custom_write_checkpoints.bulkWrite(
    checkpoints.map((checkpointOptions) => ({
      updateOne: {
        filter: { user_id: checkpointOptions.user_id, sync_rules_id: checkpointOptions.sync_rules_id },
        update: {
          $set: {
            checkpoint: checkpointOptions.checkpoint,
            sync_rules_id: checkpointOptions.sync_rules_id
          }
        },
        upsert: true
      }
    })),
    {}
  );
}
