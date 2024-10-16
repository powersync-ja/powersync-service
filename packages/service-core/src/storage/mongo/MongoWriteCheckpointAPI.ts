import * as framework from '@powersync/lib-services-framework';
import {
  CustomWriteCheckpointFilters,
  CustomWriteCheckpointOptions,
  LastWriteCheckpointFilters,
  ManagedWriteCheckpointFilters,
  ManagedWriteCheckpointOptions,
  WriteCheckpointAPI,
  WriteCheckpointMode
} from '../write-checkpoint.js';
import { PowerSyncMongo } from './db.js';

export type MongoCheckpointAPIOptions = {
  db: PowerSyncMongo;
  mode: WriteCheckpointMode;
};

export class MongoWriteCheckpointAPI implements WriteCheckpointAPI {
  readonly db: PowerSyncMongo;
  private _mode: WriteCheckpointMode;

  constructor(options: MongoCheckpointAPIOptions) {
    this.db = options.db;
    this._mode = options.mode;
  }

  get mode() {
    return this._mode;
  }

  setWriteCheckpointMode(mode: WriteCheckpointMode): void {
    this._mode = mode;
  }

  async batchCreateCustomWriteCheckpoints(checkpoints: CustomWriteCheckpointOptions[]): Promise<void> {
    return batchCreateCustomWriteCheckpoints(this.db, checkpoints);
  }

  async createCustomWriteCheckpoint(options: CustomWriteCheckpointOptions): Promise<bigint> {
    /**
     * Allow creating custom checkpoints even if the current mode is not `custom`.
     * There might be a state where the next sync rules rely on replicating custom
     * write checkpoints, but the current active sync rules uses managed checkpoints.
     */
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

  async createManagedWriteCheckpoint(checkpoint: ManagedWriteCheckpointOptions): Promise<bigint> {
    if (this.mode !== WriteCheckpointMode.MANAGED) {
      throw new framework.errors.ValidationError(
        `Creating a managed Write Checkpoint when the current Write Checkpoint mode is set to "${this.mode}"`
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

  async lastWriteCheckpoint(filters: LastWriteCheckpointFilters): Promise<bigint | null> {
    switch (this.mode) {
      case WriteCheckpointMode.CUSTOM:
        if (false == 'sync_rules_id' in filters) {
          throw new framework.errors.ValidationError(`Sync rules ID is required for custom Write Checkpoint filtering`);
        }
        return this.lastCustomWriteCheckpoint(filters);
      case WriteCheckpointMode.MANAGED:
        if (false == 'heads' in filters) {
          throw new framework.errors.ValidationError(
            `Replication HEAD is required for managed Write Checkpoint filtering`
          );
        }
        return this.lastManagedWriteCheckpoint(filters);
    }
  }

  protected async lastCustomWriteCheckpoint(filters: CustomWriteCheckpointFilters) {
    const { user_id, sync_rules_id } = filters;
    const lastWriteCheckpoint = await this.db.custom_write_checkpoints.findOne({
      user_id,
      sync_rules_id
    });
    return lastWriteCheckpoint?.checkpoint ?? null;
  }

  protected async lastManagedWriteCheckpoint(filters: ManagedWriteCheckpointFilters) {
    const { user_id } = filters;
    const lastWriteCheckpoint = await this.db.write_checkpoints.findOne({
      user_id: user_id
    });
    return lastWriteCheckpoint?.client_id ?? null;
  }
}

export async function batchCreateCustomWriteCheckpoints(
  db: PowerSyncMongo,
  checkpoints: CustomWriteCheckpointOptions[]
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
    }))
  );
}
