import { logger } from '@powersync/lib-services-framework';
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
  readonly mode: WriteCheckpointMode;

  constructor(options: MongoCheckpointAPIOptions) {
    this.db = options.db;
    this.mode = options.mode;
  }

  async batchCreateCustomWriteCheckpoints(checkpoints: CustomWriteCheckpointOptions[]): Promise<void> {
    return batchCreateCustomWriteCheckpoints(this.db, checkpoints);
  }

  async createCustomWriteCheckpoint(options: CustomWriteCheckpointOptions): Promise<bigint> {
    if (this.mode !== WriteCheckpointMode.CUSTOM) {
      logger.warn(`Creating a custom Write Checkpoint when the current Write Checkpoint mode is set to "${this.mode}"`);
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

  async createManagedWriteCheckpoint(checkpoint: ManagedWriteCheckpointOptions): Promise<bigint> {
    if (this.mode !== WriteCheckpointMode.CUSTOM) {
      logger.warn(
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
          throw new Error(`Sync rules ID is required for custom Write Checkpoint filtering`);
        }
        return this.lastCustomWriteCheckpoint(filters);
      case WriteCheckpointMode.MANAGED:
        if (false == 'heads' in filters) {
          throw new Error(`Replication HEAD is required for managed Write Checkpoint filtering`);
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
    checkpoints.map((checkpoint) => ({
      updateOne: {
        filter: { user_id: checkpoint.user_id, sync_rules_id: checkpoint.sync_rules_id },
        update: {
          $set: {
            checkpoint: checkpoint.checkpoint,
            sync_rules_id: checkpoint.sync_rules_id
          }
        },
        upsert: true
      }
    }))
  );
}
