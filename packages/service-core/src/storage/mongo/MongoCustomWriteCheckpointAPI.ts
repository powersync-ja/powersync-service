import { WriteCheckpointAPI, WriteCheckpointFilters, WriteCheckpointOptions } from '../write-checkpoint.js';
import { PowerSyncMongo } from './db.js';

/**
 * Implements a write checkpoint API which manages a mapping of
 * `user_id` to a provided incrementing `write_checkpoint` `bigint`.
 */
export class MongoCustomWriteCheckpointAPI implements WriteCheckpointAPI {
  constructor(protected db: PowerSyncMongo) {}

  async batchCreateWriteCheckpoints(checkpoints: WriteCheckpointOptions[]): Promise<void> {
    await this.db.custom_write_checkpoints.bulkWrite(
      checkpoints.map((checkpoint) => ({
        updateOne: {
          filter: { user_id: checkpoint.user_id },
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

  async createWriteCheckpoint(options: WriteCheckpointOptions): Promise<bigint> {
    const { checkpoint, user_id, sync_rules_id } = options;
    const doc = await this.db.custom_write_checkpoints.findOneAndUpdate(
      {
        user_id: user_id
      },
      {
        $set: {
          checkpoint,
          sync_rules_id
        }
      },
      { upsert: true, returnDocument: 'after' }
    );
    return doc!.checkpoint;
  }

  async lastWriteCheckpoint(filters: WriteCheckpointFilters): Promise<bigint | null> {
    const { user_id } = filters;

    const lastWriteCheckpoint = await this.db.custom_write_checkpoints.findOne({
      user_id: user_id
    });
    return lastWriteCheckpoint?.checkpoint ?? null;
  }
}
