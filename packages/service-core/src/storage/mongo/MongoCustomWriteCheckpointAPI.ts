import { WriteCheckpointAPI, WriteCheckpointFilters, WriteCheckpointOptions } from '../write-checkpoint.js';
import { PowerSyncMongo } from './db.js';

/**
 * Implements a write checkpoint API which manages a mapping of
 * `user_id` to a provided incrementing `write_checkpoint` `bigint`.
 */
export class MongoCustomWriteCheckpointAPI implements WriteCheckpointAPI {
  constructor(protected db: PowerSyncMongo) {}

  async createWriteCheckpoint(options: WriteCheckpointOptions): Promise<bigint> {
    const { checkpoint, user_id, heads, sync_rules_id } = options;
    const doc = await this.db.custom_write_checkpoints.findOneAndUpdate(
      {
        user_id: user_id
      },
      {
        $set: {
          heads, // HEADs are technically not relevant, by we can store them if provided
          checkpoint,
          sync_rules_id
        }
      },
      { upsert: true, returnDocument: 'after' }
    );
    return doc!.checkpoint;
  }

  async lastWriteCheckpoint(filters: WriteCheckpointFilters): Promise<bigint | null> {
    const { user_id, heads = {} } = filters;
    const lsnFilter = Object.fromEntries(
      Object.entries(heads).map(([connectionKey, lsn]) => {
        return [`heads.${connectionKey}`, { $lte: lsn }];
      })
    );

    const lastWriteCheckpoint = await this.db.custom_write_checkpoints.findOne({
      user_id: user_id,
      ...lsnFilter
    });
    return lastWriteCheckpoint?.checkpoint ?? null;
  }
}
