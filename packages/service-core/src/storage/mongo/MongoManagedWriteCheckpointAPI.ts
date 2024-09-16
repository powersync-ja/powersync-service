import { WriteCheckpointAPI, WriteCheckpointFilters, WriteCheckpointOptions } from '../write-checkpoint.js';
import { PowerSyncMongo } from './db.js';

/**
 * Implements a write checkpoint API which manages a mapping of
 * Replication HEAD + `user_id` to a managed incrementing `write_checkpoint` `bigint`.
 */
export class MongoManagedWriteCheckpointAPI implements WriteCheckpointAPI {
  constructor(protected db: PowerSyncMongo) {}

  async createWriteCheckpoint(options: WriteCheckpointOptions): Promise<bigint> {
    const { user_id, heads: lsns } = options;
    const doc = await this.db.write_checkpoints.findOneAndUpdate(
      {
        user_id: user_id
      },
      {
        $set: {
          lsns: lsns
        },
        $inc: {
          client_id: 1n
        }
      },
      { upsert: true, returnDocument: 'after' }
    );
    return doc!.client_id;
  }

  async lastWriteCheckpoint(filters: WriteCheckpointFilters): Promise<bigint | null> {
    const { user_id, heads: lsns } = filters;
    const lsnFilter = Object.fromEntries(
      Object.entries(lsns!).map(([connectionKey, lsn]) => {
        return [`lsns.${connectionKey}`, { $lte: lsn }];
      })
    );

    const lastWriteCheckpoint = await this.db.write_checkpoints.findOne({
      user_id: user_id,
      ...lsnFilter
    });
    return lastWriteCheckpoint?.client_id ?? null;
  }
}
