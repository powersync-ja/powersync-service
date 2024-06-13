import crypto from 'crypto';

import { PersistedSyncRulesContent, ReplicationLock } from '../BucketStorage.js';
import { PowerSyncMongo } from './db.js';
import { logger } from '@powersync/service-framework';

/**
 * Manages a lock on a sync rules document, so that only one process
 * replicates those sync rules at a time.
 */
export class MongoSyncRulesLock implements ReplicationLock {
  private readonly refreshInterval: NodeJS.Timer;

  static async createLock(db: PowerSyncMongo, sync_rules: PersistedSyncRulesContent): Promise<MongoSyncRulesLock> {
    const lockId = crypto.randomBytes(8).toString('hex');
    const doc = await db.sync_rules.findOneAndUpdate(
      { _id: sync_rules.id, $or: [{ lock: null }, { 'lock.expires_at': { $lt: new Date() } }] },
      {
        $set: {
          lock: {
            id: lockId,
            expires_at: new Date(Date.now() + 60 * 1000)
          }
        }
      },
      {
        projection: { lock: 1 },
        returnDocument: 'before'
      }
    );

    if (doc == null) {
      throw new Error(`Replication slot ${sync_rules.slot_name} is locked by another process`);
    }
    return new MongoSyncRulesLock(db, sync_rules.id, lockId);
  }

  constructor(private db: PowerSyncMongo, public sync_rules_id: number, private lock_id: string) {
    this.refreshInterval = setInterval(async () => {
      try {
        await this.refresh();
      } catch (e) {
        logger.error('Failed to refresh lock', e);
        clearInterval(this.refreshInterval);
      }
    }, 30_130);
  }

  async release(): Promise<void> {
    clearInterval(this.refreshInterval);
    const result = await this.db.sync_rules.updateOne(
      {
        _id: this.sync_rules_id,
        'lock.id': this.lock_id
      },
      {
        $unset: { lock: 1 }
      }
    );
    if (result.modifiedCount == 0) {
      // Log and ignore
      logger.warn(`Lock already released: ${this.sync_rules_id}/${this.lock_id}`);
    }
  }

  private async refresh(): Promise<void> {
    const result = await this.db.sync_rules.findOneAndUpdate(
      {
        _id: this.sync_rules_id,
        'lock.id': this.lock_id
      },
      {
        $set: { 'lock.expires_at': new Date(Date.now() + 60 * 1000) }
      },
      { returnDocument: 'after' }
    );
    if (result == null) {
      throw new Error(`Lock not held anymore: ${this.sync_rules_id}/${this.lock_id}`);
    }
  }
}
