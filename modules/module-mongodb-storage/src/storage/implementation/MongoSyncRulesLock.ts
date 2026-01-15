import crypto from 'crypto';

import { ErrorCode, logger, ServiceError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { PowerSyncMongo } from './db.js';
import { mongo } from '@powersync/lib-service-mongodb';

/**
 * Manages a lock on a sync rules document, so that only one process
 * replicates those sync rules at a time.
 */
export class MongoSyncRulesLock implements storage.ReplicationLock {
  private readonly refreshInterval: NodeJS.Timeout;

  static async createLock(
    db: PowerSyncMongo,
    sync_rules: storage.PersistedSyncRulesContent,
    session?: mongo.ClientSession
  ): Promise<MongoSyncRulesLock> {
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
        returnDocument: 'before',
        session
      }
    );

    if (doc == null) {
      // Query the existing lock to get the expiration time (best effort - it may have been released in the meantime).
      // We don't use the session here - we want to see the latest state.
      const heldLock = await db.sync_rules.findOne({ _id: sync_rules.id }, { projection: { lock: 1 } });
      if (heldLock?.lock?.expires_at) {
        throw new ServiceError(
          ErrorCode.PSYNC_S1003,
          `Sync rules: ${sync_rules.id} have been locked by another process for replication, expiring at ${heldLock.lock.expires_at.toISOString()}.`
        );
      } else {
        throw new ServiceError(
          ErrorCode.PSYNC_S1003,
          `Sync rules: ${sync_rules.id} have been locked by another process for replication.`
        );
      }
    }
    return new MongoSyncRulesLock(db, sync_rules.id, lockId);
  }

  constructor(
    private db: PowerSyncMongo,
    public sync_rules_id: number,
    private lock_id: string
  ) {
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
