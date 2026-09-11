import crypto from 'crypto';

import { mongo } from '@powersync/lib-service-mongodb';
import { ErrorCode, Logger, ReplicationAbortedError, ServiceError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { VersionedPowerSyncMongo } from './db.js';

const LOCK_DURATION_MS = 60 * 1000;

/**
 * Manages a lock on a replication stream document, so that only one process
 * processes that replication stream at a time.
 */
export class MongoSyncRulesLock implements storage.ReplicationLock {
  private readonly abort = new AbortController();
  readonly signal = this.abort.signal;

  private readonly refreshInterval: NodeJS.Timeout;

  /**
   * @param session optional session to create the lock within another transaction
   */
  static async createLock(
    db: VersionedPowerSyncMongo,
    sync_rules: storage.PersistedReplicationStream,
    session?: mongo.ClientSession
  ): Promise<MongoSyncRulesLock> {
    const lockId = crypto.randomBytes(8).toString('hex');
    const doc = await db.sync_rules.findOneAndUpdate(
      {
        _id: sync_rules.replicationStreamId,
        $or: [{ lock: null }, { $expr: { $lt: ['$lock.expires_at', '$$NOW'] } }]
      },
      [
        {
          $set: {
            lock: {
              id: lockId,
              expires_at: { $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: LOCK_DURATION_MS } }
            }
          }
        }
      ],
      {
        projection: { lock: 1 },
        returnDocument: 'before',
        session
      }
    );

    if (doc == null) {
      // Query the existing lock to get the expiration time (best effort - it may have been released in the meantime).
      const heldLock = await db.sync_rules.findOne(
        { _id: sync_rules.replicationStreamId },
        { projection: { lock: 1 }, session }
      );
      if (heldLock?.lock?.expires_at) {
        throw new ServiceError(
          ErrorCode.PSYNC_S1003,
          `Replication stream is locked by another process, standing by. Lock expiring at ${heldLock.lock.expires_at.toISOString()}.`
        );
      } else {
        throw new ServiceError(ErrorCode.PSYNC_S1003, `Replication stream is locked by another process, standing by.`);
      }
    }
    sync_rules.logger.info(`Locked replication stream for processing`);
    return new MongoSyncRulesLock(db, sync_rules.replicationStreamId, lockId, sync_rules.logger);
  }

  constructor(
    private db: VersionedPowerSyncMongo,
    public sync_rules_id: number,
    public readonly lock_id: string,
    private logger: Logger
  ) {
    this.refreshInterval = setInterval(async () => {
      try {
        await this.refresh();
      } catch (e) {
        this.abort.abort(e);
        this.logger.error('Failed to refresh lock', e);
        clearInterval(this.refreshInterval);
      }
    }, 30_130);
  }

  async release(): Promise<void> {
    this.abort.abort(new Error('Replication lock released'));
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
      this.logger.warn(`Lock already released: ${this.sync_rules_id}/${this.lock_id}`);
    }
  }

  throwIfAborted(): void {
    this.signal.throwIfAborted();
  }

  static ownerFilter(streamId: number, lock: MongoSyncRulesLock) {
    lock.throwIfAborted();
    return { _id: streamId, 'lock.id': lock.lock_id };
  }

  static heartbeatUpdate() {
    // Always change the existing heartbeat, even for writes in the same
    // millisecond. A no-op update is not sufficient for a transactional fence.
    return {
      last_keepalive_ts: {
        $max: ['$$NOW', { $add: [{ $ifNull: ['$last_keepalive_ts', new Date(0)] }, 1] }]
      }
    };
  }

  static assertOwned<T>(document: T | null): T {
    if (document == null) {
      throw new ReplicationAbortedError('Replication writer no longer owns the stream');
    }
    return document;
  }

  /**
   * A write, not just an ownership read: takeover conflicts with every transaction
   * that publishes under this owner. Every writer must hold a stream lease.
   */
  static async fence(
    db: VersionedPowerSyncMongo,
    streamId: number,
    lock: MongoSyncRulesLock,
    session: mongo.ClientSession,
    projection: mongo.Document = { last_persisted_op: 1, last_checkpoint: 1, keepalive_op: 1 }
  ) {
    const doc = await db.sync_rules.findOneAndUpdate(
      this.ownerFilter(streamId, lock),
      [{ $set: this.heartbeatUpdate() }],
      { session, returnDocument: 'after', projection }
    );
    return this.assertOwned(doc);
  }

  private async refresh(): Promise<void> {
    const result = await this.db.sync_rules.findOneAndUpdate(
      {
        _id: this.sync_rules_id,
        'lock.id': this.lock_id
      },
      [
        {
          $set: {
            'lock.expires_at': {
              $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: LOCK_DURATION_MS }
            }
          }
        }
      ],
      { returnDocument: 'after' }
    );
    if (result == null) {
      throw new Error(`Lock not held anymore: ${this.sync_rules_id}/${this.lock_id}`);
    }
  }
}
