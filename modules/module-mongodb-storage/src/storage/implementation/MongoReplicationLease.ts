import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { setTimeout } from 'node:timers/promises';
import { VersionedPowerSyncMongo } from './db.js';

// Use the existing lock collection/shape, with a deterministic _id so acquisition
// also works before a name index has been created. All replication writers use
// this lease: reserving IDs is not sufficient to order their eventual commits.
const LOCK_ID = new mongo.ObjectId('000000000000000000000001');
const LEASE_MS = 60_000;

export class MongoReplicationLease implements AsyncDisposable {
  private readonly owner = new mongo.ObjectId();
  private renewal?: ReturnType<typeof globalThis.setInterval>;
  private renewing: Promise<void> = Promise.resolve();
  private error: unknown;

  private constructor(private readonly db: VersionedPowerSyncMongo) {}

  static async acquire(db: VersionedPowerSyncMongo, signal?: AbortSignal): Promise<MongoReplicationLease> {
    const lease = new MongoReplicationLease(db);
    try {
      await db.locks.updateOne(
        { _id: LOCK_ID },
        { $setOnInsert: { name: 'replication-writer' } },
        { upsert: true, writeConcern: { w: 'majority' } }
      );
    } catch (error) {
      // Another process may initialize the same lock at the same time.
      if (!(error instanceof mongo.MongoServerError) || error.code !== 11000) throw error;
    }
    while (true) {
      signal?.throwIfAborted();
      const result = await db.locks.updateOne(
        {
          _id: LOCK_ID,
          $or: [
            { active_lock: { $exists: false } },
            { $expr: { $lte: ['$active_lock.ts', { $subtract: ['$$NOW', LEASE_MS] }] } }
          ]
        },
        [{ $set: { active_lock: { lock_id: lease.owner, ts: '$$NOW' } } }],
        { writeConcern: { w: 'majority' } }
      );
      if (result.matchedCount === 1) break;
      await setTimeout(20, undefined, { signal });
    }
    lease.renewal = globalThis.setInterval(() => {
      lease.renewing = lease.renewing
        .then(() => lease.fence())
        .catch((error) => {
          lease.error = error;
        });
    }, LEASE_MS / 3);
    lease.renewal.unref();
    return lease;
  }

  /** Write the lease inside publication transactions, fencing a replaced owner. */
  async fence(session?: mongo.ClientSession): Promise<void> {
    if (this.error != null) throw this.error;
    const result = await this.db.locks.updateOne(
      { _id: LOCK_ID, 'active_lock.lock_id': this.owner },
      { $currentDate: { 'active_lock.ts': true } },
      { session, ...(session == null ? { writeConcern: { w: 'majority' as const } } : {}) }
    );
    if (result.matchedCount !== 1) throw new ReplicationAssertionError('Replication writer lease was lost');
  }

  async [Symbol.asyncDispose]() {
    globalThis.clearInterval(this.renewal);
    await this.renewing;
    await this.db.locks.updateOne(
      { _id: LOCK_ID, 'active_lock.lock_id': this.owner },
      { $unset: { active_lock: '' } },
      { writeConcern: { w: 'majority' } }
    );
  }
}
