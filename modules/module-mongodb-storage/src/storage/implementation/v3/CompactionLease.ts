import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { setTimeout } from 'node:timers/promises';
import { CompactionLeaseLostError } from '../MongoCompactor.js';
import { BucketStateDocumentV3 } from './models.js';

const LEASE_RENEW_INTERVAL_MS = 60 * 1000;

/**
 * We wrap bulk bucket_state updates in a transaction.
 *
 * This is not needed for consistency purposes. Instead, we use this to deprioritize these updates
 * against replication bucket_state updates. If we update concurrently with replication with individual
 * operations, each operation can race the replication transaction individually, and any operation
 * winning can abort the replication transaction. While if we combine it into a transaction, it gives
 * the replication transaction a fair chance to compete.
 */
export function withBucketStateTransaction<T>(
  collection: mongo.Collection<BucketStateDocumentV3>,
  write: (session: mongo.ClientSession) => Promise<T>
): Promise<T> {
  return collection.db.client.withSession((session) => {
    let retry = false;
    return session.withTransaction(
      async () => {
        // After a write conflict, give replication time to finish before retrying.
        // The first MongoDB command opens the transaction on the server.
        if (retry) {
          await setTimeout(Math.random() * 150);
        }
        retry = true;
        return write(session);
      },
      { readPreference: 'primary', readConcern: { level: 'snapshot' }, writeConcern: { w: 'majority' } }
    );
  });
}

/**
 * Filters for buckets that have no lease or an expired lease.
 */
export const AVAILABLE_LEASE_EXPR = {
  // $$NOW is evaluated by MongoDB, avoiding lease expiry races due
  // to clocks on separate compact workers.
  $expr: {
    $or: [{ $eq: [{ $type: '$compact_lease' }, 'missing'] }, { $lte: ['$compact_lease.expires_at', '$$NOW'] }]
  }
};

/**
 * Owns one V3 bucket-compaction lease, including its server-time renewal and
 * the owner-fenced operations which release it.
 *
 * This is intended as a way to reduce redundant work if multiple jobs attempt to
 * compact the same bucket concurrently, but it's not an absolute safety guarantee.
 * The individual operations must still be designed to be safe with concurrent compacting.
 */
export class CompactionLease implements AsyncDisposable {
  readonly startedAt: Date;
  readonly lastOp: InternalOpId;

  private timer: NodeJS.Timeout | undefined;
  private renewalInFlight = false;
  private renewalError: unknown;
  private finalizing = false;
  private finished = false;

  private constructor(
    private readonly collection: mongo.Collection<BucketStateDocumentV3>,
    readonly state: BucketStateDocumentV3,
    readonly id: mongo.ObjectId,
    private readonly durationMs: number
  ) {
    this.startedAt = new Date(state.compact_lease!.expires_at.getTime() - durationMs);
    this.lastOp = state.last_op;
    this.startRenewal();
  }

  static async claim(
    collection: mongo.Collection<BucketStateDocumentV3>,
    filter: mongo.Filter<BucketStateDocumentV3>,
    sort: mongo.Sort | undefined,
    durationMs: number
  ): Promise<CompactionLease | null> {
    const id = new mongo.ObjectId();
    const state = await collection.findOneAndUpdate(
      {
        $and: [filter, AVAILABLE_LEASE_EXPR]
      },
      [
        {
          $set: {
            compact_lease: {
              id,
              expires_at: { $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: durationMs } }
            }
          }
        }
      ],
      { sort, returnDocument: 'after' }
    );
    return state == null ? null : new CompactionLease(collection, state, id, durationMs);
  }

  private startRenewal() {
    const interval = Math.max(1, Math.min(LEASE_RENEW_INTERVAL_MS, Math.floor(this.durationMs / 2)));
    this.timer = setInterval(() => {
      if (this.renewalInFlight) {
        return;
      }
      this.renewalInFlight = true;
      void this.renew()
        .catch((error) => {
          this.renewalError = error;
        })
        .finally(() => {
          this.renewalInFlight = false;
        });
    }, interval);
    this.timer.unref();
  }

  async throwIfLost() {
    if (this.renewalError != null) {
      throw this.renewalError;
    }
  }

  async finalize(fields: mongo.Document) {
    await this.throwIfLost();
    this.finalizing = true;
    const result = await this.collection.updateOne(this.filter, [{ $set: fields }, { $unset: 'compact_lease' }]);
    if (result.matchedCount != 1) {
      throw new CompactionLeaseLostError(`Lost compaction lease for bucket ${this.state._id.b}`);
    }
    this.finished = true;
  }

  /** Each update publishes and releases only the lease that produced it. */
  static async finalizeBatch(
    collection: mongo.Collection<BucketStateDocumentV3>,
    updates: readonly { lease: CompactionLease; fields: mongo.Document }[],
    rescheduleFailed: (lease: CompactionLease, error: unknown) => Promise<void>
  ): Promise<number> {
    if (updates.length === 0) {
      return 0;
    }
    for (const { lease } of updates) {
      lease.finalizing = true;
    }
    let pending = updates;
    while (pending.length > 0) {
      const operations = pending.map(({ lease, fields }) => ({
        updateOne: { filter: lease.filter, update: [{ $set: fields }, { $unset: 'compact_lease' }] }
      }));
      try {
        const result = await withBucketStateTransaction(collection, (session) =>
          collection.bulkWrite(operations, { ordered: false, session })
        );
        // A miss means another worker owns the bucket. Any remaining leases
        // still owned by this run are released by disposal if publication throws.
        for (const { lease } of pending) {
          lease.finished = true;
        }
        return result.matchedCount;
      } catch (error) {
        // A rejected update aborts the transaction. Retry the siblings only
        // when the server identified the invalid updates and the abort is known.
        if (
          !(error instanceof mongo.MongoBulkWriteError) ||
          error.hasErrorLabel('TransientTransactionError') ||
          !Array.isArray(error.writeErrors) ||
          error.writeErrors.length === 0 ||
          error.result.getWriteConcernError() != null
        ) {
          throw error;
        }
        const failed = new Set<number>();
        for (const writeError of error.writeErrors) {
          const { lease } = pending[writeError.index];
          failed.add(writeError.index);
          await lease[Symbol.asyncDispose]();
          await rescheduleFailed(lease, writeError);
        }
        pending = pending.filter((_, index) => !failed.has(index));
      }
    }
    return 0;
  }

  async [Symbol.asyncDispose]() {
    if (this.timer != null) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    if (this.finished) {
      return;
    }
    this.finalizing = true;
    await this.collection.updateOne(this.filter, { $unset: { compact_lease: '' } });
    this.finished = true;
  }

  private get filter(): mongo.Filter<BucketStateDocumentV3> {
    return { _id: this.state._id, 'compact_lease.id': this.id };
  }

  private async renew() {
    const result = await this.collection.updateOne(this.filter, [
      {
        $set: {
          'compact_lease.expires_at': {
            $dateAdd: { startDate: '$$NOW', unit: 'millisecond', amount: this.durationMs }
          }
        }
      }
    ]);
    // A finalization in progress owns the authoritative fenced result.
    if (result.matchedCount != 1 && !this.finalizing) {
      throw new CompactionLeaseLostError(`Lost compaction lease for bucket ${this.state._id.b}`);
    }
    // A successful renewal confirms that a transient failure has passed. A
    // lease-loss error remains sticky: it means another worker may own it.
    if (!(this.renewalError instanceof CompactionLeaseLostError)) {
      this.renewalError = undefined;
    }
  }
}
