import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { CompactionLeaseLostError } from '../MongoCompactor.js';
import { BucketStateDocumentV3 } from './models.js';

const LEASE_RENEW_INTERVAL_MS = 60 * 1000;

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
        $and: [
          filter,
          {
            // $$NOW is evaluated by MongoDB, avoiding lease expiry races due
            // to clocks on separate compact workers.
            $expr: {
              $or: [{ $eq: [{ $type: '$compact_lease' }, 'missing'] }, { $lte: ['$compact_lease.expires_at', '$$NOW'] }]
            }
          }
        ]
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

  startRenewal() {
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

  /** Allow a retry after a transient error during a fenced final update. */
  restartFinalization() {
    this.finalizing = false;
  }

  async reschedule(nextCompactCheck: mongo.Document) {
    await this.finish([{ $set: { next_compact_check: nextCompactCheck } }, { $unset: 'compact_lease' }]);
  }

  async finalize(update: mongo.Document) {
    await this.finish([{ $set: update }, { $unset: 'compact_lease' }]);
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

  private async finish(update: mongo.Document[]) {
    await this.throwIfLost();
    this.finalizing = true;
    const result = await this.collection.updateOne(this.filter, update);
    if (result.matchedCount != 1) {
      throw new CompactionLeaseLostError(`Lost compaction lease for bucket ${this.state._id.b}`);
    }
    this.finished = true;
  }
}
