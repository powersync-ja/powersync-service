import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { VersionedPowerSyncMongo } from './db.js';
import { MongoIdSequence, OpIdRange } from './MongoIdSequence.js';
import { REPLICATION_WRITE_OPTIONS } from './MongoReplicationOptions.js';

const RESERVATION_SIZE = 65_536n;
const LOW_WATER_MARK = 16_384n;
const MAX_OP_ID = (1n << 63n) - 1n;

/**
 * Shared by writers of one stream and lease owner in this process. Reservations
 * use short majority writes, never the publication session. A crashed owner
 * abandons its unused ranges; the global sequence is an allocation watermark.
 */
export class MongoOpIdAllocator {
  private ranges: OpIdRange[] = [];
  private reserving?: Promise<void>;

  constructor(private readonly db: VersionedPowerSyncMongo) {}

  discard() {
    this.ranges = [];
  }

  sequence(persistedOp: bigint): MongoIdSequence {
    // Transactional writers pass the fenced stream head. Prepared v3/v4 writers
    // pass their last ID within this publication (initially zero). Their per-lease
    // ranges are globally disjoint, and assigned IDs are consumed before publish.
    // Publication asserts that its final ID advances the fenced persisted head.
    return new MongoIdSequence(persistedOp, this.ranges.slice());
  }

  /**
   * Remove IDs assigned to sealed work, even if publication is later abandoned.
   * This only consumes allocation capacity; it does not advance durable progress.
   * Keep the unused tail available to the next publication under this lease.
   */
  consume(lastOp: bigint) {
    this.ranges = this.ranges
      .filter((range) => range.end > lastOp)
      .map((range) => ({ start: range.start > lastOp ? range.start : lastOp + 1n, end: range.end }));
  }

  /** Top up between transactions, retaining every unused ID in existing ranges. */
  async ensureCapacity(): Promise<void> {
    const remaining = this.ranges.reduce((total, range) => total + range.end - range.start + 1n, 0n);
    if (remaining < LOW_WATER_MARK) {
      await this.reserve();
    }
  }

  async reserve(): Promise<void> {
    if (this.reserving != null) {
      return this.reserving;
    }
    this.reserving = this.reserveRange();
    try {
      await this.reserving;
    } finally {
      this.reserving = undefined;
    }
  }

  /**
   * Atomically reserve a range of operation IDs in the global sequence.
   *
   * We continue using the range across multiple batches, until the range is exhausted,
   * so we don't "waste" large segements in long-running processes with small batches.
   * However, we can't return unused portions back to the database if we don't use them.
   */
  private async reserveRange(): Promise<void> {
    try {
      // Return the previous watermark so an unchanged value at the limit can be
      // distinguished from a successful reservation ending near the limit.
      const previous = await this.db.op_id_sequence.findOneAndUpdate(
        { _id: 'main' },
        [
          {
            $set: {
              op_id: {
                $let: {
                  vars: { current: { $ifNull: ['$op_id', 0n] } },
                  in: {
                    $cond: [
                      { $lte: ['$$current', MAX_OP_ID - RESERVATION_SIZE] },
                      { $add: ['$$current', RESERVATION_SIZE] },
                      '$$current'
                    ]
                  }
                }
              }
            }
          }
        ],
        { upsert: true, returnDocument: 'before', ...REPLICATION_WRITE_OPTIONS }
      );
      const last = previous?.op_id ?? 0n;
      if (last > MAX_OP_ID - RESERVATION_SIZE) {
        // Not expected to ever happen, unless the sequence was manually modified
        throw new ReplicationAssertionError('Operation ID sequence exhausted');
      }
      this.ranges.push({ start: last + 1n, end: last + RESERVATION_SIZE });
    } catch (error) {
      // Concurrent first reservations may race to insert the sequence document - retry in that case.
      if (error instanceof mongo.MongoServerError && error.code === 11000) {
        return this.reserveRange();
      }
      throw error;
    }
  }
}
