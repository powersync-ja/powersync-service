import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { PerformanceTracer } from '@powersync/service-core';
import { MongoOpIdAllocator } from '../MongoOpIdAllocator.js';
import { MongoSyncRulesLock } from '../MongoSyncRulesLock.js';
import { LoadedSourceRecord } from '../common/SourceRecordStore.js';

/** Accounted work, including packing headroom; not an RSS or single-row limit. */
const MAX_PENDING_BYTES = 64 * 1024 * 1024;

export interface PipelineInputOwner {
  sealPipelineInput(): Promise<unknown>;
}

interface Publication {
  readonly records: ReadonlyMap<string, LoadedSourceRecord>;
  readonly bytes: number;
  done: Promise<void>;
}

/**
 * One publishing batch and one preparing/prepared batch per stream lease.
 * Uploads and MongoDB publication form one sequential stage. Application may
 * read and evaluate its successor concurrently on lock.session under writerMutex.
 * No background task acquires that mutex.
 *
 * Each source writer separately buffers one bounded input block. Only full
 * blocks (or explicit boundaries) enter application: alternating snapshot and
 * streaming saves must not turn those blocks into single-row publications.
 */
export class ReplicationPipelineV3 {
  readonly publicationSession: mongo.ClientSession;
  readonly tracer = new PerformanceTracer<'storage' | 'evaluate'>('MongoDB publication');
  readonly inputOwners = new Set<PipelineInputOwner>();
  private readonly pending: Publication[] = [];
  private reading?: Publication;
  private publishedTime?: mongo.Timestamp;
  private active = false;
  private streamingWork = 0;
  private changed?: { promise: Promise<void>; resolve: () => void };

  /** A live lease never returns from ACTIVE to PROCESSING. */
  markActive(): void {
    if (!this.active) {
      this.active = true;
      this.notifyAdmission();
    }
  }

  private notifyAdmission(): void {
    const changed = this.changed;
    this.changed = undefined;
    changed?.resolve();
  }

  private snapshotMustWait(): boolean {
    return (
      this.active &&
      (this.streamingWork > 0 ||
        this.pending.length >= 2 ||
        (this.pending.length > 0 && this.accountedBytes > MAX_PENDING_BYTES))
    );
  }

  /**
   * ACTIVE snapshots yield to ready streaming blocks and durability operations,
   * not per-row buffering or a lone publication. A queued streaming publication
   * occupies the second slot, so capacity also prevents snapshot admission.
   * Recheck under the mutex, but wait outside it so streaming can make progress.
   * Publication remains FIFO; priority applies only before application.
   */
  async runWriter<T>(snapshot: boolean, callback: () => Promise<T>): Promise<T> {
    if (!snapshot) this.streamingWork++;
    try {
      while (true) {
        this.lock.throwIfAborted();
        // Streaming work allocates no admission notification promise.
        const changed = snapshot ? (this.changed ??= Promise.withResolvers<void>()).promise : null;
        if (!snapshot || !this.snapshotMustWait()) {
          const result = await this.lock.writerMutex.exclusiveLock(async () => {
            this.lock.throwIfAborted();
            if (snapshot && this.snapshotMustWait()) return null;
            return { value: await callback() };
          });
          if (result != null) return result.value;
        }
        // Streaming always executes its callback above; only snapshots defer.
        await this.waitFor(changed!);
      }
    } finally {
      if (!snapshot) this.streamingWork--;
      this.notifyAdmission();
    }
  }

  private async waitFor(promise: Promise<void>): Promise<void> {
    const signal = this.lock.signal;
    const aborted = Promise.withResolvers<void>();
    const onAbort = () => aborted.reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    try {
      signal.throwIfAborted();
      await Promise.race([promise, aborted.promise]);
    } finally {
      signal.removeEventListener('abort', onAbort);
    }
  }

  constructor(
    private readonly lock: MongoSyncRulesLock,
    readonly allocator: MongoOpIdAllocator
  ) {
    this.publicationSession = lock.db.client.startSession({ causalConsistency: true });
  }

  get pendingCount() {
    return this.pending.length;
  }

  get accountedBytes() {
    return (
      this.pending.reduce((sum, batch) => sum + batch.bytes, 0) +
      (this.reading != null && !this.pending.includes(this.reading) ? this.reading.bytes : 0)
    );
  }

  /** Caller holds writerMutex. Reserve the application slot before reading another block. */
  async capacity(bytes = 0): Promise<void> {
    this.lock.throwIfAborted();
    while (this.pending.length >= 2 || (this.pending.length > 0 && this.accountedBytes + bytes > MAX_PENDING_BYTES)) {
      await this.waitFor(this.pending[0].done);
      this.lock.throwIfAborted();
    }
  }

  get overlayEntryCount() {
    const batches = new Set(this.pending);
    if (this.reading != null) batches.add(this.reading);
    return [...batches].reduce((sum, batch) => sum + batch.records.size, 0);
  }

  /** Only application code advances the read session, never a background publisher. */
  advanceReadTime(): void {
    if (this.publishedTime != null) {
      this.lock.session.advanceOperationTime(this.publishedTime);
    }
  }

  /**
   * Capacity guarantees at most one predecessor. Keep its immutable delta alive
   * until application finishes, even when a read straddles its commit. Earlier
   * batches are already durable and covered by the read session's causal clock.
   */
  beginRead(): ReadonlyMap<string, LoadedSourceRecord> {
    this.lock.throwIfAborted();
    if (this.pending.length > 1) {
      throw new ReplicationAssertionError('Application requires an available pipeline slot');
    }
    this.advanceReadTime();
    this.reading = this.pending.at(-1);
    return this.reading?.records ?? new Map();
  }

  endRead(): void {
    this.reading = undefined;
  }

  /** Transfer the prepared batch to the publication stage; no uploads start before its turn. */
  enqueue(
    records: ReadonlyMap<string, LoadedSourceRecord>,
    bytes: number,
    publish: (session: mongo.ClientSession) => Promise<void>
  ): Promise<void> {
    const previous = this.pending.at(-1)?.done;
    const applicationTime = this.lock.session.operationTime;
    const batch: Publication = { records, bytes, done: Promise.resolve() };
    this.pending.push(batch);
    const task = (async () => {
      try {
        await previous;
        this.lock.throwIfAborted();
        if (applicationTime != null) {
          this.publicationSession.advanceOperationTime(applicationTime);
        }
        await publish(this.publicationSession);
        this.publishedTime = this.publicationSession.operationTime;
      } catch (error) {
        this.fail(error);
        throw error;
      } finally {
        this.pending.shift();
        this.notifyAdmission();
      }
    })();
    // Failure aborts the lease before the successor starts. Keep teardown and
    // capacity waits rejection-free; public operations surface the lease error.
    batch.done = task.catch(() => {});
    return task;
  }

  fail(error: unknown): void {
    this.lock.invalidate(
      new ReplicationAbortedError('Replication pipeline failed; restart from persisted progress', error)
    );
  }

  async join(): Promise<void> {
    await this.pending.at(-1)?.done;
  }

  async drain(): Promise<void> {
    await this.join();
    this.lock.throwIfAborted();
    this.advanceReadTime();
  }

  async dispose(): Promise<void> {
    await this.join();
    this.inputOwners.clear();
    await this.publicationSession.endSession();
  }
}
