import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import { PerformanceTracer } from '@powersync/service-core';
import { MongoOpIdAllocator } from '../MongoOpIdAllocator.js';
import { MongoSyncRulesLock } from '../MongoSyncRulesLock.js';
import { LoadedSourceRecord } from '../common/SourceRecordStore.js';

/** Count includes the publishing prefix. Keep one prefix ahead to hide publication latency. */
const MAX_PENDING_PUBLICATIONS = 2;
/**
 * Conservative accounted-work target, including packing headroom. This is not
 * an RSS limit: one application block or oversized row can exceed the target.
 */
const MAX_PENDING_BYTES = 64 * 1024 * 1024;

export interface PipelineInputOwner {
  sealPipelineInput(): Promise<unknown>;
}

/**
 * One per lease, shared even by writers opened through different factories.
 * Application and metadata use writerMutex / lock.session; only the FIFO below
 * uses publicationSession. No background task acquires writerMutex.
 */
export class ReplicationPipelineV3 {
  readonly publicationSession: mongo.ClientSession;
  readonly tracer = new PerformanceTracer<'storage' | 'evaluate'>('MongoDB publication');
  private tail: Promise<void> = Promise.resolve();
  private readonly pending = new Set<Promise<void>>();
  private bytes = 0;
  private readonly overlay = new Map<string, { record: LoadedSourceRecord; generation: symbol }>();
  private readonly committed = new Set<symbol>();
  private reading = false;
  private pinnedBytes = 0;
  private publishedTime?: mongo.Timestamp;
  inputOwner?: PipelineInputOwner;

  constructor(
    private readonly lock: MongoSyncRulesLock,
    readonly allocator: MongoOpIdAllocator,
    private readonly limits = { maxPendingPublications: MAX_PENDING_PUBLICATIONS, maxPendingBytes: MAX_PENDING_BYTES }
  ) {
    this.publicationSession = lock.db.client.startSession({ causalConsistency: true });
  }

  get pendingCount() {
    return this.pending.size;
  }
  get accountedBytes() {
    return this.bytes;
  }

  /** Caller holds writerMutex. Bound work before accepting another input block. */
  async capacity(bytes = 0): Promise<void> {
    this.lock.throwIfAborted();
    while (
      this.pending.size >= this.limits.maxPendingPublications ||
      (this.pending.size > 0 && this.bytes + bytes > this.limits.maxPendingBytes)
    ) {
      await this.waitForCapacityChange();
      this.lock.throwIfAborted();
    }
  }

  private async waitForCapacityChange(): Promise<void> {
    const aborted = Promise.withResolvers<void>();
    const signal = this.lock.signal;
    const onAbort = () => aborted.reject(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    try {
      signal.throwIfAborted();
      await Promise.race([Promise.race(this.pending), aborted.promise]);
    } finally {
      signal.removeEventListener('abort', onAbort);
    }
  }

  get overlayEntryCount() {
    return this.overlay.size;
  }

  /** Only application code advances the read session, never a background publisher. */
  advanceReadTime(): void {
    if (this.publishedTime != null) {
      this.lock.session.advanceOperationTime(this.publishedTime);
    }
  }

  /** Pin versions through reads that can finish after their publication commits. */
  beginRead(): ReadonlyMap<string, LoadedSourceRecord> {
    this.lock.throwIfAborted();
    this.advanceReadTime();
    this.reading = true;
    return new Map([...this.overlay].map(([key, entry]) => [key, entry.record]));
  }

  endRead(): void {
    this.reading = false;
    this.retire();
  }

  private retire(): void {
    if (this.reading) {
      return;
    }
    for (const [key, entry] of this.overlay) {
      if (this.committed.has(entry.generation)) {
        this.overlay.delete(key);
      }
    }
    this.committed.clear();
    this.bytes -= this.pinnedBytes;
    this.pinnedBytes = 0;
  }

  /** Called under writerMutex after application; readiness may run ahead of FIFO publication. */
  enqueue(
    records: ReadonlyMap<string, LoadedSourceRecord>,
    bytes: number,
    ready: Promise<void>,
    publish: (session: mongo.ClientSession) => Promise<void>
  ): Promise<void> {
    const generation = Symbol();
    for (const [key, record] of records) {
      this.overlay.set(key, { record, generation });
    }
    this.bytes += bytes;
    // Handle readiness rejection immediately, even while an earlier publisher is
    // stalled. Every task still joins its readiness during failure teardown.
    const readiness = ready.catch((error: unknown) => {
      this.fail(error);
      throw error;
    });
    void readiness.catch(() => {});
    const previous = this.tail;
    const applicationTime = this.lock.session.operationTime;
    const task = (async () => {
      try {
        await previous;
        await readiness;
        this.lock.throwIfAborted();
        if (applicationTime != null) {
          this.publicationSession.advanceOperationTime(applicationTime);
        }
        await publish(this.publicationSession);
        this.publishedTime = this.publicationSession.operationTime;
        this.committed.add(generation);
        this.retire();
      } catch (error) {
        this.fail(error);
        throw error;
      } finally {
        await readiness.catch(() => {});
      }
    })();
    // The settled tail never leaks an unhandled rejection. Lease failure stops
    // the suffix before publication and is surfaced by every public operation.
    const settled = task
      .then(
        () => {},
        () => {}
      )
      .finally(() => {
        this.pending.delete(settled);
        if (this.reading) {
          this.pinnedBytes += bytes;
        } else {
          this.bytes -= bytes;
        }
      });
    this.pending.add(settled);
    this.tail = settled;
    return task;
  }

  fail(error: unknown): void {
    this.lock.invalidate(
      new ReplicationAbortedError('Replication pipeline failed; restart from persisted progress', error)
    );
  }

  async join(): Promise<void> {
    await this.tail;
    if (this.lock.signal.aborted && !this.reading) {
      this.overlay.clear();
      this.committed.clear();
    }
  }

  async drain(): Promise<void> {
    await this.tail;
    this.lock.throwIfAborted();
    if (this.publishedTime != null) {
      this.lock.session.advanceOperationTime(this.publishedTime);
    }
  }

  async dispose(): Promise<void> {
    await this.tail;
    this.overlay.clear();
    this.committed.clear();
    this.inputOwner = undefined;
    await this.publicationSession.endSession();
  }
}
