import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { PerformanceTracer } from '@powersync/service-core';
import { LoadedSourceRecord } from '../common/SourceRecordStore.js';
import { MongoOpIdAllocator } from '../MongoOpIdAllocator.js';
import { MongoSyncRulesLock } from '../MongoSyncRulesLock.js';
import { MAX_PIPELINE_BATCHES, MAX_PIPELINE_BYTES } from '../ReplicationBatchLimits.js';
import type { MongoBucketBatchV3 } from './MongoBucketBatchV3.js';

interface Publication {
  readonly records: ReadonlyMap<string, LoadedSourceRecord>;
  readonly bytes: number;
  done: Promise<void>;
}

/**
 * Two stages, with at most two in-flight batches per lease:
 * 1. Preparation reads stored source records, evaluates changes and seals writes
 *    on lock.session under writerMutex. After admission, background preparation
 *    packs bucket operations and uploads offloaded chunks to object storage (e.g. S3).
 * 2. Publishing commits the prepared MongoDB writes and object references, in FIFO
 *    order on a separate session. The successor can read/evaluate, pack and upload
 *    while its predecessor uploads or publishes. Both batches share the existing
 *    object-storage request limiter; there is no additional upload queue.
 * Reads/evaluation and MongoDB publishing each remain serialized. Both slots can
 * be uploading at once. No background task acquires writerMutex or uses lock.session.
 *
 * Each source writer separately buffers one bounded input block. Only full
 * blocks (or explicit boundaries) enter preparation: alternating snapshot and
 * streaming saves must not turn those blocks into single-row publications.
 */
export class ReplicationPipelineV3 {
  readonly publicationSession: mongo.ClientSession;
  readonly tracer = new PerformanceTracer<'storage' | 'evaluate'>('MongoDB publication');
  /**
   * Live writers can buffer raw rows/checkpoints that are not yet in `pending`.
   * Metadata barriers must include all of those buffers before changing table state.
   * Writers register at construction and unregister on disposal, not on each save.
   */
  private readonly writers = new Set<MongoBucketBatchV3>();
  private readonly pending: Publication[] = [];
  private reading?: Publication;
  /**
   * MongoDB operationTime from the last successful queued publication. The
   * publisher records it here so preparation can copy its causal history from
   * publicationSession into lock.session before reading stored source records.
   */
  private publishedTime?: mongo.Timestamp;

  constructor(
    private readonly lock: MongoSyncRulesLock,
    readonly allocator: MongoOpIdAllocator
  ) {
    this.publicationSession = lock.db.client.startSession({ causalConsistency: true });
  }

  registerWriter(writer: MongoBucketBatchV3): void {
    this.writers.add(writer);
  }

  unregisterWriter(writer: MongoBucketBatchV3): void {
    this.writers.delete(writer);
  }

  /**
   * Synchronously capture and queue every writer's current input. The caller
   * then queues its metadata operation on writerMutex and drains publication
   * there. Do not await between writers: rows appended after this boundary must
   * stay behind the metadata change, even while earlier inputs are being prepared.
   */
  enqueueBufferedInputs(): void {
    for (const writer of this.writers) {
      void writer.flushBufferedInputToPipeline().catch((error) => this.fail(error));
    }
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

  /** Caller holds writerMutex. Reserve the preparation slot before reading another block. */
  async capacity(bytes = 0): Promise<void> {
    this.lock.throwIfAborted();
    while (
      this.pending.length >= MAX_PIPELINE_BATCHES ||
      (this.pending.length > 0 && this.accountedBytes + bytes > MAX_PIPELINE_BYTES)
    ) {
      const signal = this.lock.signal;
      const aborted = Promise.withResolvers<void>();
      const onAbort = () => aborted.reject(signal.reason);
      signal.addEventListener('abort', onAbort, { once: true });
      try {
        signal.throwIfAborted();
        await Promise.race([this.pending[0].done, aborted.promise]);
      } finally {
        signal.removeEventListener('abort', onAbort);
      }
      this.lock.throwIfAborted();
    }
  }

  get overlayEntryCount() {
    const batches = new Set(this.pending);
    if (this.reading != null) {
      batches.add(this.reading);
    }
    return [...batches].reduce((sum, batch) => sum + batch.records.size, 0);
  }

  /**
   * Caller holds writerMutex. Advance the preparation session's MongoDB logical
   * time so subsequent majority reads include completed publications from the
   * separate publication session. Later batches can read that persisted state
   * without retaining every earlier batch's delta. A predecessor whose publication
   * overlaps the current preparation still needs the pinned overlay from beginRead().
   *
   * Only preparation code advances the read session. The background publisher
   * records publishedTime instead of mutating lock.session during an active read
   * or transaction. Advancing the clock neither waits for nor flushes queued work.
   */
  advanceReadTime(): void {
    if (this.publishedTime != null) {
      this.lock.session.advanceOperationTime(this.publishedTime);
    }
  }

  /**
   * Capacity guarantees at most one predecessor. Keep its immutable delta alive
   * until preparation finishes, even when a read straddles its commit. Earlier
   * batches are already durable and covered by the read session's causal clock.
   */
  beginRead(): ReadonlyMap<string, LoadedSourceRecord> {
    this.lock.throwIfAborted();
    if (this.pending.length > 1) {
      throw new ReplicationAssertionError('Preparation requires an available pipeline slot');
    }
    this.advanceReadTime();
    this.reading = this.pending.at(-1);
    return this.reading?.records ?? new Map();
  }

  endRead(): void {
    this.reading = undefined;
  }

  /** Own the sealed batch before starting uploads; only MongoDB publication waits for its turn. */
  enqueue(
    records: ReadonlyMap<string, LoadedSourceRecord>,
    bytes: number,
    preparePayloads: () => Promise<void>,
    publish: (session: mongo.ClientSession) => Promise<void>
  ): Promise<void> {
    const previous = this.pending.at(-1)?.done;
    const preparationTime = this.lock.session.operationTime;
    const batch: Publication = { records, bytes, done: Promise.resolve() };
    this.pending.push(batch);
    // Start payload work as soon as this batch is accounted for. Observe failure
    // immediately, even while the predecessor is blocked in MongoDB. Keep this
    // promise fulfilled: the lease carries the error, and FIFO teardown must join
    // both the predecessor and all started uploads before removing this batch.
    const payloads = Promise.resolve()
      .then(() => {
        this.lock.throwIfAborted();
        return preparePayloads();
      })
      .catch((error) => this.fail(error));
    const task = (async () => {
      try {
        await previous;
        await payloads;
        this.lock.throwIfAborted();
        if (preparationTime != null) {
          this.publicationSession.advanceOperationTime(preparationTime);
        }
        await publish(this.publicationSession);
        // Hand off the confirmed publication time before removing the batch from pending.
        this.publishedTime = this.publicationSession.operationTime;
      } catch (error) {
        this.fail(error);
        throw error;
      } finally {
        this.pending.shift();
      }
    })();
    // Failure aborts the lease before the successor can publish. Keep teardown and
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
    this.writers.clear();
    await this.publicationSession.endSession();
  }
}
