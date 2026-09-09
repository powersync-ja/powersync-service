import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { PersistedBatch } from './common/PersistedBatch.js';
import { LoadedSourceRecord } from './common/SourceRecordStore.js';
import { VersionedPowerSyncMongo } from './db.js';
import { MongoIdSequence } from './MongoIdSequence.js';
import { MongoReplicationLease } from './MongoReplicationLease.js';

const MAX_PENDING_GROUPS = 3;
const MAX_PENDING_BYTES = 64 * 1024 * 1024;

export interface PipelineContext {
  sequence: MongoIdSequence;
  session: mongo.ClientSession;
  state: Map<string, LoadedSourceRecord>;
  published: Map<string, LoadedSourceRecord>;
  group?: PublicationGroup;
}

export interface PublicationGroup {
  batch: PersistedBatch;
  changes: Map<string, LoadedSourceRecord>;
}

export interface PublicationOptions {
  resumeLsn?: string;
  flushOptions?: storage.BatchBucketFlushOptions;
}

/**
 * One preparation stage, concurrent uploads, and one ordered publication stage.
 * The lease covers all outstanding reservations, including preparation. It is
 * released when the pipeline drains, so another writer can safely use higher IDs.
 */
export class MongoReplicationPipeline implements AsyncDisposable {
  private lease?: MongoReplicationLease;
  private context?: PipelineContext;
  private preparing = false;
  private releasing: Promise<void> = Promise.resolve();
  private pending = new Set<Promise<void>>();
  private bytes = 0;
  private publication: Promise<void> = Promise.resolve();
  private failure: unknown;
  private failed = false;
  private readonly abort = new AbortController();
  readonly signal: AbortSignal;

  constructor(
    private readonly db: VersionedPowerSyncMongo,
    signal: AbortSignal | undefined,
    private readonly publish: (
      batch: PersistedBatch,
      lastOp: bigint,
      session: mongo.ClientSession,
      options?: PublicationOptions
    ) => Promise<void>,
    private readonly committed: (lastOp: bigint) => Promise<void>
  ) {
    this.signal = signal == null ? this.abort.signal : AbortSignal.any([signal, this.abort.signal]);
  }

  check() {
    if (this.failed) throw this.failure;
    this.signal.throwIfAborted();
  }

  async prepare(callback: (context: PipelineContext) => Promise<void>) {
    this.check();
    this.preparing = true;
    try {
      await this.releasing;
      if (this.lease == null) {
        using acquire = storage.ReplicationDiagnostics.active?.span('storage.lease_and_sequence');
        this.lease = await MongoReplicationLease.acquire(this.db, this.signal);
        const sequence = await this.db.op_id_sequence.findOne({ _id: 'main' }, { readConcern: { level: 'majority' } });
        this.context = {
          sequence: new MongoIdSequence(sequence?.op_id ?? 0n),
          session: this.db.client.startSession(),
          state: new Map(),
          published: new Map()
        };
      }
      await callback(this.context!);
    } catch (error) {
      this.fail(error);
      throw error;
    } finally {
      this.preparing = false;
      if (this.context != null) this.prunePublished(this.context);
      await this.releaseIfIdle();
    }
  }

  async submit(
    batch: PersistedBatch,
    lastOp: bigint,
    changes: Map<string, LoadedSourceRecord>,
    options?: PublicationOptions
  ): Promise<storage.BatchProgressReceipt> {
    // The caller may retain one group being prepared, in addition to this bounded
    // window. Like the existing transaction limit, a single row can overshoot the
    // estimated byte target; do not accumulate further rows while backpressured.
    const size = batch.currentSize;
    using capacity = storage.ReplicationDiagnostics.active?.span('publication.capacity_wait');
    while (
      this.pending.size >= MAX_PENDING_GROUPS ||
      (this.pending.size > 0 && this.bytes + size > MAX_PENDING_BYTES)
    ) {
      await Promise.race(this.pending);
      this.check();
    }
    capacity?.end();
    this.check();
    const lease = this.lease!;
    const context = this.context!;
    // Reserve durably before uploading. The lease prevents other writers from
    // reserving/publishing past us, and abandoned reservations remain gaps.
    using reserve = storage.ReplicationDiagnostics.active?.span('storage.reserve_sequence');
    await this.db.op_id_sequence.updateOne(
      { _id: 'main' },
      { $max: { op_id: lastOp } },
      { upsert: true, writeConcern: { w: 'majority' } }
    );
    reserve?.end();
    const upload = batch.prepare();
    // Observe failures immediately even when an earlier group's upload is slow.
    void upload.catch((error) => this.fail(error));
    const previous = this.publication;
    this.bytes += size;
    const work = (async () => {
      try {
        using previousWait = storage.ReplicationDiagnostics.active?.span('publication.previous_wait');
        await previous;
        previousWait?.end();
        using uploadWait = storage.ReplicationDiagnostics.active?.span('publication.upload_wait');
        await upload;
        uploadWait?.end();
        this.check();
        const session = this.db.client.startSession();
        await using sessionLifetime = { [Symbol.asyncDispose]: () => session.endSession() };
        const diagnostics = storage.ReplicationDiagnostics.active;
        if (diagnostics) {
          // This session is private to this publication. Time the driver's actual commit
          // calls without replacing withTransaction's retry/abort handling.
          const commit = session.commitTransaction.bind(session);
          session.commitTransaction = async () => {
            using timing = diagnostics.span('transaction.commit');
            await commit();
          };
          const abort = session.abortTransaction.bind(session);
          session.abortTransaction = async () => {
            using timing = diagnostics.span('transaction.abort');
            await abort();
          };
        }
        using transaction = diagnostics?.span('publication.transaction');
        await session.withTransaction(
          async () => {
            // A retry records another callback attempt, including any failed phase.
            using attempt = diagnostics?.span('transaction.callback');
            this.check();
            using fence = diagnostics?.span('transaction.fence');
            await lease.fence(session);
            fence?.end();
            await this.publish(batch, lastOp, session, options);
          },
          { readConcern: { level: 'snapshot' }, writeConcern: { w: 'majority' }, maxCommitTimeMS: 10000 }
        );
        transaction?.end();
        for (const [key, value] of changes) context.published.set(key, value);
        if (!this.preparing) this.prunePublished(context);
        await this.committed(lastOp);
      } catch (error) {
        this.fail(error);
      } finally {
        // Never release ownership while an abandoned PUT can still be running.
        await upload.catch(() => {});
        this.bytes -= size;
      }
    })();
    this.publication = work;
    this.pending.add(work);
    void work
      .then(async () => {
        this.pending.delete(work);
        await this.releaseIfIdle();
      })
      .catch((error) => this.fail(error));
    const persisted = work.then(() => this.check());
    // A producer may choose to await only the next explicit flush/commit.
    void persisted.catch(() => {});
    return { persisted };
  }

  get hasWork(): boolean {
    return this.context?.group != null || this.pending.size > 0;
  }

  async seal(options?: PublicationOptions): Promise<storage.BatchProgressReceipt | undefined> {
    if (this.context?.group == null) return;
    let receipt: storage.BatchProgressReceipt | undefined;
    await this.prepare(async (context) => {
      const group = context.group!;
      context.group = undefined;
      receipt = await this.submit(group.batch, context.sequence.last(), group.changes, options);
    });
    return receipt;
  }

  async drain() {
    await Promise.all(this.pending);
    await this.releaseIfIdle();
    await this.releasing;
    this.check();
  }

  private fail(error: unknown) {
    if (!this.failed) {
      this.failed = true;
      this.failure = error;
      this.abort.abort(error);
    }
  }

  private prunePublished(context: PipelineContext) {
    // A membership read can race publication. Retain the overlay for the whole
    // preparation block so a read started before commit cannot restore old state.
    for (const [key, value] of context.published) {
      if (context.state.get(key) === value) context.state.delete(key);
    }
    context.published.clear();
  }

  private async releaseIfIdle() {
    if (this.preparing || this.hasWork || this.lease == null) return;
    const lease = this.lease;
    const context = this.context;
    this.lease = undefined;
    this.context = undefined;
    this.releasing = (async () => {
      await context?.session.endSession();
      await lease[Symbol.asyncDispose]();
    })();
    await this.releasing;
  }

  /** Stop admission and uploads before joining an outstanding preparation block. */
  cancel(reason?: unknown) {
    this.abort.abort(reason);
  }

  async [Symbol.asyncDispose]() {
    this.cancel();
    // Unsealed output has no uploads or durable reservations of its own.
    if (this.context != null) this.context.group = undefined;
    await Promise.all(this.pending);
    await this.releaseIfIdle();
    await this.releasing;
  }
}
