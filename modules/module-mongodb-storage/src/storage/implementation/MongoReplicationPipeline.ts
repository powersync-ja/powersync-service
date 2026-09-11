import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { MongoReplicationCoordinator } from './MongoReplicationCoordinator.js';
import { LoadedSourceRecord } from './common/SourceRecordStore.js';
import { SyncRuleDocumentBase } from './models.js';
import { PersistedBatchV3 } from './v3/PersistedBatchV3.js';

const MAX_PENDING_GROUPS = 3;
const MAX_PENDING_BYTES = 64 * 1024 * 1024;

export interface PublicationGroup {
  batch: PersistedBatchV3;
  changes: Map<string, LoadedSourceRecord>;
}

export interface PipelineContext {
  session: mongo.ClientSession;
  state: Map<string, LoadedSourceRecord>;
  published: Map<string, LoadedSourceRecord>;
  lastOp: bigint;
  submittedHead: bigint;
  group?: PublicationGroup;
}

export interface PublicationOptions {
  resumeLsn?: string;
  flushOptions?: storage.BatchBucketFlushOptions;
  checkpoint?: (stream: SyncRuleDocumentBase, lastOp: bigint) => Promise<void>;
}

/**
 * Sequential application, overlapping uploads, and ordered publication. No database
 * lease is acquired here: each publication uses the writer's replication stream fence.
 */
export class MongoReplicationPipeline implements AsyncDisposable {
  private context?: PipelineContext;
  private release?: () => void;
  private yielding?: Promise<void>;
  private admitting?: Promise<void>;
  private preparing?: Promise<void>;
  private pending = new Set<Promise<void>>();
  private bytes = 0;
  private publication: Promise<void> = Promise.resolve();
  private failure: unknown;
  private failed = false;
  private readonly abort = new AbortController();
  readonly signal: AbortSignal;
  readonly uploadSignal: AbortSignal;

  constructor(
    private readonly session: mongo.ClientSession,
    private readonly coordinator: MongoReplicationCoordinator,
    leaseSignal: AbortSignal,
    sourceSignal: AbortSignal | undefined,
    private readonly readHead: () => Promise<bigint>,
    private readonly createBatch: () => PersistedBatchV3,
    private readonly publish: (
      batch: PersistedBatchV3,
      expectedHead: bigint,
      lastOp: bigint,
      options?: PublicationOptions
    ) => Promise<void>
  ) {
    this.signal = AbortSignal.any([leaseSignal, this.abort.signal]);
    // Graceful source cancellation may still finish a durable page boundary.
    // It cancels external PUTs, but only lease loss/disposal forbids publication.
    this.uploadSignal = sourceSignal == null ? this.signal : AbortSignal.any([sourceSignal, this.signal]);
  }

  check(): void {
    if (this.failed) {
      throw this.failure;
    }
    this.signal.throwIfAborted();
  }

  async prepare(callback: (context: PipelineContext) => Promise<void>, before?: () => Promise<void>): Promise<void> {
    await this.yielding;
    this.check();
    if (this.admitting != null) {
      throw new Error('Concurrent publication preparation is not supported');
    }
    const done = Promise.withResolvers<void>();
    this.admitting = done.promise;
    try {
      // Hooks can wait for another writer. Run them before taking write access,
      // and allow any previously prepared group to yield while the hook waits.
      await before?.();
      await this.yielding;
      this.check();
      this.preparing = done.promise;
      if (this.context == null) {
        this.release = await this.coordinator.acquire(() => this.yieldToWriter());
        this.check();
        const head = await this.readHead();
        this.context = {
          session: this.session,
          state: new Map(),
          published: new Map(),
          lastOp: head,
          submittedHead: head
        };
      }
      await callback(this.context);
    } catch (error) {
      this.fail(error);
      throw error;
    } finally {
      this.admitting = undefined;
      this.preparing = undefined;
      if (this.context != null) {
        this.prunePublished(this.context);
      }
      done.resolve();
      this.releaseIfIdle();
    }
  }

  async submit(group: PublicationGroup, options?: PublicationOptions): Promise<storage.BatchProgressReceipt> {
    const size = group.batch.currentSize;
    // One additional group can be retained by preparation. A single indivisible
    // row may exceed the byte limit, but must wait until the window is empty.
    while (
      this.pending.size >= MAX_PENDING_GROUPS ||
      (this.pending.size > 0 && this.bytes + size > MAX_PENDING_BYTES)
    ) {
      await Promise.race(this.pending);
      this.check();
    }
    this.check();
    const context = this.context!;
    const expectedHead = context.submittedHead;
    const lastOp = context.lastOp;
    context.submittedHead = lastOp;
    // All IDs have already been consumed from a majority-persisted reservation.
    const upload = group.batch.prepare((error) => this.fail(error));
    void upload.catch((error) => this.fail(error));
    const previous = this.publication;
    this.bytes += size;
    const work = (async () => {
      try {
        await previous;
        await upload;
        this.check();
        await this.publish(group.batch, expectedHead, lastOp, options);
        for (const [key, value] of group.changes) {
          context.published.set(key, value);
        }
        if (this.preparing == null) {
          this.prunePublished(context);
        }
      } catch (error) {
        this.fail(error);
      } finally {
        // Join every started PUT even if a preceding publication failed.
        await upload.catch(() => {});
        this.bytes -= size;
      }
    })();
    this.publication = work;
    this.pending.add(work);
    void work.then(() => {
      this.pending.delete(work);
      this.releaseIfIdle();
    });
    const persisted = work.then(() => this.check());
    // Callers may await only a later flush or checkpoint barrier.
    void persisted.catch(() => {});
    return { persisted };
  }

  async seal(options?: PublicationOptions, allowEmpty = false): Promise<storage.BatchProgressReceipt | undefined> {
    await this.yielding;
    if (this.context?.group == null && !allowEmpty) {
      return undefined;
    }
    let receipt: storage.BatchProgressReceipt | undefined;
    await this.prepare(async (context) => {
      const group = context.group ?? (allowEmpty ? { batch: this.createBatch(), changes: new Map() } : undefined);
      context.group = undefined;
      if (group != null) {
        receipt = await this.submit(group, options);
      }
    });
    return receipt;
  }

  async drain(): Promise<void> {
    await this.yielding;
    await this.waitForPublications();
  }

  private async waitForPublications(): Promise<void> {
    await Promise.all(this.pending);
    this.check();
    // A later writer may have advanced this stream by the next admission.
    this.releaseIfIdle();
  }

  private async yieldToWriter(): Promise<void> {
    if (this.yielding != null) {
      return this.yielding;
    }
    this.yielding = (async () => {
      try {
        await this.preparing;
        const group = this.context?.group;
        if (group != null) {
          this.context!.group = undefined;
          await this.submit(group);
        }
        await this.waitForPublications();
      } catch (error) {
        this.fail(error);
      } finally {
        if (this.context != null) {
          this.context.group = undefined;
        }
        await Promise.all(this.pending);
        this.releaseIfIdle();
      }
    })();
    try {
      await this.yielding;
    } finally {
      this.yielding = undefined;
    }
  }

  private releaseIfIdle(): void {
    if (this.preparing != null || this.pending.size > 0 || (!this.failed && this.context?.group != null)) {
      return;
    }
    this.context = undefined;
    this.release?.();
    this.release = undefined;
  }

  private fail(error: unknown): void {
    if (!this.failed) {
      this.failed = true;
      this.failure = error;
      this.abort.abort(error);
    }
  }

  private prunePublished(context: PipelineContext): void {
    // Keep committed entries through the entire read/application block: a read
    // started before commit must not replace the overlay with stale membership.
    for (const [key, value] of context.published) {
      if (context.state.get(key) === value) {
        context.state.delete(key);
      }
    }
    context.published.clear();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.abort.abort();
    await this.admitting;
    await this.yielding;
    await Promise.all(this.pending);
    this.context = undefined;
    this.releaseIfIdle();
    await this.session.endSession();
  }
}
