import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { Mutex } from 'async-mutex';
import { PersistedBatch } from './common/PersistedBatch.js';
import { PreparedPublication } from './common/PreparedPublication.js';
import { LoadedSourceRecord } from './common/SourceRecordStore.js';
import { SyncRuleDocumentBase } from './models.js';

const MAX_PENDING_GROUPS = 3;
const MAX_PENDING_BYTES = 64 * 1024 * 1024;

type Membership = LoadedSourceRecord | null;
export interface PublicationGroup {
  batch: PersistedBatch;
  changes: Map<string, Membership>;
}

export interface PipelineContext {
  session: mongo.ClientSession;
  state: Map<string, Membership>;
  published: Map<string, Membership>;
  lastOp: bigint;
  submittedHead: bigint;
  group?: PublicationGroup;
}

export interface PublicationOptions {
  resumeLsn?: string;
  flushOptions?: storage.BatchBucketFlushOptions;
  checkpoint?: (stream: SyncRuleDocumentBase, lastOp: bigint) => Promise<void>;
  beforePublish?: (session: mongo.ClientSession) => Promise<void>;
}

export interface PublicationWriterOptions {
  sourceSignal?: AbortSignal;
  readHead(session: mongo.ClientSession): Promise<bigint>;
  createBatch(): PersistedBatch;
  publish(
    publication: PreparedPublication,
    expectedHead: bigint,
    lastOp: bigint,
    options?: PublicationOptions
  ): Promise<void>;
  session: mongo.ClientSession;
}

interface QueueEntry {
  writer: MongoPublicationWriter;
  publication: PreparedPublication;
  expectedHead: bigint;
  lastOp: bigint;
  changes: Map<string, Membership>;
  options?: PublicationOptions;
  completion: ReturnType<typeof Promise.withResolvers<void>>;
}

/**
 * One stream and lease owns one application sequence, membership overlay and FIFO.
 * Application is serialized only while reading/evaluating a block. Uploads and
 * ordered publication continue independently, including across snapshot/CDC writers.
 */
export class MongoReplicationPipeline {
  private readonly admission = new Mutex();
  private readonly writers = new Set<MongoPublicationWriter>();
  private readonly queue: QueueEntry[] = [];
  private readonly abort = new AbortController();
  private session?: mongo.ClientSession;
  private context?: PipelineContext;
  private groupWriter?: MongoPublicationWriter;
  private applying?: MongoPublicationWriter;
  private pumping?: Promise<void>;
  private bytes = 0;
  private failure?: { error: unknown };
  readonly signal: AbortSignal;

  constructor(
    private readonly client: mongo.MongoClient,
    leaseSignal: AbortSignal
  ) {
    this.signal = AbortSignal.any([leaseSignal, this.abort.signal]);
  }

  register(options: PublicationWriterOptions): MongoPublicationWriter {
    const writer = new MongoPublicationWriter(this, options);
    this.writers.add(writer);
    return writer;
  }

  check(): void {
    if (this.failure != null) {
      throw this.failure.error;
    }
    this.signal.throwIfAborted();
  }

  private async initialize(writer: MongoPublicationWriter): Promise<PipelineContext> {
    if (this.context == null) {
      this.session ??= this.client.startSession();
      const head = await writer.options.readHead(this.session);
      this.context = {
        session: this.session,
        state: new Map(),
        published: new Map(),
        lastOp: head,
        submittedHead: head
      };
    }
    return this.context;
  }

  async prepare(writer: MongoPublicationWriter, callback: (context: PipelineContext) => Promise<void>): Promise<void> {
    await this.admission.runExclusive(async () => {
      writer.check();
      this.applying = writer;
      try {
        const context = await this.initialize(writer);
        if (this.groupWriter !== writer) {
          // A writer switch seals the old group, but does not wait for its upload.
          await this.sealCurrent();
          this.groupWriter = writer;
        }
        await callback(context);
      } catch (error) {
        this.fail(error);
        throw error;
      } finally {
        this.applying = undefined;
        this.prunePublished();
        this.resetIfIdle();
      }
    });
  }

  /** Called from the serialized application stage, after IDs have been consumed. */
  async submit(
    writer: MongoPublicationWriter,
    group: PublicationGroup,
    options?: PublicationOptions
  ): Promise<storage.BatchProgressReceipt> {
    const size = group.batch.currentSize;
    // One additional group can be retained by preparation. An indivisible row
    // may exceed the byte limit, but must wait until the window is empty.
    while (
      this.queue.length >= MAX_PENDING_GROUPS ||
      (this.queue.length > 0 && this.bytes + size > MAX_PENDING_BYTES)
    ) {
      await this.queue[0].completion.promise;
      writer.check();
    }
    writer.check();
    const context = this.context!;
    const completion = Promise.withResolvers<void>();
    void completion.promise.catch(() => {});
    const publication = group.batch.seal((error) => this.fail(error));
    void publication.ready.catch((error) => this.fail(error));
    const entry: QueueEntry = {
      writer,
      publication,
      completion,
      options,
      expectedHead: context.submittedHead,
      lastOp: context.lastOp,
      changes: group.changes
    };
    context.submittedHead = context.lastOp;
    this.bytes += size;
    this.queue.push(entry);
    this.startPump();
    const receipt = { persisted: completion.promise };
    writer.receipt = receipt;
    return receipt;
  }

  private startPump(): void {
    if (this.pumping != null) {
      return;
    }
    this.pumping = this.publishQueued().finally(() => {
      this.pumping = undefined;
      this.resetIfIdle();
      if (this.queue.length > 0) {
        this.startPump();
      }
    });
  }

  private async publishQueued(): Promise<void> {
    while (this.queue.length > 0) {
      const entry = this.queue[0];
      try {
        await entry.publication.ready;
        this.check();
        await entry.writer.options.publish(entry.publication, entry.expectedHead, entry.lastOp, entry.options);
        // Reads begun after publication must include it before we reclaim overlay entries.
        const operationTime = entry.writer.options.session.operationTime;
        if (operationTime != null) {
          this.session!.advanceOperationTime(operationTime);
        }
        for (const [key, value] of entry.changes) {
          this.context!.published.set(key, value);
        }
        if (this.applying == null) {
          this.prunePublished();
        }
      } catch (error) {
        this.fail(error);
      }
      // Even on failure, join all PUTs before releasing the entry or its writer.
      await entry.publication.ready.catch(() => {});
      this.queue.shift();
      this.bytes -= entry.publication.size;
      if (this.failure != null) {
        entry.completion.reject(this.failure.error);
      } else {
        entry.completion.resolve();
      }
    }
  }

  private async sealCurrent(options?: PublicationOptions): Promise<storage.BatchProgressReceipt | undefined> {
    const group = this.context?.group;
    if (group == null) {
      return undefined;
    }
    this.context!.group = undefined;
    return this.submit(this.groupWriter!, group, options);
  }

  async seal(
    writer: MongoPublicationWriter,
    options?: PublicationOptions,
    allowEmpty = false
  ): Promise<storage.BatchProgressReceipt | undefined> {
    let own: storage.BatchProgressReceipt | undefined;
    await this.prepare(writer, async (context) => {
      if (context.group == null && allowEmpty) {
        context.group = { batch: writer.options.createBatch(), changes: new Map() };
      }
      own = await this.sealCurrent(options);
      // An empty flush still waits for the prefix preceding its admission, never
      // for work admitted later by another writer.
      const preceding = this.queue.at(-1);
      writer.receipt = own ?? (preceding == null ? writer.receipt : { persisted: preceding.completion.promise });
    });
    return own;
  }

  /** Metadata barriers seal the prefix and prevent application against changing metadata. */
  async exclusive<T>(writer: MongoPublicationWriter, callback: () => Promise<T>): Promise<T> {
    return this.admission.runExclusive(async () => {
      writer.check();
      this.applying = writer;
      try {
        await this.drainPrefix();
        const result = await callback();
        this.context = undefined;
        this.groupWriter = undefined;
        return result;
      } finally {
        this.applying = undefined;
        this.resetIfIdle();
      }
    });
  }

  /** Called under admission when a database scan must include all preceding writes. */
  async drainPrefix(): Promise<void> {
    await this.sealCurrent();
    await this.queue.at(-1)?.completion.promise;
    this.check();
  }

  fail(error: unknown): void {
    if (this.failure == null) {
      this.failure = { error };
      this.abort.abort(error);
    }
  }

  private prunePublished(): void {
    const context = this.context;
    if (context == null) {
      return;
    }
    // Retain published membership through an entire read/application block so
    // a read started before commit cannot restore stale membership. Null entries
    // represent v1/v2 hard deletes and must mask the database result too.
    for (const [key, value] of context.published) {
      if (context.state.get(key) === value) {
        context.state.delete(key);
      }
    }
    context.published.clear();
  }

  private resetIfIdle(): void {
    if (this.applying == null && this.queue.length === 0 && this.context?.group == null) {
      this.context = undefined;
      this.groupWriter = undefined;
    }
  }

  async detach(writer: MongoPublicationWriter): Promise<void> {
    if (
      this.applying === writer ||
      (this.groupWriter === writer && this.context?.group != null) ||
      this.queue.some((entry) => entry.writer === writer)
    ) {
      // Later work may depend on this writer's unpublished membership. Cancelling
      // such a prefix fails the shared suffix; it cannot be silently skipped.
      this.fail(writer.signal.reason);
    }
    await writer.join();
    await this.admission.runExclusive(async () => {
      if (this.failure != null) {
        if (this.context != null) {
          this.context.group = undefined;
        }
        await this.pumping;
      }
      this.writers.delete(writer);
      if (this.writers.size === 0) {
        await this.session?.endSession();
        this.session = undefined;
        this.context = undefined;
        this.groupWriter = undefined;
        // The allocator can be reused by a fresh writer under the same lease.
        // Its owner replaces a failed pipeline before registering that writer.
      }
    });
  }

  get reusable(): boolean {
    return this.failure == null || this.writers.size > 0;
  }
}

/** A writer owns its admission lifetime and receipts, not the stream's executor. */
export class MongoPublicationWriter implements AsyncDisposable {
  private readonly abort = new AbortController();
  private readonly admissions = new Set<Promise<void>>();
  receipt?: storage.BatchProgressReceipt;
  readonly signal: AbortSignal;
  readonly uploadSignal: AbortSignal;

  constructor(
    private readonly pipeline: MongoReplicationPipeline,
    readonly options: PublicationWriterOptions
  ) {
    this.signal = AbortSignal.any([pipeline.signal, this.abort.signal]);
    this.uploadSignal =
      options.sourceSignal == null ? this.signal : AbortSignal.any([options.sourceSignal, this.signal]);
  }

  check(): void {
    this.pipeline.check();
    this.signal.throwIfAborted();
  }

  async prepare(callback: (context: PipelineContext) => Promise<void>, before?: () => Promise<void>): Promise<void> {
    const done = Promise.withResolvers<void>();
    this.admissions.add(done.promise);
    try {
      this.check();
      // Hooks may wait for another writer and therefore run before admission.
      await before?.();
      await this.pipeline.prepare(this, callback);
    } catch (error) {
      if (!this.signal.aborted) {
        this.pipeline.fail(error);
      }
      throw error;
    } finally {
      this.admissions.delete(done.promise);
      done.resolve();
    }
  }

  submit(group: PublicationGroup, options?: PublicationOptions) {
    return this.pipeline.submit(this, group, options);
  }

  seal(options?: PublicationOptions, allowEmpty = false) {
    return this.pipeline.seal(this, options, allowEmpty);
  }

  async drain(): Promise<void> {
    await this.receipt?.persisted;
    this.check();
  }

  /** Only call inside prepare: keep new application out until the scan finishes. */
  drainPrefix(): Promise<void> {
    return this.pipeline.drainPrefix();
  }

  exclusive<T>(callback: () => Promise<T>): Promise<T> {
    return this.pipeline.exclusive(this, callback);
  }

  async join(): Promise<void> {
    await Promise.all(this.admissions);
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.abort.abort(new ReplicationAbortedError('Replication writer disposed'));
    await this.pipeline.detach(this);
  }
}
