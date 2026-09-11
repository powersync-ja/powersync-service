import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAbortedError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { Mutex } from 'async-mutex';
import { PersistedBatch } from './common/PersistedBatch.js';
import { PreparedPublication } from './common/PreparedPublication.js';
import { LoadedSourceRecord } from './common/SourceRecordStore.js';
import { SyncRuleDocumentBase } from './models.js';
import { MongoIdSequence, OpIdRangeExhausted } from './MongoIdSequence.js';
import type { MongoOpIdAllocator } from './MongoOpIdAllocator.js';

const MAX_PENDING_GROUPS = 3;
const MAX_PENDING_BYTES = 64 * 1024 * 1024;

type Membership = LoadedSourceRecord | null;
interface PublicationGroup {
  writer: MongoPublicationWriter;
  batch: PersistedBatch;
  changes: Map<string, Membership>;
}

interface PipelineContext {
  session: mongo.ClientSession;
  state: Map<string, Membership>;
  published: Map<string, Membership>;
  lastOp: bigint;
  submittedHead: bigint;
  group?: PublicationGroup;
}

/** Valid only during the admitted callback. Mutable stream state stays in the pipeline. */
export interface PublicationApplication {
  readonly session: mongo.ClientSession;
  readonly lastOp: bigint;
  lookup(key: string, loaded: LoadedSourceRecord | undefined): Membership;
  overlaySizes(sizes: Map<string, number>): void;
  applyRow<T>(apply: (row: PersistedBatch, sequence: MongoIdSequence) => T): Promise<T>;
  recordMembership(key: string, value: Membership): void;
  publishIfFull(eager?: boolean): Promise<void>;
}

export type PublicationReceipt<T = void> =
  | { published: false; persisted: Promise<void> }
  | { published: true; persisted: Promise<T> };

export interface PublicationOptions<T = void> {
  resumeLsn?: string;
  flushOptions?: storage.BatchBucketFlushOptions;
  checkpoint?: (stream: SyncRuleDocumentBase, lastOp: bigint) => Promise<T>;
  beforePublish?: (session: mongo.ClientSession) => Promise<void>;
}

export interface PublicationWriterOptions {
  sourceSignal?: AbortSignal;
  allocator: MongoOpIdAllocator;
  readHead(session: mongo.ClientSession): Promise<bigint>;
  createBatch(): PersistedBatch;
  publish<T>(
    publication: PreparedPublication,
    expectedHead: bigint,
    lastOp: bigint,
    options?: PublicationOptions<T>
  ): Promise<T | undefined>;
  session: mongo.ClientSession;
}

interface QueueEntry {
  writer: MongoPublicationWriter;
  publication: PreparedPublication;
  expectedHead: bigint;
  lastOp: bigint;
  changes: Map<string, Membership>;
  options?: PublicationOptions<unknown>;
  completion: ReturnType<typeof Promise.withResolvers<unknown>>;
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

  private async admit<T>(
    writer: MongoPublicationWriter,
    callback: (context: PipelineContext) => Promise<T>
  ): Promise<T> {
    return this.admission.runExclusive(async () => {
      writer.check();
      this.applying = writer;
      try {
        const context = await this.initialize(writer);
        if (context.group != null && context.group.writer !== writer) {
          // A writer switch seals the old group, but does not wait for its upload.
          await this.sealCurrent();
        }
        return await callback(context);
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

  async prepare(
    writer: MongoPublicationWriter,
    callback: (application: PublicationApplication) => Promise<void>
  ): Promise<void> {
    return this.admit(writer, (context) => callback(this.application(writer, context)));
  }

  /** A table scan must include rows that exist only in preceding pending groups. */
  async scan(
    writer: MongoPublicationWriter,
    callback: (application: PublicationApplication) => Promise<void>
  ): Promise<void> {
    return this.admit(writer, async (context) => {
      await this.drainPrefix();
      await callback(this.application(writer, context));
    });
  }

  private group(writer: MongoPublicationWriter, context: PipelineContext): PublicationGroup {
    return (context.group ??= { writer, batch: writer.options.createBatch(), changes: new Map() });
  }

  private application(writer: MongoPublicationWriter, context: PipelineContext): PublicationApplication {
    return {
      session: context.session,
      get lastOp() {
        return context.lastOp;
      },
      lookup: (key, loaded) => (context.state.has(key) ? context.state.get(key)! : (loaded ?? null)),
      overlaySizes: (sizes) => {
        for (const [key, value] of context.state) {
          sizes.set(key, value?.data?.length() ?? 0);
        }
      },
      applyRow: (apply) => this.applyRow(writer, context, apply),
      recordMembership: (key, value) => {
        context.state.set(key, value);
        this.group(writer, context).changes.set(key, value);
      },
      publishIfFull: async (eager = false) => {
        if (context.group?.batch.shouldPublish()) {
          const receipt = await this.sealCurrent();
          if (eager) {
            await receipt?.persisted;
          }
        }
      }
    };
  }

  private async applyRow<T>(
    writer: MongoPublicationWriter,
    context: PipelineContext,
    apply: (row: PersistedBatch, sequence: MongoIdSequence) => T
  ): Promise<T> {
    const allocator = writer.options.allocator;
    await allocator.ensureCapacity();
    for (;;) {
      writer.check();
      const sequence = allocator.sequence(context.lastOp);
      const row = writer.options.createBatch();
      let result: T;
      try {
        result = apply(row, sequence);
        if (row.currentSize > 0 && sequence.last() === context.lastOp) {
          // Membership-only changes must also advance the prepared prefix.
          sequence.next();
        }
      } catch (error) {
        if (!(error instanceof OpIdRangeExhausted)) {
          throw error;
        }
        // This row has not touched the group or overlay. Reuse its reserved IDs
        // after extending the range; earlier rows and uploads remain immutable.
        await allocator.reserve();
        continue;
      }
      context.lastOp = sequence.last();
      // Consume now, before yielding: other writers sharing this allocator must
      // never use IDs assigned to unpublished operations.
      allocator.consume(context.lastOp);
      this.group(writer, context).batch.append(row);
      return result;
    }
  }

  /** Called from the serialized application stage, after IDs have been consumed. */
  private async submit(
    group: PublicationGroup,
    options?: PublicationOptions<unknown>
  ): Promise<PublicationReceipt<unknown>> {
    const writer = group.writer;
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
    const completion = Promise.withResolvers<unknown>();
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
    return { published: true, persisted: completion.promise };
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
      let result: unknown;
      try {
        await entry.publication.ready;
        this.check();
        result = await entry.writer.options.publish(entry.publication, entry.expectedHead, entry.lastOp, entry.options);
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
        entry.completion.resolve(result);
      }
    }
  }

  private async sealCurrent(options?: PublicationOptions<unknown>): Promise<PublicationReceipt<unknown> | undefined> {
    const group = this.context?.group;
    if (group == null) {
      return undefined;
    }
    this.context!.group = undefined;
    return this.submit(group, options);
  }

  async seal<T = void>(
    writer: MongoPublicationWriter,
    options?: PublicationOptions<T>,
    allowEmpty = false
  ): Promise<PublicationReceipt<T>> {
    return this.admit(writer, async (context) => {
      if (allowEmpty) {
        this.group(writer, context);
      }
      const own = await this.sealCurrent(options);
      // An empty flush still waits for the prefix preceding its admission, never
      // for work admitted later by another writer. Ignore that prefix's result.
      const persisted = (own?.persisted ?? this.queue.at(-1)?.completion.promise ?? Promise.resolve()).then((value) => {
        writer.check();
        return own == null ? undefined : value;
      });
      // Receipts may be awaited later, after other source pages are admitted.
      void persisted.catch(() => {});
      if (own == null) {
        return { published: false, persisted: persisted as Promise<void> };
      }
      // The queue contains heterogeneous results; this entry was submitted with options<T>.
      return { published: true, persisted: persisted as Promise<T> };
    });
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
        return result;
      } finally {
        this.applying = undefined;
        this.resetIfIdle();
      }
    });
  }

  /** Called under admission when a database scan must include all preceding writes. */
  private async drainPrefix(): Promise<void> {
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
    }
  }

  async detach(writer: MongoPublicationWriter): Promise<void> {
    if (
      this.applying === writer ||
      this.context?.group?.writer === writer ||
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

  async prepare(
    callback: (application: PublicationApplication) => Promise<void>,
    before?: () => Promise<void>
  ): Promise<void> {
    return this.track(async () => {
      // Hooks may wait for another writer and therefore run before admission.
      await before?.();
      await this.pipeline.prepare(this, callback);
    });
  }

  private async track(callback: () => Promise<void>): Promise<void> {
    const done = Promise.withResolvers<void>();
    this.admissions.add(done.promise);
    try {
      this.check();
      await callback();
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

  seal<T = void>(options?: PublicationOptions<T>, allowEmpty = false) {
    return this.pipeline.seal(this, options, allowEmpty);
  }

  scan(callback: (application: PublicationApplication) => Promise<void>): Promise<void> {
    return this.track(() => this.pipeline.scan(this, callback));
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
