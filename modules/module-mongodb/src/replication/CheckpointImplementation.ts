import { mongo } from '@powersync/lib-service-mongodb';
import { Logger, ServiceAssertionError } from '@powersync/lib-services-framework';
import { ReplicationHeadCallback, storage } from '@powersync/service-core';

import { MongoLSN } from '../common/MongoLSN.js';
import { normalizeSentinel, SentinelLSN } from '../common/SentinelLSN.js';
import { ChangeStreamInvalidatedError } from './ChangeStream.js';
import { createCheckpoint, createCosmosCheckpointLsn, STANDALONE_CHECKPOINT_ID } from './MongoRelation.js';
import { ProjectedChangeStreamDocument } from './RawChangeStream.js';
import { CHECKPOINTS_COLLECTION, timestampToDate } from './replication-utils.js';

/**
 * Classification of an event on the `_powersync_checkpoints` collection.
 *
 * - 'standalone': source-wide checkpoint (write checkpoints, keepalive bumps,
 *   snapshot markers). Processed regardless of which process created it.
 * - 'own-barrier': this stream's private batch barrier document.
 * - 'foreign': another stream's barrier document — ignore.
 */
export type CheckpointEventKind = 'standalone' | 'own-barrier' | 'foreign';

export interface StreamResumePosition {
  resumeAfter: mongo.ResumeToken | null;
  /**
   * Legacy startAtOperationTime fallback. Only produced by the timestamp implementation,
   * and only for old LSNs persisted without a resume token.
   */
  startAfter: mongo.Timestamp | null;
}

export interface CheckpointImplementationContext {
  client: mongo.MongoClient;
  db: mongo.Db;
  checkpointStreamId: mongo.ObjectId;
  logger: Logger;
}

/**
 * Strategy for producing and interpreting replication checkpoints.
 *
 * Two implementations exist:
 *
 * - {@link TimestampCheckpointImplementation}: standard MongoDB. The ordered LSN
 *   coordinate is the oplog clusterTime, which is unique per operation and
 *   parseable from resume tokens.
 * - {@link SentinelCheckpointImplementation}: for sources without a usable clusterTime
 *   (Cosmos DB, and technically usable on any MongoDB). The ordered coordinate
 *   is a shared monotonic counter document, observed through the change
 *   stream itself.
 *
 * An implementation instance belongs to a single ChangeStream (or API adapter) and may
 * hold per-stream coordinate state; call {@link seedPosition} at the start of
 * each streaming loop.
 */
export interface CheckpointImplementation {
  /** LSN representing "before any data". */
  readonly zeroLsn: string;

  /**
   * Whether markers returned by {@link createBatchCheckpoint} are comparable
   * LSNs that can also seed a change stream position (timestamp implementation), as
   * opposed to opaque content-matched markers (sentinel implementation).
   */
  readonly barrierMarkerIsLsn: boolean;

  /**
   * Whether the implementation has a usable coordinate for building LSNs. The sentinel
   * mode has no position until it has observed a checkpoint event or was
   * seeded from a stored LSN.
   */
  hasPosition(): boolean;

  /** Parse a stored LSN into change stream resume options. Pure. */
  parseResumePosition(lsn: string): StreamResumePosition;

  /** Reset/seed the implementation's coordinate state for a new stream loop. */
  seedPosition(lsn: string | null): void;

  /** Log the resume position at the start of a streaming loop. */
  logResume(lsn: string): void;

  /**
   * Create a source-wide consistency checkpoint (snapshot boundaries,
   * `no_checkpoint_before` markers). Returns a comparable LSN.
   */
  createStandaloneCheckpoint(): Promise<string>;

  /**
   * Create a batch barrier for this stream. Returns a marker that is resolved
   * by a later change stream event via {@link barrierResolved}.
   */
  createBatchCheckpoint(): Promise<string>;

  /**
   * Idle keepalive for an empty change stream batch. May persist a checkpoint
   * directly (timestamp implementation) or nudge the source so that a later event
   * commits (sentinel implementation).
   */
  keepalive(batch: storage.BucketStorageBatch, resumeToken: mongo.ResumeToken): Promise<void>;

  /**
   * Build a comparable resume LSN from a bare batch-level resume token (no
   * change event). Used for the per-batch `setResumeLsn` progress marker.
   *
   * The timestamp implementation parses the timestamp embedded in the token.
   * The sentinel implementation pairs the token with the current coordinate.
   */
  resumeLsnFromToken(resumeToken: mongo.ResumeToken): string;

  /**
   * Source-side replication head for write checkpoints. The LSN passed to the
   * callback must compare at or below any LSN committed after the caller's
   * preceding writes.
   */
  createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T>;

  /**
   * Classify a checkpoint-collection event and absorb any coordinate it
   * carries (sentinel implementation). Must be called for every checkpoint event, in
   * stream order.
   */
  observeCheckpointEvent(doc: ProjectedChangeStreamDocument): CheckpointEventKind;

  /** Comparable LSN for committing/resuming at this event. */
  eventLsn(doc: ProjectedChangeStreamDocument): string;

  /** Whether a barrier marker from {@link createBatchCheckpoint} is resolved by this event. */
  barrierResolved(marker: string, doc: ProjectedChangeStreamDocument): boolean;

  /**
   * Called when an event LSN compares below the last committed LSN. Returns
   * true when this is tolerable (sentinel implementation: equal coordinate, opaque
   * token suffix tie — the commit no-ops in storage) instead of a fatal
   * ordering violation.
   */
  isTolerableDescendingLsn(lsn: string, lastCheckpointLsn: string): boolean;

  /** Human-readable event position for error messages. */
  describeEventPosition(doc: ProjectedChangeStreamDocument): string;

  /** Filter for clearing the checkpoints collection on startup. */
  checkpointClearFilter(): mongo.Filter<mongo.Document>;
}

/**
 * Extract the event timestamp: clusterTime when present, otherwise wallTime
 * truncated to second precision.
 */
export function getEventTimestamp(changeDocument: ProjectedChangeStreamDocument): mongo.Timestamp {
  if (changeDocument.clusterTime) {
    return changeDocument.clusterTime;
  }
  const wallTime = (changeDocument as any).wallTime as Date | undefined;
  if (wallTime != null) {
    return mongo.Timestamp.fromBits(0, Math.floor(wallTime.getTime() / 1000));
  }
  throw new Error('Change event has neither clusterTime nor wallTime');
}

function getCheckpointId(doc: ProjectedChangeStreamDocument): string | mongo.ObjectId | null {
  if (!('documentKey' in doc)) {
    return null;
  }
  return doc.documentKey._id as string | mongo.ObjectId;
}

function deserializeFullDocument(doc: ProjectedChangeStreamDocument): mongo.Document | null {
  const fullDocument = 'fullDocument' in doc ? doc.fullDocument : null;
  if (!fullDocument) {
    return null;
  }
  // fullDocument is a raw BSON Buffer from parseChangeDocument.
  return mongo.BSON.deserialize(fullDocument as Buffer, { useBigInt64: true });
}

/**
 * Standard MongoDB checkpoint implementation. The ordered LSN coordinate is the oplog
 * clusterTime — unique per operation, monotonic, and parseable from resume
 * tokens. Barriers and event LSNs are plain comparable LSN strings.
 */
export class TimestampCheckpointImplementation implements CheckpointImplementation {
  readonly zeroLsn = MongoLSN.ZERO.comparable;
  readonly barrierMarkerIsLsn = true;

  constructor(private context: CheckpointImplementationContext) {}

  hasPosition(): boolean {
    return true;
  }

  parseResumePosition(lsn: string): StreamResumePosition {
    const parsed = MongoLSN.fromSerialized(lsn);
    return { resumeAfter: parsed.resumeToken ?? null, startAfter: parsed.timestamp };
  }

  seedPosition(_lsn: string | null): void {
    // The coordinate comes from each event's clusterTime; no state to seed.
  }

  logResume(lsn: string): void {
    const parsed = MongoLSN.fromSerialized(lsn);
    // It is normal for this to be a minute or two old when there is a low volume
    // of ChangeStream events.
    const tokenAgeSeconds = Math.round((Date.now() - timestampToDate(parsed.timestamp).getTime()) / 1000);
    this.context.logger.info(
      `Resume streaming at ${parsed.timestamp.inspect()} / ${parsed} | Token age: ${tokenAgeSeconds}s`
    );
  }

  async createStandaloneCheckpoint(): Promise<string> {
    return createCheckpoint(this.context.client, this.context.db, STANDALONE_CHECKPOINT_ID);
  }

  async createBatchCheckpoint(): Promise<string> {
    return createCheckpoint(this.context.client, this.context.db, this.context.checkpointStreamId);
  }

  async keepalive(batch: storage.BucketStorageBatch, resumeToken: mongo.ResumeToken): Promise<void> {
    // Parse the timestamp from the resume token. The ordered LSN prefix
    // advances together with the token, so persisting is always safe.
    const { comparable: lsn, timestamp } = MongoLSN.fromResumeToken(resumeToken);
    await batch.keepalive(lsn);
    // Log the token update. This helps as a general "replication is still active" message in the logs.
    // This token would typically be around 10s behind.
    this.context.logger.info(
      `Idle change stream. Persisted resumeToken for ${timestampToDate(timestamp).toISOString()}`
    );
  }

  resumeLsnFromToken(resumeToken: mongo.ResumeToken): string {
    // The timestamp is embedded in the resume token.
    return MongoLSN.fromResumeToken(resumeToken).comparable;
  }

  async createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T> {
    const session = this.context.client.startSession();
    try {
      await this.context.db.command({ hello: 1 }, { session });
      const head = session.clusterTime?.clusterTime;
      if (head == null) {
        throw new ServiceAssertionError(`clusterTime not available for write checkpoint`);
      }

      const r = await callback(new MongoLSN({ timestamp: head }).comparable);

      // Trigger a change on the changestream, so that the write checkpoint
      // is processed without waiting for other writes.
      await this.context.db.collection(CHECKPOINTS_COLLECTION).findOneAndUpdate(
        {
          _id: STANDALONE_CHECKPOINT_ID as any
        },
        {
          $inc: { i: 1 }
        },
        {
          upsert: true,
          returnDocument: 'after',
          session
        }
      );
      const time = session.operationTime!;
      if (time == null) {
        throw new ServiceAssertionError(`operationTime not available for write checkpoint`);
      } else if (time.lt(head)) {
        throw new ServiceAssertionError(`operationTime must be > clusterTime`);
      }

      return r;
    } finally {
      await session.endSession();
    }
  }

  observeCheckpointEvent(doc: ProjectedChangeStreamDocument): CheckpointEventKind {
    const checkpointId = getCheckpointId(doc);
    if (checkpointId == null) {
      return 'foreign';
    }
    if (checkpointId == STANDALONE_CHECKPOINT_ID) {
      return 'standalone';
    }
    return this.context.checkpointStreamId.equals(checkpointId) ? 'own-barrier' : 'foreign';
  }

  eventLsn(doc: ProjectedChangeStreamDocument): string {
    return new MongoLSN({
      timestamp: getEventTimestamp(doc),
      resume_token: doc._id
    }).comparable;
  }

  barrierResolved(marker: string, doc: ProjectedChangeStreamDocument): boolean {
    // Barrier markers are comparable LSNs in this implementation.
    return this.eventLsn(doc) >= marker;
  }

  isTolerableDescendingLsn(_lsn: string, _lastCheckpointLsn: string): boolean {
    return false;
  }

  describeEventPosition(doc: ProjectedChangeStreamDocument): string {
    return timestampToDate(getEventTimestamp(doc)).toISOString();
  }

  checkpointClearFilter(): mongo.Filter<mongo.Document> {
    // It's safe to clear the entire _powersync_checkpoints collection in this mode.
    return {};
  }
}

/**
 * Sentinel checkpoint implementation, used for sources without a usable clusterTime
 * (Cosmos DB). The ordered LSN coordinate is the shared standalone checkpoint
 * counter, observed through the change stream:
 *
 * - Batch checkpoints advance the global counter and embed its value in this
 *   stream's barrier document, so barrier events are self-describing and do
 *   not depend on cross-document event ordering.
 * - Standalone events carry the coordinate as their own `i` value.
 * - Keepalives advance the counter without persisting; the bump's own event
 *   flows through the stream and commits via the standalone handling,
 *   preserving the "commit only what the stream has seen" barrier property.
 *
 * See docs/cosmos-db-lsn-sentinel-checkpoints.md for the full design.
 */
export class SentinelCheckpointImplementation implements CheckpointImplementation {
  readonly zeroLsn = SentinelLSN.ZERO.comparable;
  readonly barrierMarkerIsLsn = false;

  /**
   * The highest global sentinel value proven so far, merged monotonically from
   * the resume seed, standalone events and embedded barrier values. Needed
   * because data events (setResumeLsn during catch-up) carry no coordinate of
   * their own.
   */
  private position = SentinelLSN.ZERO.sentinel;

  constructor(private context: CheckpointImplementationContext) {}

  parseResumePosition(lsn: string): StreamResumePosition {
    const parsed = SentinelLSN.fromSerialized(lsn);
    return { resumeAfter: parsed.resumeToken ?? null, startAfter: null };
  }

  seedPosition(lsn: string | null): void {
    this.position = lsn == null ? SentinelLSN.ZERO.sentinel : SentinelLSN.fromSerialized(lsn).sentinel;
  }

  logResume(lsn: string): void {
    const parsed = SentinelLSN.fromSerialized(lsn);
    this.context.logger.info(`Resume streaming at sentinel ${parsed.sentinel} / ${parsed}`);
  }

  async createStandaloneCheckpoint(): Promise<string> {
    return createCosmosCheckpointLsn(this.context.client, this.context.db);
  }

  async createBatchCheckpoint(): Promise<string> {
    // Two sentinel writes for one batch checkpoint:
    //
    // 1. Advance the shared standalone checkpoint — the global source-database
    //    coordinate. It must be shared so the LSN domain survives new
    //    ChangeStream instances and new sync rules.
    // 2. Advance this stream's private barrier document, embedding the global
    //    value from write 1. The barrier event is then self-describing: the
    //    committed LSN does not depend on the standalone event having been
    //    delivered first, since change stream ordering across different
    //    documents is not guaranteed.
    const globalLsn = SentinelLSN.fromSerialized(await createCosmosCheckpointLsn(this.context.client, this.context.db));
    return createCheckpoint(this.context.client, this.context.db, this.context.checkpointStreamId, {
      mode: 'sentinel',
      globalSentinel: globalLsn.sentinel
    });
  }

  async keepalive(_batch: storage.BucketStorageBatch, _resumeToken: mongo.ResumeToken): Promise<void> {
    // Advance the shared sentinel, but do not persist a checkpoint here. The
    // bump's own change event will flow through the stream and be committed by
    // the standalone checkpoint handling, which advances the ordered LSN
    // prefix and refreshes the resume token (using the event's own token).
    //
    // Why bump at all: with an unchanged sentinel, LSN comparison against the
    // previously persisted LSN falls to the opaque base64 token suffix, which
    // is not lexicographically meaningful — storage would silently reject
    // roughly half of plain token refreshes, leaving the persisted resume
    // token to go stale on an idle stream. (The timestamp implementation avoids this
    // because its keepalive timestamp is parsed from the resume token itself,
    // so the ordered prefix always advances with the token.)
    //
    // Why not persist immediately: writes that landed after this empty batch
    // was read — including a write checkpoint head — would be covered by the
    // persisted LSN before the stream has processed them, allowing write
    // checkpoints to resolve before the corresponding data is replicated.
    // Committing only when the bump's event is observed preserves the
    // "commit only what the stream has seen" barrier property.
    await createCosmosCheckpointLsn(this.context.client, this.context.db);
    this.context.logger.info(
      `Idle change stream (sentinel implementation). Bumped sentinel to advance the checkpoint.`
    );
  }

  resumeLsnFromToken(resumeToken: mongo.ResumeToken): string {
    // Pair the bare token with the current coordinate. The coordinate is
    // frozen between checkpoint events, so a per-batch resume marker only
    // advances the token; that is all resumption needs (see the design doc).
    return new SentinelLSN({ sentinel: this.position, resume_token: resumeToken }).comparable;
  }

  async createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T> {
    const head = await createCosmosCheckpointLsn(this.context.client, this.context.db);
    const result = await callback(head);
    // Create another bump to ensure movement after the reported head. This
    // covers the race where the head's own event is committed before the
    // write checkpoint document is stored.
    await createCosmosCheckpointLsn(this.context.client, this.context.db);
    return result;
  }

  observeCheckpointEvent(doc: ProjectedChangeStreamDocument): CheckpointEventKind {
    const checkpointId = getCheckpointId(doc);
    if (checkpointId == null) {
      return 'foreign';
    }
    if (checkpointId == STANDALONE_CHECKPOINT_ID) {
      // The standalone document's own `i` is the global coordinate.
      this.mergePosition(this.readStandaloneSentinel(doc));
      return 'standalone';
    }
    if (!this.context.checkpointStreamId.equals(checkpointId)) {
      return 'foreign';
    }
    // Our own barrier event embeds the global sentinel advanced just before
    // the barrier write.
    const embedded = this.readEmbeddedGlobalSentinel(doc);
    if (embedded != null) {
      this.mergePosition(embedded);
    }
    return 'own-barrier';
  }

  hasPosition(): boolean {
    return this.position != SentinelLSN.ZERO.sentinel;
  }

  eventLsn(doc: ProjectedChangeStreamDocument): string {
    // A zero position is permitted here: a resume LSN persisted before any
    // checkpoint event has been observed still carries a valid resume token,
    // and a low sentinel is conservative (it only causes more replay).
    return new SentinelLSN({
      sentinel: this.position,
      resume_token: doc._id
    }).comparable;
  }

  barrierResolved(marker: string, doc: ProjectedChangeStreamDocument): boolean {
    // Barriers are matched by content: same document and same increment.
    // The `i` value matters because the barrier document is reused — it
    // identifies a specific sentinel write.
    const [prefix, sentinelId, sentinelI] = marker.split(':');
    if (prefix !== 'sentinel') {
      throw new Error(`Invalid sentinel barrier marker: ${marker}`);
    }
    const fullDoc = deserializeFullDocument(doc);
    if (fullDoc == null) {
      this.context.logger.warn('Checkpoint event missing fullDocument — cannot match sentinel barrier');
      return false;
    }
    const docId = 'documentKey' in doc ? String(doc.documentKey._id) : null;
    return docId === sentinelId && String(fullDoc.i) === sentinelI;
  }

  isTolerableDescendingLsn(lsn: string, lastCheckpointLsn: string): boolean {
    // An LSN with the same sentinel as the last committed LSN differs only in
    // the opaque resume token suffix, which carries no ordering meaning —
    // e.g. when a restart replays an event behind an already-persisted
    // checkpoint. Not an ordering violation: the commit no-ops in storage
    // (checkpointBlocked).
    return SentinelLSN.fromSerialized(lsn).sentinel === SentinelLSN.fromSerialized(lastCheckpointLsn).sentinel;
  }

  describeEventPosition(_doc: ProjectedChangeStreamDocument): string {
    return `sentinel ${this.position}`;
  }

  checkpointClearFilter(): mongo.Filter<mongo.Document> {
    // The standalone checkpoint document must survive restarts. Its counter is
    // the globally-ordered component of every committed LSN, including write
    // checkpoint heads. Deleting it would reset the counter to 1 on the next
    // upsert, moving the LSN coordinate system backwards: new commits would
    // compare below the persisted last_checkpoint_lsn (failing the
    // out-of-order commit guard in a restart loop), and new write checkpoint
    // heads would resolve against old, higher committed LSNs before their
    // data has actually replicated.
    //
    // Note: this only protects against our own startup cleanup. The global
    // LSN coordinate still lives in a user-visible collection, so a consumer
    // can delete it in their source database. Dropping the whole checkpoints
    // collection is detected (the streaming loop invalidates the stream on the
    // collection drop event). Deleting just this document is mitigated by
    // createCosmosCheckpointLsn seeding re-created counters at the current
    // epoch milliseconds, so the coordinate jumps forward instead of resetting
    // below already-committed LSNs.
    return { _id: { $ne: STANDALONE_CHECKPOINT_ID } as any };
  }

  /** Never move the position backwards — replayed or reordered events must not regress the coordinate. */
  private mergePosition(observed: bigint) {
    if (observed > this.position) {
      this.position = observed;
    }
  }

  /**
   * Read the sentinel counter from a standalone checkpoint change event.
   *
   * `fullDocument` and its `i` field are required: the source materializes the
   * write-time post-image on insert/update/replace events, and every sentinel
   * write `$inc`s the `i` field. If either is missing, the event cannot be a
   * sentinel write we created — most likely the document was deleted or
   * replaced externally, which destroys the LSN coordinate system (a
   * re-created counter restarts below already-committed LSNs). Invalidate the
   * stream so replication restarts from scratch instead of stalling against a
   * broken coordinate.
   */
  private readStandaloneSentinel(doc: ProjectedChangeStreamDocument): bigint {
    const fullDoc = deserializeFullDocument(doc);
    if (fullDoc == null) {
      throw new ChangeStreamInvalidatedError(
        'Standalone checkpoint event has no fullDocument — cannot read the sentinel',
        new Error(`Unexpected ${doc.operationType} event on the standalone checkpoint document`)
      );
    }
    if (fullDoc.i == null) {
      throw new ChangeStreamInvalidatedError(
        'Standalone checkpoint document has no `i` field — cannot read the sentinel',
        new Error(`Standalone checkpoint document: ${JSON.stringify(fullDoc)}`)
      );
    }
    return normalizeSentinel(fullDoc.i);
  }

  /**
   * Read the embedded global sentinel from one of this stream's own barrier
   * events. Returns null when absent; the standalone-event path still provides
   * the coordinate in that case.
   */
  private readEmbeddedGlobalSentinel(doc: ProjectedChangeStreamDocument): bigint | null {
    const fullDoc = deserializeFullDocument(doc);
    return fullDoc?.globalSentinel == null ? null : normalizeSentinel(fullDoc.globalSentinel);
  }
}

/**
 * Select the checkpoint implementation for a source: sentinel-based for Cosmos
 * DB, clusterTime-based for standard MongoDB.
 */
export function createCheckpointImplementation(
  isCosmosDb: boolean,
  context: CheckpointImplementationContext
): CheckpointImplementation {
  return isCosmosDb ? new SentinelCheckpointImplementation(context) : new TimestampCheckpointImplementation(context);
}
