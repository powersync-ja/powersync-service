import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationHeadCallback, storage } from '@powersync/service-core';
import { SentinelLSN } from '../../common/SentinelLSN.js';
import { ChangeStreamInvalidatedError } from '../ChangeStream.js';
import { createCheckpoint, createCosmosCheckpointLsn, STANDALONE_CHECKPOINT_ID } from '../MongoRelation.js';
import { ProjectedChangeStreamDocument } from '../RawChangeStream.js';
import {
  CheckpointEventKind,
  CheckpointImplementation,
  CheckpointImplementationContext,
  getCheckpointId,
  StreamResumePosition
} from './CheckPointImplementation.js';

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
    return BigInt(fullDoc.i);
  }

  /**
   * Read the embedded global sentinel from one of this stream's own barrier
   * events. Returns null when absent; the standalone-event path still provides
   * the coordinate in that case.
   */
  private readEmbeddedGlobalSentinel(doc: ProjectedChangeStreamDocument): bigint | null {
    const fullDoc = deserializeFullDocument(doc);
    return fullDoc?.globalSentinel == null ? null : BigInt(fullDoc.globalSentinel);
  }
}

function deserializeFullDocument(doc: ProjectedChangeStreamDocument): mongo.Document | null {
  const fullDocument = 'fullDocument' in doc ? doc.fullDocument : null;
  if (!fullDocument) {
    return null;
  }
  // fullDocument is a raw BSON Buffer from parseChangeDocument.
  return mongo.BSON.deserialize(fullDocument as Buffer, { useBigInt64: true });
}
