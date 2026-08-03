import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationHeadCallback, storage } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { SentinelLSN } from '../../common/SentinelLSN.js';
import { ChangeStreamInvalidatedError } from '../ChangeStream.js';
import { createSentinelCheckpointLsn, SENTINEL_CHECKPOINT_ID } from '../MongoRelation.js';
import { ProjectedChangeStreamDocument } from '../RawChangeStream.js';
import {
  CheckpointEventApi,
  CheckpointEventKind,
  CheckpointImplementation,
  CheckpointImplementationContext,
  getCheckpointId,
  StreamResumePosition
} from './CheckpointImplementation.js';

/**
 * Sentinel checkpoint implementation, used for sources without a usable clusterTime
 * (DocumentDB). The ordered LSN coordinate is a single shared sentinel checkpoint
 * counter ({@link SENTINEL_CHECKPOINT_ID}), observed through the change stream:
 *
 * - Batch checkpoints advance the global counter and stamp it with this stream's
 *   id, so the stream recognises its own private barriers by content (stream_id
 *   + counter) rather than by cross-document event ordering.
 * - Standalone bumps (write checkpoint heads, snapshot markers) advance the same
 *   counter with a null stream_id; every stream observes these as the global
 *   coordinate.
 * - Keepalives advance the counter without persisting; the bump's own event
 *   flows through the stream and commits via the standalone handling,
 *   preserving the "commit only what the stream has seen" barrier property.
 *
 * See docs/documentdb/documentdb-lsn-sentinel-checkpoints.md for the full design.
 */
export class SentinelCheckpointImplementation implements CheckpointImplementation {
  readonly zeroLsn = SentinelLSN.ZERO.comparable;

  /**
   * The highest global sentinel value proven so far, merged monotonically from
   * the resume seed, standalone events and embedded barrier values. Needed
   * for lsnFromResumeToken: per-batch resume markers carry only a resume token,
   * so they need the latest observed coordinate paired in.
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
    return createSentinelCheckpointLsn(this.context.client, this.context.db);
  }

  async createBatchCheckpoint(): Promise<string> {
    // Advance the shared sentinel checkpoint — the global source-database
    // coordinate. It must be shared so the LSN domain survives new
    // ChangeStream instances and new sync rules.
    // This advance is associated with the change stream in order to track stream
    // barriers. The returned LSN (counter only, no resume token) is the barrier
    // marker matched by resolvesBarrier.
    return createSentinelCheckpointLsn(this.context.client, this.context.db, this.context.checkpointStreamId);
  }

  async createFirstBarrier(): Promise<string | null> {
    // The barrier marker is an opaque content-matched marker, not a resume
    // position, so there is no LSN to open the stream from: a fresh DocumentDB
    // stream opens from "now" and the snapshot loop re-creates barriers until
    // one is observed.
    await this.createBatchCheckpoint();
    return null;
  }

  async keepalive(_batch: storage.BucketStorageBatch, _resumeToken: mongo.ResumeToken): Promise<void> {
    // Advance the shared sentinel, but do not persist a checkpoint here. The
    // bump is stamped with this stream's id, so its own change event flows
    // through the stream as an own-barrier event and is committed by the
    // own-barrier handling, which advances the ordered LSN prefix and refreshes
    // the resume token (using the event's own token).
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
    await createSentinelCheckpointLsn(this.context.client, this.context.db, this.context.checkpointStreamId);
    this.context.logger.info(
      `Idle change stream (sentinel implementation). Bumped sentinel to advance the checkpoint.`
    );
  }

  lsnFromResumeToken(resumeToken: mongo.ResumeToken): string {
    // Pair the bare token with the current coordinate. The coordinate is
    // frozen between checkpoint events, so a per-batch resume marker only
    // advances the token; that is all resumption needs (see the design doc).
    return new SentinelLSN({ sentinel: this.position, resume_token: resumeToken }).comparable;
  }

  async createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T> {
    const head = await createSentinelCheckpointLsn(this.context.client, this.context.db);
    const { response, shouldAdvance } = await callback(head);
    if (shouldAdvance) {
      // Create another bump to ensure movement after the reported head. This
      // covers the race where the head's own event is committed before the
      // write checkpoint document is stored.
      // Note that this checkpoint should not be associated with a change stream Id.
      await createSentinelCheckpointLsn(this.context.client, this.context.db);
    }
    return response;
  }

  readonly event: CheckpointEventApi = {
    observe: (doc) => {
      const checkpointId = getCheckpointId(doc);
      if (checkpointId != SENTINEL_CHECKPOINT_ID) {
        // The sentinel implementation only uses the SENTINEL_CHECKPOINT_ID
        // document; anything else (including the timestamp impl's standalone
        // document) is foreign.
        return 'foreign';
      }
      const fullDoc = deserializeFullDocument(doc);
      if (fullDoc == null) {
        // An insert/update/replace on our sentinel document must carry a
        // post-image. A missing fullDocument means the document was deleted or
        // replaced out from under us, which destroys the coordinate; invalidate
        // so replication restarts clean instead of stalling.
        throw new ChangeStreamInvalidatedError(
          'Sentinel checkpoint event has no fullDocument — cannot read the sentinel',
          new Error(`Unexpected ${doc.operationType} event on the sentinel checkpoint document`)
        );
      }
      const streamId = fullDoc.stream_id;

      let kind: CheckpointEventKind;
      if (streamId == null) {
        // No stream_id: a standalone bump (write checkpoint head, snapshot
        // marker). Every stream tracks these as the global coordinate.
        kind = 'standalone';
      } else if (this.context.checkpointStreamId.equals(streamId)) {
        // Stamped with our id: one of our own private batch/keepalive barriers.
        kind = 'own-barrier';
      } else {
        // Another stream's private barrier — ignore.
        return 'foreign';
      }

      // Both kinds carry the global coordinate in `i`; keep our position current.
      this.mergePosition(this.readSentinel(fullDoc));
      return kind;
    },

    lsn: (doc) => {
      const fullDoc = deserializeFullDocument(doc);
      if (fullDoc == null) {
        throw new ChangeStreamInvalidatedError(
          'Sentinel checkpoint event has no fullDocument — cannot read the sentinel',
          new Error(`Unexpected ${doc.operationType} event on the sentinel checkpoint document`)
        );
      }

      // Checkpoint events carry the global coordinate in `i`; pair that exact
      // coordinate with this event's resume token. Plain data-batch resume
      // markers use lsnFromResumeToken, which pairs the token with the tracked
      // position instead.
      return new SentinelLSN({
        sentinel: this.readSentinel(fullDoc),
        resume_token: doc._id
      }).comparable;
    },

    resolvesBarrier: (marker, doc) => {
      const fullDoc = deserializeFullDocument(doc);
      if (fullDoc == null) {
        this.context.logger.warn('Checkpoint event missing fullDocument — cannot match sentinel barrier');
        return false;
      }
      const parsed = SentinelLSN.fromSerialized(marker);
      return this.context.checkpointStreamId.equals(fullDoc.stream_id) && fullDoc.i >= parsed.sentinel;
    }
  };

  // The sentinel checkpoint document must survive restarts (hence the $ne
  // below — the startup cleanup deletes every other checkpoint document). Its
  // counter is the globally-ordered component of every committed LSN, including
  // write checkpoint heads. Deleting it would re-seed the counter on the next
  // upsert, risking moving the LSN coordinate system backwards: new commits
  // could compare below the persisted last_checkpoint_lsn (so storage rejects
  // them via checkpointBlocked and the checkpoint stalls), and new write
  // checkpoint heads could resolve against old, higher committed LSNs before
  // their data has actually replicated.
  //
  // Note: this only protects against our own startup cleanup. The global
  // LSN coordinate still lives in a user-visible collection, so a consumer
  // can delete it in their source database. Dropping the whole checkpoints
  // collection is detected (the streaming loop invalidates the stream on the
  // collection drop event). Deleting just this document is mitigated by
  // createSentinelCheckpointLsn seeding re-created counters at the current
  // epoch seconds, so the coordinate jumps forward instead of resetting
  // below already-committed LSNs.
  readonly checkpointClearFilter: mongo.Filter<mongo.Document> = { _id: { $ne: SENTINEL_CHECKPOINT_ID } as any };

  /** Never move the position backwards — replayed or reordered events must not regress the coordinate. */
  private mergePosition(observed: bigint) {
    if (observed > this.position) {
      this.position = observed;
    }
  }

  /**
   * Read the sentinel counter from a sentinel checkpoint post-image.
   *
   * The `i` field is required: every sentinel write `$inc`s it. If it is
   * missing, the document was replaced externally, which destroys the LSN
   * coordinate system (a re-created counter restarts below already-committed
   * LSNs). Invalidate the stream so replication restarts from scratch instead
   * of stalling against a broken coordinate.
   */
  private readSentinel(fullDoc: mongo.Document): bigint {
    if (fullDoc.i == null) {
      throw new ChangeStreamInvalidatedError(
        'Sentinel checkpoint document has no `i` field — cannot read the sentinel',
        // JSONBig.stringify, since the post-image is deserialized with useBigInt64.
        new Error(`Sentinel checkpoint document: ${JSONBig.stringify(fullDoc)}`)
      );
    }
    return fullDoc.i;
  }
}

/**
 * A change event with the decoded `fullDocument` post-image memoized on it, so
 * a consumer that reads it more than once — e.g. observe() then resolvesBarrier()
 * for the same event — decodes the buffer only once. `undefined` until first
 * decoded; `null` when there is no fullDocument.
 */
type MemoizedChangeStreamDocument = ProjectedChangeStreamDocument & {
  parsedFullDocument?: mongo.Document | null;
};

function deserializeFullDocument(doc: ProjectedChangeStreamDocument): mongo.Document | null {
  const memo = doc as MemoizedChangeStreamDocument;
  if (memo.parsedFullDocument !== undefined) {
    return memo.parsedFullDocument;
  }
  const fullDocument = 'fullDocument' in doc ? doc.fullDocument : null;
  // fullDocument is a raw BSON Buffer from parseChangeDocument.
  const parsed = fullDocument ? mongo.BSON.deserialize(fullDocument as Buffer, { useBigInt64: true }) : null;
  memo.parsedFullDocument = parsed;
  return parsed;
}
