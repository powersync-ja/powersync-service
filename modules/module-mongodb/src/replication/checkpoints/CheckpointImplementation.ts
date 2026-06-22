import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import { ReplicationHeadCallback, storage } from '@powersync/service-core';

import { ProjectedChangeStreamDocument } from '../RawChangeStream.js';

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
 * Event-interpretation surface of a {@link CheckpointImplementation}. Every method
 * operates on a single raw change event.
 *
 * Ordering contract: {@link observe} must be called before {@link lsn} for a given
 * event, so the sentinel implementation's coordinate is current.
 */
export interface CheckpointEventApi {
  /**
   * Classify a checkpoint-collection event and absorb any coordinate it
   * carries (sentinel implementation). Must be called for every checkpoint event, in
   * stream order.
   */
  observe(doc: ProjectedChangeStreamDocument): CheckpointEventKind;

  /**
   * Comparable LSN for committing/resuming at this event.
   *
   * Note this is not purely a function of `doc`. The timestamp implementation
   * derives the whole LSN from the event (its coordinate is the event's
   * clusterTime). The sentinel implementation pairs the *tracked* coordinate
   * (updated by {@link observe}) with the event's resume token, because data
   * events carry no coordinate of their own — hence the observe-before-lsn
   * contract.
   */
  lsn(doc: ProjectedChangeStreamDocument): string;

  /** Whether a barrier marker from {@link CheckpointImplementation.createBatchCheckpoint} is resolved by this event. */
  resolvesBarrier(marker: string, doc: ProjectedChangeStreamDocument): boolean;
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
 *   (DocumentDB, and technically usable on any MongoDB). The ordered coordinate
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
   * by a later change stream event via {@link CheckpointEventApi.resolvesBarrier}.
   */
  createBatchCheckpoint(): Promise<string>;

  /**
   * Create the first batch barrier for snapshot-LSN acquisition and return the
   * LSN to open the change stream from, or null to open from "now".
   *
   * The timestamp implementation's barrier marker is itself a comparable LSN, so
   * the stream resumes from it. The sentinel implementation's marker is opaque
   * (content-matched) and carries no resume position, so it returns null and the
   * stream opens from the current point.
   */
  createFirstBarrier(): Promise<string | null>;

  /**
   * Idle keepalive for an empty change stream batch. May persist a checkpoint
   * directly (timestamp implementation) or nudge the source so that a later event
   * commits (sentinel implementation).
   */
  keepalive(batch: storage.BucketStorageBatch, resumeToken: mongo.ResumeToken): Promise<void>;

  /**
   * Build a comparable LSN from a bare batch-level resume token (no change
   * event). Used for the per-batch `setResumeLsn` progress marker.
   *
   * The timestamp implementation parses the timestamp embedded in the token.
   * The sentinel implementation pairs the token with the current coordinate.
   */
  lsnFromResumeToken(resumeToken: mongo.ResumeToken): string;

  /**
   * Source-side replication head for write checkpoints. The LSN passed to the
   * callback must compare at or below any LSN committed after the caller's
   * preceding writes.
   */
  createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T>;

  /** Event-interpretation methods, all operating on a single raw change event. */
  readonly event: CheckpointEventApi;

  /** Filter for clearing the checkpoints collection on startup. */
  readonly checkpointClearFilter: mongo.Filter<mongo.Document>;
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

export function getCheckpointId(doc: ProjectedChangeStreamDocument): string | mongo.ObjectId | null {
  if (!('documentKey' in doc)) {
    return null;
  }
  return doc.documentKey._id as string | mongo.ObjectId;
}
