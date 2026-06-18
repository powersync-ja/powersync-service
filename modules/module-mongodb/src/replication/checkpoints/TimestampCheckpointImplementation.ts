import { mongo } from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { ReplicationHeadCallback, storage } from '@powersync/service-core';
import { MongoLSN } from '../../common/MongoLSN.js';
import { createCheckpoint, STANDALONE_CHECKPOINT_ID } from '../MongoRelation.js';
import { ProjectedChangeStreamDocument } from '../RawChangeStream.js';
import { CHECKPOINTS_COLLECTION, timestampToDate } from '../replication-utils.js';
import {
  CheckpointEventApi,
  CheckpointImplementation,
  CheckpointImplementationContext,
  classifyCheckpointEvent,
  descendingLsnError,
  getEventTimestamp,
  StreamResumePosition
} from './CheckpointImplementation.js';

/**
 * Standard MongoDB checkpoint implementation. The ordered LSN coordinate is the oplog
 * clusterTime — unique per operation, monotonic, and parseable from resume
 * tokens. Barriers and event LSNs are plain comparable LSN strings.
 */
export class TimestampCheckpointImplementation implements CheckpointImplementation {
  readonly zeroLsn = MongoLSN.ZERO.comparable;

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

  async createFirstBarrier(): Promise<string | null> {
    // The barrier marker is a comparable LSN; resume the snapshot stream from it.
    return this.createBatchCheckpoint();
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

  lsnFromResumeToken(resumeToken: mongo.ResumeToken): string {
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

  readonly event: CheckpointEventApi = {
    observe: (doc) => classifyCheckpointEvent(doc, this.context.checkpointStreamId),

    lsn: (doc) => {
      return new MongoLSN({
        timestamp: getEventTimestamp(doc),
        resume_token: doc._id
      }).comparable;
    },

    resolvesBarrier: (marker, doc) => {
      // Barrier markers are comparable LSNs in this implementation.
      return this.event.lsn(doc) >= marker;
    },

    describe: (doc) => {
      return timestampToDate(getEventTimestamp(doc)).toISOString();
    }
  };

  checkDescendingLsn(lsn: string, lastCheckpointLsn: string | null, doc: ProjectedChangeStreamDocument): void {
    // clusterTime is globally ordered, so any descent below the last committed
    // LSN is a real ordering violation.
    if (lastCheckpointLsn != null && lsn < lastCheckpointLsn) {
      throw descendingLsnError(this, lastCheckpointLsn, doc);
    }
  }

  // It's safe to clear the entire _powersync_checkpoints collection in this mode.
  readonly checkpointClearFilter: mongo.Filter<mongo.Document> = {};
}
