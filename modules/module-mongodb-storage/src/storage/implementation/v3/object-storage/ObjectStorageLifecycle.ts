import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import * as bson from 'bson';
import { createHash, randomUUID } from 'node:crypto';
import { REPLICATION_WRITE_OPTIONS } from '../../MongoReplicationOptions.js';
import { MongoWriteBatch } from '../../MongoWriteBatch.js';
import { ObjectStorageDeletionMarker } from '../models.js';
import { VersionedPowerSyncMongoV3 } from '../VersionedPowerSyncMongoV3.js';
import { BucketDataObjectStorage } from './BucketDataObjectStorage.js';
import { ObjectStorage, ObjectStorageOperationOptions } from './ObjectStorage.js';

export const OBJECT_STORAGE_UPLOAD_LEASE_MS = 60 * 60 * 1000;
/**
 * Leave time for the bounded publication transaction and clock skew before
 * cleanup can select a marker. Publication must finish within this margin after
 * its eligibility check; checked marker deletion alone cannot protect an object
 * from a cleanup worker that already selected an expired marker.
 */
export const OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS = 2 * 60 * 1000;
export const OBJECT_STORAGE_REFERENCE_GRACE_MS = 15 * 60 * 1000;
/**
 * Protect against arbitrary-long bucket names and parameters exceeding the S3 key length limits.
 *
 * Any bucket name over this length is hashed.
 */
const MAX_READABLE_BUCKET_SEGMENT_BYTES = 128;

export interface PreparedObjectStorageUpload {
  markerId: bson.ObjectId;
  path: string;
  deleteAfter: Date;
}

export interface ObjectStorageUploadPayload {
  upload: PreparedObjectStorageUpload;
  payload: Uint8Array;
}

/**
 * Coordinates the MongoDB outbox used to make object-storage changes recoverable.
 *
 * A MongoDB collection is used to track files that will be uploaded, or should be deleted.
 * By using a MongoDB collection, we can use the same transaction that creates or removes
 * references to these files, making the operations atomic.
 *
 * These markers indicate which files should be deleted - either by being orphaned during create,
 * or after references to them are removed.
 */
export class ObjectStorageLifecycle {
  readonly bucketData: BucketDataObjectStorage;

  constructor(
    private readonly db: VersionedPowerSyncMongoV3,
    private readonly replicationStreamId: number,
    private readonly objectStorage: ObjectStorage
  ) {
    this.bucketData = new BucketDataObjectStorage(objectStorage);
  }

  streamPrefix(): string {
    return `bucket-data/${this.replicationStreamId}/`;
  }

  definitionPrefix(definitionId: string): string {
    return `${this.streamPrefix()}${definitionId}/`;
  }

  allocatePath(definitionId: string, bucket: string, minOp: bigint, maxOp: bigint): string {
    // Preserve readable paths for normal buckets, but bound user-controlled
    // parameter values so they cannot consume the S3 key-length budget.
    const escapedBucket = encodeURIComponent(bucket);
    const bucketSegment =
      Buffer.byteLength(escapedBucket, 'utf8') <= MAX_READABLE_BUCKET_SEGMENT_BYTES
        ? escapedBucket
        : `sha256-${createHash('sha256').update(bucket).digest('base64url')}`;
    return `${this.definitionPrefix(definitionId)}${bucketSegment}/${minOp}-${maxOp}-${randomUUID()}.bson`;
  }

  /**
   * Indicate that files will be uploaded to S3.
   *
   * These markers are expected to be removed using publishUploads(). If that does not happen within
   * the grace period, the files will be deleted. Registration must be majority
   * acknowledged before any PUT starts: losing a marker on failover would lose
   * the only cleanup obligation for an unpublished object. A failed/ambiguous
   * insert must therefore prevent uploads, even if some markers were inserted.
   */
  async prepareUploads(
    paths: string[],
    now = new Date(),
    session?: mongo.ClientSession
  ): Promise<PreparedObjectStorageUpload[]> {
    const deleteAfter = new Date(now.getTime() + OBJECT_STORAGE_UPLOAD_LEASE_MS);
    const uploads = paths.map((path) => ({ markerId: new bson.ObjectId(), path, deleteAfter }));
    if (uploads.length > 0) {
      await this.db.pendingObjectStorageDeletes(this.replicationStreamId).insertMany(
        uploads.map((upload) => ({
          _id: upload.markerId,
          path: upload.path,
          delete_after: upload.deleteAfter
        })),
        { ...REPLICATION_WRITE_OPTIONS, session }
      );
    }
    return uploads;
  }

  canPublish(upload: PreparedObjectStorageUpload, now = new Date()): boolean {
    return now.getTime() < upload.deleteAfter.getTime() - OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS;
  }

  /**
   * Upload a durably registered generation, joining every started PUT on failure.
   * The shared adapter limits concurrency; its queue must observe the same signal
   * as an active request so no queued PUT can start after the protection deadline.
   *
   * Failed/ambiguous PUTs retain their original markers. S3's bounded request
   * timeouts are far shorter than the one-hour orphan grace, including when a
   * response is lost or the process dies. Cancellation does not prove an object
   * is absent, so never delete an object or its marker on upload failure.
   */
  async storeUploads(uploads: ObjectStorageUploadPayload[], options?: ObjectStorageOperationOptions): Promise<void> {
    if (uploads.length === 0) {
      return;
    }
    const deadline =
      Math.min(...uploads.map(({ upload }) => upload.deleteAfter.getTime())) -
      OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS;
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      throw new Error('Object storage upload lease expired before PUT');
    }
    const expiration = new AbortController();
    const timer = setTimeout(
      () => expiration.abort(new Error('Object storage upload lease expired during PUT')),
      remaining
    );
    timer.unref();
    const signal = options?.signal ? AbortSignal.any([options.signal, expiration.signal]) : expiration.signal;
    let results: PromiseSettledResult<unknown>[];
    try {
      results = await Promise.allSettled(
        uploads.map(async ({ upload, payload }) => {
          signal.throwIfAborted();
          // The timer handles time in the adapter's queue; this check also catches
          // wall-clock changes before dispatch. Publication checks again later.
          if (!this.canPublish(upload)) {
            throw new Error(`Object storage upload lease expired for ${upload.path}`);
          }
          await this.bucketData.storeSerialized(upload.path, payload, { signal });
        })
      );
    } finally {
      clearTimeout(timer);
    }
    const failure = results.find((result) => result.status === 'rejected');
    if (failure?.status === 'rejected') {
      throw failure.reason;
    }
    signal.throwIfAborted();
  }

  /**
   * Remove markers for files that have finished uploading, and are now referenced from other documents.
   *
   * Call this in the same transaction as the one creating the references.
   */
  publishUploads(uploads: PreparedObjectStorageUpload[], writes: MongoWriteBatch): void {
    for (const upload of uploads) {
      if (!this.canPublish(upload)) {
        throw new Error(`Object storage publication lease expired for ${upload.path}`);
      }
    }
    if (uploads.length === 0) {
      return;
    }

    writes.deleteMany(
      this.db.pendingObjectStorageDeletes(this.replicationStreamId),
      { _id: { $in: uploads.map((upload) => upload.markerId) } },
      (deletedCount) => {
        if (deletedCount !== uploads.length) {
          throw new Error('Missing object storage publication markers');
        }
      }
    );
  }

  /**
   * Mark old paths for deletion.
   *
   * By marking for deletion instead of immediately deleting:
   * 1. This avoids inconsistencies/orphaned files between MongoDB and S3, by allowing the markers to
   *    be persisted transactionally, while the deletes happen asynchronously and can be retried.
   * 2. The deletes are delayed, giving clients a grace period to continue using the files for
   *    in-progress requests.
   *
   * Call this in the same transaction as the one removing the references to these files.
   */
  retire(paths: Iterable<string>, writes: MongoWriteBatch, now = new Date()): void {
    const markers: ObjectStorageDeletionMarker[] = Array.from(paths, (path) => ({
      _id: new bson.ObjectId(),
      path,
      delete_after: new Date(now.getTime() + OBJECT_STORAGE_REFERENCE_GRACE_MS)
    }));
    if (markers.length) {
      writes.insertMany(this.db.pendingObjectStorageDeletes(this.replicationStreamId), markers);
    }
  }

  /**
   * Discover and delete objects by their storage prefix
   * without touching the database.
   */
  async deletePrefix(prefix: string, options?: ObjectStorageOperationOptions): Promise<{ objectCount: number }> {
    return this.objectStorage.deletePrefix(prefix, { signal: options?.signal });
  }

  async cleanup(logger: Logger, options?: ObjectStorageOperationOptions): Promise<void> {
    const signal = options?.signal;
    const markers = this.db.pendingObjectStorageDeletes(this.replicationStreamId);
    const cursor = markers.find({ delete_after: { $lte: new Date() } });
    let batch: ObjectStorageDeletionMarker[] = [];

    const deleteBatch = async () => {
      if (batch.length === 0) {
        return;
      }
      const deleting = batch;
      batch = [];
      try {
        await this.bucketData.delete(
          deleting.map((marker) => marker.path),
          { signal }
        );
        await markers.deleteMany({ _id: { $in: deleting.map((marker) => marker._id) } });
      } catch (error) {
        if (signal?.aborted) {
          // Shutting down: stop instead of working through the remaining markers. They stay
          // pending, and are cleaned up during the next compaction. Checked on the signal rather
          // than the error, since a caller may abort with any reason.
          throw error;
        }
        logger.warn('Failed to clean up object storage deletion markers; will retry during the next compaction', error);
      }
    };

    for await (const marker of cursor) {
      signal?.throwIfAborted();
      batch.push(marker);
      if (batch.length === 500) {
        await deleteBatch();
      }
    }
    await deleteBatch();
  }
}
