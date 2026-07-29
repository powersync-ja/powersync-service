import { mongo } from '@powersync/lib-service-mongodb';
import { Logger } from '@powersync/lib-services-framework';
import * as bson from 'bson';
import { createHash, randomUUID } from 'node:crypto';
import { ObjectStorageDeletionMarker } from '../models.js';
import { VersionedPowerSyncMongoV3 } from '../VersionedPowerSyncMongoV3.js';
import { BucketDataObjectStorage } from './BucketDataObjectStorage.js';
import { ObjectStorage } from './ObjectStorage.js';

export const OBJECT_STORAGE_UPLOAD_LEASE_MS = 60 * 60 * 1000;
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
   * the grace period, the files will be deleted.
   */
  async prepareUploads(paths: string[], now = new Date()): Promise<PreparedObjectStorageUpload[]> {
    const deleteAfter = new Date(now.getTime() + OBJECT_STORAGE_UPLOAD_LEASE_MS);
    const uploads = paths.map((path) => ({ markerId: new bson.ObjectId(), path, deleteAfter }));
    if (uploads.length) {
      await this.db.pendingObjectStorageDeletes(this.replicationStreamId).insertMany(
        uploads.map((upload) => ({
          _id: upload.markerId,
          path: upload.path,
          delete_after: upload.deleteAfter
        }))
      );
    }
    return uploads;
  }

  canPublish(upload: PreparedObjectStorageUpload, now = new Date()): boolean {
    return now.getTime() < upload.deleteAfter.getTime() - OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS;
  }

  /**
   * Remove markers for files that have finished uploading, and are now referenced from other documents.
   *
   * Call this in the same transaction as the one creating the references.
   */
  async publishUploads(uploads: PreparedObjectStorageUpload[], session: mongo.ClientSession): Promise<void> {
    for (const upload of uploads) {
      if (!this.canPublish(upload)) {
        throw new Error(`Object storage publication lease expired for ${upload.path}`);
      }
    }
    if (uploads.length === 0) {
      return;
    }

    const result = await this.db
      .pendingObjectStorageDeletes(this.replicationStreamId)
      .deleteMany({ _id: { $in: uploads.map((upload) => upload.markerId) } }, { session });
    if (result.deletedCount !== uploads.length) {
      throw new Error(`Missing object storage publication markers`);
    }
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
  async retire(paths: Iterable<string>, session: mongo.ClientSession, now = new Date()): Promise<void> {
    const markers: ObjectStorageDeletionMarker[] = Array.from(paths, (path) => ({
      _id: new bson.ObjectId(),
      path,
      delete_after: new Date(now.getTime() + OBJECT_STORAGE_REFERENCE_GRACE_MS)
    }));
    if (markers.length) {
      await this.db.pendingObjectStorageDeletes(this.replicationStreamId).insertMany(markers, { session });
    }
  }

  /**
   * Discover and delete objects by their storage prefix
   * without touching the database.
   */
  async deletePrefix(prefix: string, options?: { signal?: AbortSignal }): Promise<{ objectCount: number }> {
    return this.objectStorage.deletePrefix(prefix, { signal: options?.signal });
  }

  async cleanup(logger: Logger): Promise<void> {
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
        await this.bucketData.delete(deleting.map((marker) => marker.path));
        await markers.deleteMany({ _id: { $in: deleting.map((marker) => marker._id) } });
      } catch (error) {
        logger.warn('Failed to clean up object storage deletion markers; will retry during the next compaction', error);
      }
    };

    for await (const marker of cursor) {
      batch.push(marker);
      if (batch.length === 500) {
        await deleteBatch();
      }
    }
    await deleteBatch();
  }
}
