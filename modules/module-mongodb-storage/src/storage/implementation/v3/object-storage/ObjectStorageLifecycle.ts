import { mongo } from '@powersync/lib-service-mongodb';
import * as bson from 'bson';
import { randomUUID } from 'node:crypto';
import { ObjectStorageDeletionMarker } from '../models.js';
import { VersionedPowerSyncMongoV3 } from '../VersionedPowerSyncMongoV3.js';
import { BucketDataObjectStorage } from './BucketDataObjectStorage.js';
import { ObjectStorage } from './ObjectStorage.js';

export const OBJECT_STORAGE_UPLOAD_LEASE_MS = 60 * 60 * 1000;
export const OBJECT_STORAGE_PUBLICATION_SAFETY_MARGIN_MS = 2 * 60 * 1000;
export const OBJECT_STORAGE_REFERENCE_GRACE_MS = 15 * 60 * 1000;

export interface PreparedObjectStorageUpload {
  markerId: bson.ObjectId;
  path: string;
  deleteAfter: Date;
}

/** Coordinates the MongoDB outbox used to make object-storage changes recoverable. */
export class ObjectStorageLifecycle {
  readonly bucketData: BucketDataObjectStorage;

  constructor(
    private readonly db: VersionedPowerSyncMongoV3,
    private readonly replicationStreamId: number,
    objectStorage: ObjectStorage
  ) {
    this.bucketData = new BucketDataObjectStorage(objectStorage);
  }

  allocatePath(definitionId: string, bucket: string, minOp: bigint, maxOp: bigint): string {
    return `bucket-data/${this.replicationStreamId}/${definitionId}/${bucket}/${minOp}-${maxOp}-${randomUUID()}.bson.zstd`;
  }

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

  async cleanup(logger: { warn(message: string, error?: unknown): void }): Promise<void> {
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
