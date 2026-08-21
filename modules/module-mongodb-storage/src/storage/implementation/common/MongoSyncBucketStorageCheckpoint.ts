import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId, ReplicationCheckpoint } from '@powersync/service-core';
import * as bson from 'bson';

export interface MongoSyncBucketStorageCheckpoint extends ReplicationCheckpoint {
  checkpoint: InternalOpId;
  snapshotTime: bson.Timestamp;
  clusterTime: mongo.ClusterTime;

  /**
   * The stream's `parameter_compaction.checkpoint_changes_invalid_before` boundary, read in the
   * same snapshot as {@link checkpoint} and {@link snapshotTime}.
   *
   * Parameter compaction may have deleted parameter entries below this boundary, so checkpoint
   * change detection cannot enumerate individual lookup changes from a checkpoint below it.
   *
   * 0n for streams that have never been compacted.
   */
  parameterChangesInvalidBefore: InternalOpId;
}

/**
 * MongoDB-specific version of GetCheckpointChangesOptions.
 *
 * The next checkpoint carries the snapshot and invalidation boundary that change detection must
 * be evaluated against. Only `checkpoint` is used from the previous one.
 */
export interface MongoGetCheckpointChangesOptions {
  lastCheckpoint: ReplicationCheckpoint;
  nextCheckpoint: MongoSyncBucketStorageCheckpoint;
}
