/**
 * The priority in which to synchronize buckets.
 *
 * Lower numbers represent higher priorities.
 * Generally, the sync service _may_ synchronize buckets with higher priorities first.
 * Priorities also refine the consistency notion by the sync service in the way that clients
 * may choose to publish data when all buckets of a certain priority have been synchronized.
 * So, when clients are synchronizing buckets with different priorities, they will only get
 * consistent views within each priority.
 *
 * Additionally, data from buckets with priority 0 may be made visible when clients still
 * have data in their upload queue.
 */
export type BucketPriority = 0 | 1 | 2 | 3;

export const defaultBucketPriority: BucketPriority = 1;

export interface BucketDescription {
  /**
   * The id of the bucket, which is derived from the name of the bucket's definition
   * in the sync rules as well as the values returned by the parameter queries.
   */
  bucket: string;
  /**
   * The priority used to synchronize this bucket, derived from its definition.
   */
  priority: BucketPriority;
}
