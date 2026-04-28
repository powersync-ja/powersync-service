import { BucketDataSource } from './BucketSource.js';

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

export const DEFAULT_BUCKET_PRIORITY: BucketPriority = 3;

export const isValidPriority = (i: number): i is BucketPriority => {
  return Number.isInteger(i) && i >= 0 && i <= 3;
};

/**
 * There is no _direct_ way to define that a property is not enumerable in TypeScript.
 *
 * A getter on a class does that indirectly.
 *
 * This is _not_ the same as defining `get source(): BucketDataSource` directly on the interface.
 *
 * We never instantiate or extend this class directly - we only use the type.
 */
abstract class NonEnumerableSourceClass {
  private constructor() {}

  /**
   * This is specifically not enumerable - must be excluded from tests and serialization.
   */
  abstract get source(): BucketDataSource;
}

export type NonEnumerableBucketDataSource = NonEnumerableSourceClass;

export interface BucketDescription extends NonEnumerableBucketDataSource {
  /**
   * The id of the bucket, which is derived from the name of the bucket's definition
   * in the sync rules as well as the values returned by the parameter queries.
   */
  bucket: string;
  /**
   * The priority used to synchronize this bucket, derived from its definition or
   * a priority override on the Sync Stream subscription that created it.
   */
  priority: BucketPriority;
}

/**
 * A bucket that was resolved to a specific request including stream subscriptions.
 *
 * This includes information on why the bucket has been included in a checkpoint subset
 * shown to clients.
 */
export interface ResolvedBucket extends BucketDescription {
  /**
   * The name of the sync rule or stream definition from which the bucket is derived.
   */
  definition: string;
  inclusion_reasons: BucketInclusionReason[];
}

export type BucketInclusionReason = 'default' | { subscription: number };
