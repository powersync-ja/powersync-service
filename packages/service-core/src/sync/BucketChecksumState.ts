import {
  BucketDescription,
  BucketPriority,
  BucketSource,
  HydratedSyncRules,
  RequestedStream,
  RequestJwtPayload,
  RequestParameters,
  ResolvedBucket
} from '@powersync/service-sync-rules';

import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import {
  logger as defaultLogger,
  ErrorCode,
  Logger,
  ServiceAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import { JSONBig } from '@powersync/service-jsonbig';
import { BucketParameterQuerier, QuerierError } from '@powersync/service-sync-rules/src/BucketParameterQuerier.js';
import { SyncContext } from './SyncContext.js';
import { getIntersection, hasIntersection } from './util.js';

export interface BucketChecksumStateOptions {
  syncContext: SyncContext;
  bucketStorage: BucketChecksumStateStorage;
  syncRules: HydratedSyncRules;
  tokenPayload: RequestJwtPayload;
  syncRequest: util.StreamingSyncRequest;
  logger?: Logger;
}

type BucketSyncState = {
  start_op_id: util.InternalOpId;
};

/**
 * Represents the state of the checksums and data for a specific connection.
 *
 * Handles incrementally re-computing checkpoints.
 */
export class BucketChecksumState {
  private readonly context: SyncContext;
  private readonly bucketStorage: BucketChecksumStateStorage;

  /**
   * Bucket state of bucket id -> op_id.
   * This starts with the state from the client. May contain buckets that the user do not have access to (anymore).
   *
   * This is always updated in-place.
   */
  public readonly bucketDataPositions = new Map<string, BucketSyncState>();

  /**
   * Last checksums sent to the client. We keep this to calculate checkpoint diffs.
   */
  private lastChecksums: util.ChecksumMap | null = null;
  private lastWriteCheckpoint: bigint | null = null;
  /**
   * Once we've sent the first full checkpoint line including all {@link util.Checkpoint.streams} that the user is
   * subscribed to, we keep an index of the stream names to their index in that array.
   *
   * This is used to compress the representation of buckets in `checkpoint` and `checkpoint_diff` lines: For buckets
   * that are part of sync rules or default streams, we need to include the name of the defining sync rule or definition
   * yielding that bucket (so that clients can track progress for default streams).
   * But instead of sending the name for each bucket, we use the fact that it's part of the streams array and only send
   * their index, reducing the size of those messages.
   */
  private streamNameToIndex: Map<string, number> | null = null;

  private readonly parameterState: BucketParameterState;

  /**
   * Keep track of buckets that need to be downloaded. This is specifically relevant when
   * partial checkpoints are sent.
   */
  private pendingBucketDownloads = new Set<string>();

  private readonly logger: Logger;

  constructor(options: BucketChecksumStateOptions) {
    this.context = options.syncContext;
    this.bucketStorage = options.bucketStorage;
    this.logger = options.logger ?? defaultLogger;
    this.parameterState = new BucketParameterState(
      options.syncContext,
      options.bucketStorage,
      options.syncRules,
      options.tokenPayload,
      options.syncRequest,
      this.logger
    );
    this.bucketDataPositions = new Map();

    for (let { name, after: start } of options.syncRequest.buckets ?? []) {
      this.bucketDataPositions.set(name, { start_op_id: BigInt(start) });
    }
  }

  /**
   * Build a new checkpoint line for an underlying storage checkpoint update if any buckets have changed.
   *
   * This call is idempotent - no internal state is updated directly when this is called.
   *
   * When the checkpoint line is sent to the client, call `CheckpointLine.advance()` to update the internal state.
   * A line may be safely discarded (not sent to the client) if `advance()` is not called.
   *
   * @param next The updated checkpoint
   * @returns A {@link CheckpointLine} if any of the buckets watched by this connected was updated, or otherwise `null`.
   */
  async buildNextCheckpointLine(next: storage.StorageCheckpointUpdate): Promise<CheckpointLine | null> {
    const { writeCheckpoint, base } = next;
    const user_id = this.parameterState.syncParams.userId;

    const storage = this.bucketStorage;

    const update = await this.parameterState.getCheckpointUpdate(next);
    const { buckets: allBuckets, updatedBuckets } = update;

    /** Set of all buckets in this checkpoint. */
    const bucketDescriptionMap = new Map(allBuckets.map((b) => [b.bucket, b]));

    if (bucketDescriptionMap.size > this.context.maxBuckets) {
      throw new ServiceError(
        ErrorCode.PSYNC_S2305,
        `Too many buckets: ${bucketDescriptionMap.size} (limit of ${this.context.maxBuckets})`
      );
    }

    let checksumMap: util.ChecksumMap;
    if (updatedBuckets != INVALIDATE_ALL_BUCKETS) {
      if (this.lastChecksums == null) {
        throw new ServiceAssertionError(`Bucket diff received without existing checksums`);
      }

      // Re-check updated buckets only
      let checksumLookups: string[] = [];

      let newChecksums = new Map<string, util.BucketChecksum>();
      for (let bucket of bucketDescriptionMap.keys()) {
        if (!updatedBuckets.has(bucket)) {
          const existing = this.lastChecksums.get(bucket);
          if (existing == null) {
            // If this happens, it means updatedBuckets did not correctly include all new buckets
            throw new ServiceAssertionError(`Existing checksum not found for bucket ${bucket}`);
          }
          // Bucket is not specifically updated, and we have a previous checksum
          newChecksums.set(bucket, existing);
        } else {
          checksumLookups.push(bucket);
        }
      }

      if (checksumLookups.length > 0) {
        let updatedChecksums = await storage.getChecksums(base.checkpoint, checksumLookups);
        for (let [bucket, value] of updatedChecksums.entries()) {
          newChecksums.set(bucket, value);
        }
      }
      checksumMap = newChecksums;
    } else {
      // Re-check all buckets
      const bucketList = [...bucketDescriptionMap.keys()];
      checksumMap = await storage.getChecksums(base.checkpoint, bucketList);
    }

    // Subset of buckets for which there may be new data in this batch.
    let bucketsToFetch: BucketDescription[];

    let checkpointLine: util.StreamingSyncCheckpointDiff | util.StreamingSyncCheckpoint;

    // Log function that is deferred until the checkpoint line is sent to the client.
    let deferredLog: () => void;

    if (this.lastChecksums) {
      // TODO: If updatedBuckets is present, we can use that to more efficiently calculate a diff,
      // and avoid any unnecessary loops through the entire list of buckets.
      const diff = util.checksumsDiff(this.lastChecksums, checksumMap);
      const streamNameToIndex = this.streamNameToIndex!;

      if (
        this.lastWriteCheckpoint == writeCheckpoint &&
        diff.removedBuckets.length == 0 &&
        diff.updatedBuckets.length == 0
      ) {
        // No changes - don't send anything to the client
        return null;
      }

      let generateBucketsToFetch = new Set<string>();
      for (let bucket of diff.updatedBuckets) {
        generateBucketsToFetch.add(bucket.bucket);
      }
      for (let bucket of this.pendingBucketDownloads) {
        // Bucket from a previous checkpoint that hasn't been downloaded yet.
        // If we still have this bucket, include it in the list of buckets to fetch.
        if (checksumMap.has(bucket)) {
          generateBucketsToFetch.add(bucket);
        }
      }

      const updatedBucketDescriptions = diff.updatedBuckets.map((e) => ({
        ...e,
        ...this.parameterState.translateResolvedBucket(bucketDescriptionMap.get(e.bucket)!, streamNameToIndex)
      }));
      bucketsToFetch = [...generateBucketsToFetch].map((b) => {
        return {
          priority: bucketDescriptionMap.get(b)!.priority,
          bucket: b
        };
      });

      deferredLog = () => {
        let message = `Updated checkpoint: ${base.checkpoint} | `;
        message += `write: ${writeCheckpoint} | `;
        message += `buckets: ${allBuckets.length} | `;
        message += `updated: ${limitedBuckets(diff.updatedBuckets, 20)} | `;
        message += `removed: ${limitedBuckets(diff.removedBuckets, 20)}`;
        this.logger.info(message, {
          checkpoint: base.checkpoint,
          user_id: user_id,
          buckets: allBuckets.length,
          updated: diff.updatedBuckets.length,
          removed: diff.removedBuckets.length
        });
      };

      checkpointLine = {
        checkpoint_diff: {
          last_op_id: util.internalToExternalOpId(base.checkpoint),
          write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
          removed_buckets: diff.removedBuckets,
          updated_buckets: updatedBucketDescriptions
        }
      } satisfies util.StreamingSyncCheckpointDiff;
    } else {
      deferredLog = () => {
        let message = `New checkpoint: ${base.checkpoint} | write: ${writeCheckpoint} | `;
        message += `buckets: ${allBuckets.length} ${limitedBuckets(allBuckets, 20)}`;
        this.logger.info(message, { checkpoint: base.checkpoint, user_id: user_id, buckets: allBuckets.length });
      };
      bucketsToFetch = allBuckets.map((b) => ({ bucket: b.bucket, priority: b.priority }));

      const subscriptions: util.StreamDescription[] = [];
      const streamNameToIndex = new Map<string, number>();
      this.streamNameToIndex = streamNameToIndex;

      for (const source of this.parameterState.syncRules.definition.bucketSources) {
        if (this.parameterState.isSubscribedToStream(source)) {
          streamNameToIndex.set(source.name, subscriptions.length);

          subscriptions.push({
            name: source.name,
            is_default: source.subscribedToByDefault,
            errors:
              this.parameterState.streamErrors[source.name]?.map((e) => ({
                subscription: e.subscription?.opaque_id ?? 'default',
                message: e.message
              })) ?? []
          });
        }
      }

      checkpointLine = {
        checkpoint: {
          last_op_id: util.internalToExternalOpId(base.checkpoint),
          write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
          buckets: [...checksumMap.values()].map((e) => ({
            ...e,
            ...this.parameterState.translateResolvedBucket(bucketDescriptionMap.get(e.bucket)!, streamNameToIndex)
          })),
          streams: subscriptions
        }
      } satisfies util.StreamingSyncCheckpoint;
    }

    const pendingBucketDownloads = new Set(bucketsToFetch.map((b) => b.bucket));

    let hasAdvanced = false;

    return {
      checkpointLine,
      bucketsToFetch,
      advance: () => {
        hasAdvanced = true;
        // bucketDataPositions must be updated in-place - it represents the current state of
        // the connection, not of the checkpoint line.
        // The following could happen:
        // 1. A = buildCheckpointLine()
        // 2. A.advance()
        // 3. B = buildCheckpointLine()
        // 4. A.updateBucketPosition()
        // 5. B.advance()
        // In that case, it is important that the updated bucket position for A takes effect
        // for checkpoint B.
        let bucketsToRemove: string[] = [];
        for (let bucket of this.bucketDataPositions.keys()) {
          if (!bucketDescriptionMap.has(bucket)) {
            bucketsToRemove.push(bucket);
          }
        }
        for (let bucket of bucketsToRemove) {
          this.bucketDataPositions.delete(bucket);
        }
        for (let bucket of allBuckets) {
          if (!this.bucketDataPositions.has(bucket.bucket)) {
            // Bucket the client hasn't seen before - initialize with 0.
            this.bucketDataPositions.set(bucket.bucket, { start_op_id: 0n });
          }
          // If the bucket position is already present, we keep the current position.
        }

        this.lastChecksums = checksumMap;
        this.lastWriteCheckpoint = writeCheckpoint;
        this.pendingBucketDownloads = pendingBucketDownloads;
        deferredLog();
      },

      getFilteredBucketPositions: (buckets?: BucketDescription[]): Map<string, util.InternalOpId> => {
        if (!hasAdvanced) {
          throw new ServiceAssertionError('Call line.advance() before getFilteredBucketPositions()');
        }
        buckets ??= bucketsToFetch;
        const filtered = new Map<string, util.InternalOpId>();

        for (let bucket of buckets) {
          const state = this.bucketDataPositions.get(bucket.bucket);
          if (state) {
            filtered.set(bucket.bucket, state.start_op_id);
          }
        }
        return filtered;
      },

      updateBucketPosition: (options: { bucket: string; nextAfter: util.InternalOpId; hasMore: boolean }) => {
        if (!hasAdvanced) {
          throw new ServiceAssertionError('Call line.advance() before updateBucketPosition()');
        }
        const state = this.bucketDataPositions.get(options.bucket);
        if (state) {
          state.start_op_id = options.nextAfter;
        } else {
          // If we hit this, another checkpoint has removed the bucket in the meantime, meaning
          // line.advance() has been called on it. In that case we don't need the bucket state anymore.
          // It is generally not expected to happen, but we still cover the case.
        }
        if (!options.hasMore) {
          // This specifically updates the per-checkpoint line. Completing a download for one line,
          // does not remove it from the next line, since it could have new updates there.
          pendingBucketDownloads.delete(options.bucket);
        }
      }
    };
  }
}

const INVALIDATE_ALL_BUCKETS = Symbol('INVALIDATE_ALL_BUCKETS');

export interface CheckpointUpdate {
  /**
   * All buckets forming part of the checkpoint.
   */
  buckets: ResolvedBucket[];

  /**
   * If present, a set of buckets that have been updated since the last checkpoint.
   *
   * If null, assume that any bucket in `buckets` may have been updated.
   */
  updatedBuckets: Set<string> | typeof INVALIDATE_ALL_BUCKETS;
}

export class BucketParameterState {
  private readonly context: SyncContext;
  public readonly bucketStorage: BucketChecksumStateStorage;
  public readonly syncRules: HydratedSyncRules;
  public readonly syncParams: RequestParameters;
  private readonly querier: BucketParameterQuerier;
  /**
   * Static buckets. This map is guaranteed not to change during a request, since resolving static buckets can only
   * take request parameters into account,
   */
  private readonly staticBuckets: Map<string, ResolvedBucket>;
  private readonly includeDefaultStreams: boolean;
  // Indexed by the client-side id
  private readonly explicitStreamSubscriptions: util.RequestedStreamSubscription[];
  // Indexed by descriptor name.
  readonly streamErrors: Record<string, QuerierError[]>;
  private readonly subscribedStreamNames: Set<string>;
  private readonly logger: Logger;
  private cachedDynamicBuckets: ResolvedBucket[] | null = null;
  private cachedDynamicBucketSet: Set<string> | null = null;

  private lookupsFromPreviousCheckpoint: Set<string> | null = null;

  constructor(
    context: SyncContext,
    bucketStorage: BucketChecksumStateStorage,
    syncRules: HydratedSyncRules,
    tokenPayload: RequestJwtPayload,
    request: util.StreamingSyncRequest,
    logger: Logger
  ) {
    this.context = context;
    this.bucketStorage = bucketStorage;
    this.syncRules = syncRules;
    this.syncParams = new RequestParameters(tokenPayload, request.parameters ?? {});
    this.logger = logger;

    const streamsByName: Record<string, RequestedStream[]> = {};
    const subscriptions = request.streams;
    const explicitStreamSubscriptions: util.RequestedStreamSubscription[] = subscriptions?.subscriptions ?? [];
    if (subscriptions) {
      for (let i = 0; i < explicitStreamSubscriptions.length; i++) {
        const subscription = explicitStreamSubscriptions[i];

        const syncRuleStream: RequestedStream = {
          parameters: subscription.parameters ?? {},
          opaque_id: i
        };
        if (Object.hasOwn(streamsByName, subscription.stream)) {
          streamsByName[subscription.stream].push(syncRuleStream);
        } else {
          streamsByName[subscription.stream] = [syncRuleStream];
        }
      }
    }
    this.includeDefaultStreams = subscriptions?.include_defaults ?? true;
    this.explicitStreamSubscriptions = explicitStreamSubscriptions;

    const { querier, errors } = syncRules.getBucketParameterQuerier({
      globalParameters: this.syncParams,
      hasDefaultStreams: this.includeDefaultStreams,
      streams: streamsByName
    });
    this.querier = querier;
    this.streamErrors = Object.groupBy(errors, (e) => e.descriptor) as Record<string, QuerierError[]>;

    this.staticBuckets = new Map<string, ResolvedBucket>(
      mergeBuckets(this.querier.staticBuckets).map((b) => [b.bucket, b])
    );
    this.subscribedStreamNames = new Set(Object.keys(streamsByName));
  }

  /**
   * Translates an internal sync-rules {@link ResolvedBucket} instance to the public
   * {@link util.ClientBucketDescription}.
   *
   * @param lookupIndex A map from stream names to their index in {@link util.Checkpoint.streams}. These are used to
   * reference default buckets by their stream index instead of duplicating the name on wire.
   */
  translateResolvedBucket(description: ResolvedBucket, lookupIndex: Map<string, number>): util.ClientBucketDescription {
    // If the client is overriding the priority of any stream that yields this bucket, sync the bucket with that
    // priority.
    let priorityOverride: BucketPriority | null = null;
    for (const reason of description.inclusion_reasons) {
      if (reason != 'default') {
        const requestedPriority = this.explicitStreamSubscriptions[reason.subscription]?.override_priority;
        if (requestedPriority != null) {
          if (priorityOverride == null) {
            priorityOverride = requestedPriority as BucketPriority;
          } else {
            priorityOverride = Math.min(requestedPriority, priorityOverride) as BucketPriority;
          }
        }
      }
    }

    return {
      bucket: description.bucket,
      priority: priorityOverride ?? description.priority,
      subscriptions: description.inclusion_reasons.map((reason) => {
        if (reason == 'default') {
          const stream = description.definition;
          return { default: lookupIndex.get(stream)! };
        } else {
          return { sub: reason.subscription };
        }
      })
    };
  }

  isSubscribedToStream(desc: BucketSource): boolean {
    return (desc.subscribedToByDefault && this.includeDefaultStreams) || this.subscribedStreamNames.has(desc.name);
  }

  async getCheckpointUpdate(checkpoint: storage.StorageCheckpointUpdate): Promise<CheckpointUpdate> {
    const querier = this.querier;
    let update: CheckpointUpdate;
    if (querier.hasDynamicBuckets) {
      update = await this.getCheckpointUpdateDynamic(checkpoint);
    } else {
      update = await this.getCheckpointUpdateStatic(checkpoint);
    }

    if (update.buckets.length > this.context.maxParameterQueryResults) {
      // TODO: Limit number of results even before we get to this point
      // This limit applies _before_ we get the unique set
      const error = new ServiceError(
        ErrorCode.PSYNC_S2305,
        `Too many parameter query results: ${update.buckets.length} (limit of ${this.context.maxParameterQueryResults})`
      );
      this.logger.error(error.message, {
        checkpoint: checkpoint,
        user_id: this.syncParams.userId,
        buckets: update.buckets.length
      });

      throw error;
    }
    return update;
  }

  /**
   * For static buckets, we can keep track of which buckets have been updated.
   */
  private async getCheckpointUpdateStatic(checkpoint: storage.StorageCheckpointUpdate): Promise<CheckpointUpdate> {
    const staticBuckets = [...this.staticBuckets.values()];
    const update = checkpoint.update;

    if (update.invalidateDataBuckets) {
      return {
        buckets: staticBuckets,
        updatedBuckets: INVALIDATE_ALL_BUCKETS
      };
    }

    const updatedBuckets = new Set<string>(getIntersection(this.staticBuckets, update.updatedDataBuckets));
    return {
      buckets: staticBuckets,
      updatedBuckets
    };
  }

  /**
   * For dynamic buckets, we need to re-query the list of buckets every time.
   */
  private async getCheckpointUpdateDynamic(checkpoint: storage.StorageCheckpointUpdate): Promise<CheckpointUpdate> {
    const querier = this.querier;
    const staticBuckets = this.staticBuckets.values();
    const update = checkpoint.update;

    let hasParameterChange = false;
    let invalidateDataBuckets = false;
    // If hasParameterChange == true, then invalidateDataBuckets = true
    // If invalidateDataBuckets == true, we ignore updatedBuckets
    let updatedBuckets = new Set<string>();

    if (update.invalidateDataBuckets) {
      invalidateDataBuckets = true;
    }

    if (update.invalidateParameterBuckets || this.lookupsFromPreviousCheckpoint == null) {
      hasParameterChange = true;
    } else {
      if (hasIntersection(this.lookupsFromPreviousCheckpoint, update.updatedParameterLookups)) {
        // This is a very coarse re-check of all queries
        hasParameterChange = true;
      }
    }

    let dynamicBuckets: ResolvedBucket[];
    if (hasParameterChange || this.cachedDynamicBuckets == null || this.cachedDynamicBucketSet == null) {
      const recordedLookups = new Set<string>();

      dynamicBuckets = await querier.queryDynamicBucketDescriptions({
        getParameterSets(lookups) {
          for (const lookup of lookups) {
            recordedLookups.add(lookup.serializedRepresentation);
          }

          return checkpoint.base.getParameterSets(lookups);
        }
      });
      this.cachedDynamicBuckets = dynamicBuckets;
      this.cachedDynamicBucketSet = new Set<string>(dynamicBuckets.map((b) => b.bucket));
      this.lookupsFromPreviousCheckpoint = recordedLookups;
      invalidateDataBuckets = true;
    } else {
      dynamicBuckets = this.cachedDynamicBuckets;

      if (!invalidateDataBuckets) {
        for (let bucket of getIntersection(this.staticBuckets, update.updatedDataBuckets)) {
          updatedBuckets.add(bucket);
        }
        for (let bucket of getIntersection(this.cachedDynamicBucketSet, update.updatedDataBuckets)) {
          updatedBuckets.add(bucket);
        }
      }
    }
    const allBuckets = [...staticBuckets, ...mergeBuckets(dynamicBuckets)];

    if (invalidateDataBuckets) {
      return {
        buckets: allBuckets,
        // We cannot track individual bucket updates for dynamic lookups yet
        updatedBuckets: INVALIDATE_ALL_BUCKETS
      };
    } else {
      return {
        buckets: allBuckets,
        updatedBuckets: updatedBuckets
      };
    }
  }
}

export interface CheckpointLine {
  checkpointLine: util.StreamingSyncCheckpointDiff | util.StreamingSyncCheckpoint;
  bucketsToFetch: BucketDescription[];

  /**
   * Call when a checkpoint line is being sent to a client, to update the internal state.
   */
  advance: () => void;

  /**
   * Get bucket positions to sync, given the list of buckets.
   *
   * @param bucketsToFetch List of buckets to fetch - either this.bucketsToFetch, or a subset of it. Defaults to this.bucketsToFetch.
   */
  getFilteredBucketPositions(bucketsToFetch?: BucketDescription[]): Map<string, util.InternalOpId>;

  /**
   * Update the position of bucket data the client has, after it was sent to the client.
   *
   * @param bucket the bucket name
   * @param nextAfter sync operations >= this value in the next batch
   */
  updateBucketPosition(options: { bucket: string; nextAfter: util.InternalOpId; hasMore: boolean }): void;
}

// Use a more specific type to simplify testing
export type BucketChecksumStateStorage = Pick<storage.SyncRulesBucketStorage, 'getChecksums'>;

function limitedBuckets(buckets: string[] | { bucket: string }[], limit: number) {
  buckets = buckets.map((b) => {
    if (typeof b != 'string') {
      return b.bucket;
    } else {
      return b;
    }
  });
  if (buckets.length <= limit) {
    return JSON.stringify(buckets);
  }
  const limited = buckets.slice(0, limit);
  return `${JSON.stringify(limited)}...`;
}

/**
 * Resolves duplicate buckets in the given array, merging the inclusion reasons for duplicate.
 *
 * It's possible for duplicates to occur when a stream has multiple subscriptions, consider e.g.
 *
 * ```
 * sync_streams:
 *  assets_by_category:
 *    query: select * from assets where category in (request.parameters() -> 'categories')
 * ```
 *
 * Here, a client might subscribe once with `{"categories": [1]}` and once with `{"categories": [1, 2]}`. Since each
 * subscription is evaluated independently, this would lead to three buckets, with a duplicate `assets_by_category[1]`
 * bucket.
 */
function mergeBuckets(buckets: ResolvedBucket[]): ResolvedBucket[] {
  const byBucketId: Record<string, ResolvedBucket> = {};

  for (const bucket of buckets) {
    if (Object.hasOwn(byBucketId, bucket.bucket)) {
      byBucketId[bucket.bucket].inclusion_reasons.push(...bucket.inclusion_reasons);
    } else {
      byBucketId[bucket.bucket] = structuredClone(bucket);
    }
  }

  return Object.values(byBucketId);
}
