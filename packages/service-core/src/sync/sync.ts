import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import { BucketDescription, BucketPriority, RequestParameters, SqlSyncRules } from '@powersync/service-sync-rules';
import { Semaphore, withTimeout } from 'async-mutex';

import { AbortError } from 'ix/aborterror.js';

import * as auth from '../auth/auth-index.js';
import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import { ErrorCode, logger, ServiceAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { BucketParameterQuerier } from '@powersync/service-sync-rules/src/BucketParameterQuerier.js';
import { mergeAsyncIterables } from './merge.js';
import { RequestTracker } from './RequestTracker.js';
import { TokenStreamOptions, acquireSemaphoreAbortable, tokenStream } from './util.js';

/**
 * Maximum number of connections actively fetching data.
 */
const MAX_ACTIVE_CONNECTIONS = 10;

/**
 * Maximum duration to wait for the mutex to become available.
 *
 * This gives an explicit error if there are mutex issues, rather than just hanging.
 */
const MUTEX_ACQUIRE_TIMEOUT = 30_000;

const syncSemaphore = withTimeout(
  new Semaphore(MAX_ACTIVE_CONNECTIONS),
  MUTEX_ACQUIRE_TIMEOUT,
  new Error(`Timeout while waiting for data`)
);

export interface SyncStreamParameters {
  bucketStorage: storage.SyncRulesBucketStorage;
  syncRules: SqlSyncRules;
  params: util.StreamingSyncRequest;
  syncParams: RequestParameters;
  token: auth.JwtPayload;
  /**
   * If this signal is aborted, the stream response ends as soon as possible, without error.
   */
  signal?: AbortSignal;
  tokenStreamOptions?: Partial<TokenStreamOptions>;

  tracker: RequestTracker;
}

export async function* streamResponse(
  options: SyncStreamParameters
): AsyncIterable<util.StreamingSyncLine | string | null> {
  const { bucketStorage, syncRules, params, syncParams, token, tokenStreamOptions, tracker, signal } = options;
  // We also need to be able to abort, so we create our own controller.
  const controller = new AbortController();
  if (signal) {
    signal.addEventListener(
      'abort',
      () => {
        controller.abort();
      },
      { once: true }
    );
    if (signal.aborted) {
      controller.abort();
    }
  }
  const ki = tokenStream(token, controller.signal, tokenStreamOptions);
  const stream = streamResponseInner(bucketStorage, syncRules, params, syncParams, tracker, controller.signal);
  // Merge the two streams, and abort as soon as one of the streams end.
  const merged = mergeAsyncIterables([stream, ki], controller.signal);

  try {
    yield* merged;
  } catch (e) {
    if (e instanceof AbortError) {
      return;
    } else {
      throw e;
    }
  } finally {
    // This ensures all the underlying streams are aborted as soon as possible if the
    // parent loop stops.
    controller.abort();
  }
}

type BucketSyncState = {
  description?: BucketDescription; // Undefined if the bucket has not yet been resolved by us.
  start_op_id: string;
};

async function* streamResponseInner(
  bucketStorage: storage.SyncRulesBucketStorage,
  syncRules: SqlSyncRules,
  params: util.StreamingSyncRequest,
  syncParams: RequestParameters,
  tracker: RequestTracker,
  signal: AbortSignal
): AsyncGenerator<util.StreamingSyncLine | string | null> {
  // Bucket state of bucket id -> op_id.
  // This starts with the state from the client. May contain buckets that the user do not have access to (anymore).
  let initialBuckets = new Map<string, BucketSyncState>();

  const { raw_data, binary_data } = params;

  if (params.buckets) {
    for (let { name, after: start } of params.buckets) {
      initialBuckets.set(name, { start_op_id: start });
    }
  }

  const checkpointUserId = util.checkpointUserId(syncParams.token_parameters.user_id as string, params.client_id);

  const checksumState = new BucketChecksumState(bucketStorage, syncRules, syncParams, initialBuckets);
  const stream = bucketStorage.watchWriteCheckpoint({
    user_id: checkpointUserId,
    signal,
    filter: checksumState.checkpointFilter
  });
  const newCheckpoints = stream[Symbol.asyncIterator]();

  try {
    let nextCheckpointPromise: Promise<IteratorResult<storage.WriteCheckpoint>> | undefined;

    do {
      if (!nextCheckpointPromise) {
        nextCheckpointPromise = newCheckpoints.next();
      }
      const next = await nextCheckpointPromise;
      nextCheckpointPromise = undefined;
      if (next.done) {
        break;
      }
      const line = await checksumState.buildNextCheckpointLine(next.value);
      if (line == null) {
        // No update to sync
        continue;
      }

      const { checkpointLine, bucketsToFetch } = line;

      yield checkpointLine;
      // Start syncing data for buckets up to the checkpoint. As soon as we have completed at least one priority and
      // at least 1000 operations, we also start listening for new checkpoints concurrently. When a new checkpoint comes
      // in while we're still busy syncing data for lower priorities, interrupt the current operation and start syncing
      // the new checkpoint.
      const abortCheckpointController = new AbortController();
      let didCompletePartialSync = false;
      let syncedOperations = 0;

      const abortCheckpointSignal = AbortSignal.any([abortCheckpointController.signal, signal]);

      const bucketsByPriority = [...Map.groupBy(bucketsToFetch, (bucket) => bucket.priority).entries()];
      bucketsByPriority.sort((a, b) => b[0] - a[0]); // Inverting sort order, high priority buckets have smaller priority values
      const lowestPriority = bucketsByPriority.at(-1)?.[0];

      function maybeRaceForNewCheckpoint() {
        if (didCompletePartialSync && syncedOperations >= 1000 && nextCheckpointPromise === undefined) {
          nextCheckpointPromise = (async () => {
            const next = await newCheckpoints.next();
            if (!next.done) {
              // Stop the running bucketDataInBatches() iterations, making the main flow reach the new checkpoint.
              abortCheckpointController.abort();
            }

            return next;
          })();
        }
      }

      function markOperationsSent(operations: number) {
        syncedOperations += operations;
        tracker.addOperationsSynced(operations);
        maybeRaceForNewCheckpoint();
      }

      // This incrementally updates dataBuckets with each individual bucket position.
      // At the end of this, we can be sure that all buckets have data up to the checkpoint.
      for (const [priority, buckets] of bucketsByPriority) {
        if (abortCheckpointSignal.aborted) {
          break;
        }

        const isLast = priority === lowestPriority;
        yield* bucketDataInBatches({
          bucketStorage: bucketStorage,
          checkpoint: next.value.base.checkpoint,
          bucketsToFetch: buckets,
          checksumState,
          raw_data,
          binary_data,
          onRowsSent: markOperationsSent,
          abort_connection: signal,
          abort_batch: abortCheckpointSignal,
          user_id: syncParams.user_id,
          // Passing undefined will emit a full sync complete message at the end. If we pass a priority, we'll emit a partial
          // sync complete message.
          forPriority: !isLast ? priority : undefined
        });

        didCompletePartialSync = true;
        maybeRaceForNewCheckpoint();
      }

      if (!abortCheckpointSignal.aborted) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    } while (!signal.aborted);
  } finally {
    await newCheckpoints.return?.();
  }
}

interface BucketDataRequest {
  bucketStorage: storage.SyncRulesBucketStorage;
  checkpoint: string;
  bucketsToFetch: BucketDescription[];
  /** Contains current bucket state. Modified by the request as data is sent.  */
  checksumState: BucketChecksumState;
  raw_data: boolean | undefined;
  binary_data: boolean | undefined;
  /** Signals that the connection was aborted and that streaming should stop ASAP. */
  abort_connection: AbortSignal;
  /**
   * Signals that higher-priority batches are available. The current batch can stop at a sensible point.
   * This signal also fires when abort_connection fires.
   */
  abort_batch: AbortSignal;
  user_id?: string;
  forPriority?: BucketPriority;
  onRowsSent: (amount: number) => void;
}

async function* bucketDataInBatches(request: BucketDataRequest) {
  let isDone = false;
  while (!request.abort_connection.aborted && !isDone) {
    // The code below is functionally the same as this for-await loop below.
    // However, the for-await loop appears to have a memory leak, so we avoid it.
    // for await (const { done, data } of bucketDataBatch(storage, checkpoint, dataBuckets, raw_data, signal)) {
    //   yield data;
    //   if (done) {
    //     isDone = true;
    //   }
    //   break;
    // }
    const iter = bucketDataBatch(request);
    try {
      while (true) {
        const { value, done: iterDone } = await iter.next();
        if (iterDone) {
          break;
        } else {
          const { done, data } = value;
          yield data;
          if (done) {
            isDone = true;
          }
        }
      }
    } finally {
      await iter.return();
    }
  }
}

interface BucketDataBatchResult {
  done: boolean;
  data: any;
}

/**
 * Extracted as a separate internal function just to avoid memory leaks.
 */
async function* bucketDataBatch(request: BucketDataRequest): AsyncGenerator<BucketDataBatchResult, void> {
  const {
    bucketStorage: storage,
    checkpoint,
    bucketsToFetch,
    checksumState,
    raw_data,
    binary_data,
    abort_connection,
    abort_batch,
    onRowsSent
  } = request;

  const checkpointOp = BigInt(checkpoint);
  let checkpointInvalidated = false;

  if (syncSemaphore.isLocked()) {
    logger.info('Sync concurrency limit reached, waiting for lock', { user_id: request.user_id });
  }
  const acquired = await acquireSemaphoreAbortable(syncSemaphore, AbortSignal.any([abort_batch]));
  if (acquired === 'aborted') {
    return;
  }

  const [value, release] = acquired;
  try {
    if (value <= 3) {
      // This can be noisy, so we only log when we get close to the
      // concurrency limit.
      logger.info(`Got sync lock. Slots available: ${value - 1}`, {
        user_id: request.user_id,
        sync_data_slots: value - 1
      });
    }
    // Optimization: Only fetch buckets for which the checksums have changed since the last checkpoint
    // For the first batch, this will be all buckets.
    const filteredBuckets = checksumState.getFilteredBucketStates(bucketsToFetch);
    const data = storage.getBucketDataBatch(checkpoint, filteredBuckets);

    let has_more = false;

    for await (let { batch: r, targetOp } of data) {
      // Abort in current batch if the connection is closed
      if (abort_connection.aborted) {
        return;
      }
      if (r.has_more) {
        has_more = true;
      }
      if (targetOp != null && targetOp > checkpointOp) {
        checkpointInvalidated = true;
      }
      if (r.data.length == 0) {
        continue;
      }
      logger.debug(`Sending data for ${r.bucket}`);

      let send_data: any;
      if (binary_data) {
        // Send the object as is, will most likely be encoded as a BSON document
        send_data = { data: r };
      } else if (raw_data) {
        /**
         * Data is a raw string - we can use the more efficient JSON.stringify.
         */
        const response: util.StreamingSyncData = {
          data: r
        };
        send_data = JSON.stringify(response);
      } else {
        // We need to preserve the embedded data exactly, so this uses a JsonContainer
        // and JSONBig to stringify.
        const response: util.StreamingSyncData = {
          data: transformLegacyResponse(r)
        };
        send_data = JSONBig.stringify(response);
      }
      yield { data: send_data, done: false };
      if (send_data.length > 50_000) {
        // IMPORTANT: This does not affect the output stream, but is used to flush
        // iterator memory in case if large data sent.
        yield { data: null, done: false };
      }
      onRowsSent(r.data.length);

      checksumState.updateState(r.bucket, r.next_after);

      // Check if syncing bucket data is supposed to stop before fetching more data
      // from storage.
      if (abort_batch.aborted) {
        break;
      }
    }

    if (!has_more) {
      if (checkpointInvalidated) {
        // Checkpoint invalidated by a CLEAR or MOVE op.
        // Don't send the checkpoint_complete line in this case.
        // More data should be available immediately for a new checkpoint.
        yield { data: null, done: true };
      } else {
        if (request.forPriority !== undefined) {
          const line: util.StreamingSyncCheckpointPartiallyComplete = {
            partial_checkpoint_complete: {
              last_op_id: checkpoint,
              priority: request.forPriority
            }
          };
          yield { data: line, done: true };
        } else {
          const line: util.StreamingSyncCheckpointComplete = {
            checkpoint_complete: {
              last_op_id: checkpoint
            }
          };
          yield { data: line, done: true };
        }
      }
    }
  } finally {
    if (value <= 3) {
      // This can be noisy, so we only log when we get close to the
      // concurrency limit.
      logger.info(`Releasing sync lock`, {
        user_id: request.user_id
      });
    }
    release();
  }
}

function transformLegacyResponse(bucketData: util.SyncBucketData): any {
  return {
    ...bucketData,
    data: bucketData.data.map((entry) => {
      return {
        ...entry,
        data: entry.data == null ? null : new JsonContainer(entry.data as string),
        checksum: BigInt(entry.checksum)
      };
    })
  };
}

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

interface CheckpointLine {
  checkpointLine: util.StreamingSyncCheckpointDiff | util.StreamingSyncCheckpoint;
  bucketsToFetch: BucketDescription[];
}

export class BucketChecksumState {
  // Bucket state of bucket id -> op_id.
  // This starts with the state from the client. May contain buckets that the user do not have access to (anymore).
  private dataBuckets = new Map<string, BucketSyncState>();

  private lastChecksums: util.ChecksumMap | null = null;
  private lastWriteCheckpoint: bigint | null = null;

  private readonly parameterState: BucketParameterState;

  constructor(
    private bucketStorage: storage.SyncRulesBucketStorage,
    syncRules: SqlSyncRules,
    syncParams: RequestParameters,
    initialBucketState: Map<string, BucketSyncState>
  ) {
    this.parameterState = new BucketParameterState(bucketStorage, syncRules, syncParams);
    this.dataBuckets = initialBucketState;
  }

  async buildNextCheckpointLine(next: storage.WriteCheckpoint): Promise<CheckpointLine | null> {
    const { writeCheckpoint, base } = next;
    const user_id = this.parameterState.syncParams.user_id;

    const storage = this.bucketStorage;

    const { buckets: allBuckets, updatedBuckets } = await this.parameterState.getCheckpointUpdate(base.checkpoint);

    let dataBucketsNew = new Map<string, BucketSyncState>();
    for (let bucket of allBuckets) {
      dataBucketsNew.set(bucket.bucket, {
        description: bucket,
        start_op_id: this.dataBuckets.get(bucket.bucket)?.start_op_id ?? '0'
      });
    }
    this.dataBuckets = dataBucketsNew;

    let checksumMap: util.ChecksumMap;
    if (updatedBuckets != null) {
      if (this.lastChecksums == null) {
        throw new ServiceAssertionError(`Bucket diff received without existing checksums`);
      }

      // Re-check updated buckets only
      let checksumLookups: string[] = [];

      let newChecksums = new Map<string, util.BucketChecksum>();
      for (let bucket of dataBucketsNew.keys()) {
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

      let updatedChecksums = await storage.getChecksums(base.checkpoint, checksumLookups);
      for (let [bucket, value] of updatedChecksums.entries()) {
        newChecksums.set(bucket, value);
      }
      checksumMap = newChecksums;
    } else {
      // Re-check all buckets
      const bucketList = [...dataBucketsNew.keys()];
      checksumMap = await storage.getChecksums(base.checkpoint, bucketList);
    }
    // Subset of buckets for which there may be new data in this batch.
    let bucketsToFetch: BucketDescription[];

    let checkpointLine: util.StreamingSyncCheckpointDiff | util.StreamingSyncCheckpoint;

    if (this.lastChecksums) {
      // TODO: If updatedBuckets is present, we can use that to more efficiently calculate a diff,
      // and avoid any unnecessary loops through the entire list of buckets.
      const diff = util.checksumsDiff(this.lastChecksums, checksumMap);

      if (
        this.lastWriteCheckpoint == writeCheckpoint &&
        diff.removedBuckets.length == 0 &&
        diff.updatedBuckets.length == 0
      ) {
        // No changes - don't send anything to the client
        return null;
      }

      const updatedBucketDescriptions = diff.updatedBuckets.map((e) => ({
        ...e,
        priority: this.dataBuckets.get(e.bucket)!.description!.priority
      }));
      bucketsToFetch = updatedBucketDescriptions;

      let message = `Updated checkpoint: ${base.checkpoint} | `;
      message += `write: ${writeCheckpoint} | `;
      message += `buckets: ${allBuckets.length} | `;
      message += `updated: ${limitedBuckets(diff.updatedBuckets, 20)} | `;
      message += `removed: ${limitedBuckets(diff.removedBuckets, 20)}`;
      logger.info(message, {
        checkpoint: base.checkpoint,
        user_id: user_id,
        buckets: allBuckets.length,
        updated: diff.updatedBuckets.length,
        removed: diff.removedBuckets.length
      });

      checkpointLine = {
        checkpoint_diff: {
          last_op_id: base.checkpoint,
          write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
          removed_buckets: diff.removedBuckets,
          updated_buckets: updatedBucketDescriptions
        }
      } satisfies util.StreamingSyncCheckpointDiff;
    } else {
      let message = `New checkpoint: ${base.checkpoint} | write: ${writeCheckpoint} | `;
      message += `buckets: ${allBuckets.length} ${limitedBuckets(allBuckets, 20)}`;
      logger.info(message, { checkpoint: base.checkpoint, user_id: user_id, buckets: allBuckets.length });
      bucketsToFetch = allBuckets;
      checkpointLine = {
        checkpoint: {
          last_op_id: base.checkpoint,
          write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
          buckets: [...checksumMap.values()].map((e) => ({
            ...e,
            priority: this.dataBuckets.get(e.bucket)!.description!.priority
          }))
        }
      } satisfies util.StreamingSyncCheckpoint;
    }

    this.lastChecksums = checksumMap;
    this.lastWriteCheckpoint = writeCheckpoint;

    return {
      checkpointLine,
      bucketsToFetch
    };
  }

  get checkpointFilter() {
    return this.parameterState.checkpointFilter;
  }

  getFilteredBucketStates(bucketsToFetch: BucketDescription[]): Map<string, string> {
    const filtered = new Map<string, string>();
    for (let bucket of bucketsToFetch) {
      const state = this.dataBuckets.get(bucket.bucket);
      if (state) {
        filtered.set(bucket.bucket, state.start_op_id);
      }
    }
    return filtered;
  }

  updateState(bucket: string, nextAfter: string) {
    const state = this.dataBuckets.get(bucket);
    if (state) {
      state.start_op_id = nextAfter;
    }
  }
}

interface InternalBucketParameterCache {
  querier: BucketParameterQuerier;
  staticBuckets: Map<string, BucketDescription>;
}

interface CheckpointUpdate {
  /**
   * All buckets forming part of the checkpoint.
   */
  buckets: BucketDescription[];

  /**
   * If present, a set of buckets that have been updated since the last checkpoint.
   *
   * If null, assume that any bucket in `buckets` may have been updated.
   */
  updatedBuckets: Set<string> | null;
}

class BucketParameterState {
  public readonly bucketStorage: storage.SyncRulesBucketStorage;
  public readonly syncRules: SqlSyncRules;
  public readonly syncParams: RequestParameters;
  private readonly querier: BucketParameterQuerier;
  private readonly staticBuckets: Map<string, BucketDescription>;

  private pendingBuckets = new Set<string>();
  // First time we're called, we need to fetch all buckets.
  private invalidated: boolean = true;

  constructor(bucketStorage: storage.SyncRulesBucketStorage, syncRules: SqlSyncRules, syncParams: RequestParameters) {
    this.bucketStorage = bucketStorage;
    this.syncRules = syncRules;
    this.syncParams = syncParams;

    this.querier = syncRules.getBucketParameterQuerier(this.syncParams);
    this.staticBuckets = new Map<string, BucketDescription>(this.querier.staticBuckets.map((b) => [b.bucket, b]));
  }

  checkpointFilter = (event: storage.WatchFilterEvent): boolean => {
    if (this.invalidated) {
      return true;
    } else if (event.invalidate) {
      this.invalidated = true;
      this.pendingBuckets.clear();
      return true;
    } else if (event.bucket != null) {
      const querier = this.querier;
      const staticBuckets = this.staticBuckets;
      if (querier.hasDynamicBuckets) {
        // TODO: Check if the dynamic buckets may match this one
        this.invalidated = true;
        this.pendingBuckets.clear();
        return true;
      }

      if (staticBuckets.has(event.bucket)) {
        this.pendingBuckets.add(event.bucket);
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  };

  async getCheckpointUpdate(checkpoint: util.OpId): Promise<CheckpointUpdate> {
    const querier = this.querier;
    let update: CheckpointUpdate;
    if (querier.hasDynamicBuckets) {
      update = await this.getCheckpointUpdateDynamic(checkpoint);
    } else {
      update = await this.getCheckpointUpdateStatic();
    }

    if (update.buckets.length > 1000) {
      // TODO: Limit number of buckets even before we get to this point
      const error = new ServiceError(ErrorCode.PSYNC_S2305, `Too many buckets: ${update.buckets.length}`);
      logger.error(error.message, {
        checkpoint: checkpoint,
        user_id: this.syncParams.user_id,
        buckets: update.buckets.length
      });

      throw error;
    }
    return update;
  }

  private async getCheckpointUpdateStatic(): Promise<CheckpointUpdate> {
    const querier = this.querier;

    // This case is optimized to track pending buckets.
    const staticBuckets = querier.staticBuckets;
    let updatedBuckets: Set<string> | null = this.pendingBuckets;
    if (this.invalidated) {
      // Invalidation event, for example on a sync rule update.
      // We do not have the individual updated buckets in this case.
      updatedBuckets = null;
    }

    this.pendingBuckets = new Set();
    this.invalidated = false;

    return {
      buckets: staticBuckets,
      updatedBuckets
    };
  }

  private async getCheckpointUpdateDynamic(checkpoint: util.OpId): Promise<CheckpointUpdate> {
    const querier = this.querier;
    const storage = this.bucketStorage;
    const staticBuckets = querier.staticBuckets;
    const dynamicBuckets = await querier.queryDynamicBucketDescriptions({
      getParameterSets(lookups) {
        return storage.getParameterSets(checkpoint, lookups);
      }
    });
    const allBuckets = [...staticBuckets, ...dynamicBuckets];

    return {
      buckets: allBuckets,
      // We cannot track individual bucket updates for dynamic lookups yet
      updatedBuckets: null
    };
  }
}
