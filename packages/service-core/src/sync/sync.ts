import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import { BucketDescription, BucketPriority, RequestParameters } from '@powersync/service-sync-rules';
import { Semaphore, withTimeout } from 'async-mutex';

import { AbortError } from 'ix/aborterror.js';

import * as auth from '../auth/auth-index.js';
import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import { logger } from '@powersync/lib-services-framework';
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
  storage: storage.BucketStorageFactory;
  params: util.StreamingSyncRequest;
  syncParams: RequestParameters;
  token: auth.JwtPayload;
  parseOptions: storage.ParseSyncRulesOptions;
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
  const { storage, params, syncParams, token, tokenStreamOptions, tracker, signal, parseOptions } = options;
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
  const stream = streamResponseInner(storage, params, syncParams, tracker, parseOptions, controller.signal);
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
  storage: storage.BucketStorageFactory,
  params: util.StreamingSyncRequest,
  syncParams: RequestParameters,
  tracker: RequestTracker,
  parseOptions: storage.ParseSyncRulesOptions,
  signal: AbortSignal
): AsyncGenerator<util.StreamingSyncLine | string | null> {
  // Bucket state of bucket id -> op_id.
  // This starts with the state from the client. May contain buckets that the user do not have access to (anymore).
  let dataBuckets = new Map<string, BucketSyncState>();

  let lastChecksums: util.ChecksumMap | null = null;
  let lastWriteCheckpoint: bigint | null = null;

  const { raw_data, binary_data } = params;

  if (params.buckets) {
    for (let { name, after: start } of params.buckets) {
      dataBuckets.set(name, { start_op_id: start });
    }
  }

  const checkpointUserId = util.checkpointUserId(syncParams.token_parameters.user_id as string, params.client_id);
  const stream = storage.watchWriteCheckpoint(checkpointUserId, signal);
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

      const { base, writeCheckpoint } = next.value;
      const checkpoint = base.checkpoint;

      const storage = await base.getBucketStorage();
      if (storage == null) {
        // Sync rules deleted in the meantime - try again with the next checkpoint.
        continue;
      }
      const syncRules = storage.getParsedSyncRules(parseOptions);

      const allBuckets = await syncRules.queryBucketDescriptions({
        getParameterSets(lookups) {
          return storage.getParameterSets(checkpoint, lookups);
        },
        parameters: syncParams
      });

      if (allBuckets.length > 1000) {
        logger.error(`Too many buckets`, {
          checkpoint,
          user_id: syncParams.user_id,
          buckets: allBuckets.length
        });
        // TODO: Limit number of buckets even before we get to this point
        throw new Error(`Too many buckets: ${allBuckets.length}`);
      }

      let dataBucketsNew = new Map<string, BucketSyncState>();
      for (let bucket of allBuckets) {
        dataBucketsNew.set(bucket.bucket, {
          description: bucket,
          start_op_id: dataBuckets.get(bucket.bucket)?.start_op_id ?? '0'
        });
      }
      dataBuckets = dataBucketsNew;

      const bucketList = [...dataBuckets.keys()];
      const checksumMap = await storage.getChecksums(checkpoint, bucketList);
      // Subset of buckets for which there may be new data in this batch.
      let bucketsToFetch: BucketDescription[];

      if (lastChecksums) {
        const diff = util.checksumsDiff(lastChecksums, checksumMap);

        if (
          lastWriteCheckpoint == writeCheckpoint &&
          diff.removedBuckets.length == 0 &&
          diff.updatedBuckets.length == 0
        ) {
          // No changes - don't send anything to the client
          continue;
        }
        const updatedBucketDescriptions = diff.updatedBuckets.map((e) => ({
          ...e,
          priority: dataBuckets.get(e.bucket)!.description!.priority
        }));
        bucketsToFetch = updatedBucketDescriptions;

        let message = `Updated checkpoint: ${checkpoint} | `;
        message += `write: ${writeCheckpoint} | `;
        message += `buckets: ${allBuckets.length} | `;
        message += `updated: ${limitedBuckets(diff.updatedBuckets, 20)} | `;
        message += `removed: ${limitedBuckets(diff.removedBuckets, 20)}`;
        logger.info(message, {
          checkpoint,
          user_id: syncParams.user_id,
          buckets: allBuckets.length,
          updated: diff.updatedBuckets.length,
          removed: diff.removedBuckets.length
        });

        const checksum_line: util.StreamingSyncCheckpointDiff = {
          checkpoint_diff: {
            last_op_id: checkpoint,
            write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
            removed_buckets: diff.removedBuckets,
            updated_buckets: updatedBucketDescriptions
          }
        };

        yield checksum_line;
      } else {
        let message = `New checkpoint: ${checkpoint} | write: ${writeCheckpoint} | `;
        message += `buckets: ${allBuckets.length} ${limitedBuckets(allBuckets, 20)}`;
        logger.info(message, { checkpoint, user_id: syncParams.user_id, buckets: allBuckets.length });
        bucketsToFetch = allBuckets;
        const checksum_line: util.StreamingSyncCheckpoint = {
          checkpoint: {
            last_op_id: checkpoint,
            write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
            buckets: [...checksumMap.values()].map((e) => ({
              ...e,
              priority: dataBuckets.get(e.bucket)!.description!.priority
            }))
          }
        };
        yield checksum_line;
      }
      lastChecksums = checksumMap;
      lastWriteCheckpoint = writeCheckpoint;

      // Start syncing data for buckets up to the checkpoint. As soon as we have completed at least one priority and
      // at least 1000 operations, we also start listening for new checkpoints concurrently. When a new checkpoint comes
      // in while we're still busy syncing data for lower priorities, interrupt the current operation and start syncing
      // the new checkpoint.
      const abortCheckpointController = new AbortController();
      let syncedOperations = 0;

      const abortCheckpointSignal = AbortSignal.any([abortCheckpointController.signal, signal]);

      const bucketsByPriority = [...Map.groupBy(bucketsToFetch, (bucket) => bucket.priority).entries()];
      bucketsByPriority.sort((a, b) => a[0] - b[0]); // Sort from high to lower priorities
      const lowestPriority = bucketsByPriority.at(-1)?.[0];

      function maybeRaceForNewCheckpoint() {
        if (syncedOperations >= 1000 && nextCheckpointPromise === undefined) {
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
        const isLast = priority === lowestPriority;
        if (abortCheckpointSignal.aborted) {
          break;
        }

        yield* bucketDataInBatches({
          storage,
          checkpoint,
          bucketsToFetch: buckets,
          dataBuckets,
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
  storage: storage.SyncRulesBucketStorage;
  checkpoint: string;
  bucketsToFetch: BucketDescription[];
  /** Bucket data position, modified by the request.  */
  dataBuckets: Map<string, BucketSyncState>;
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
  while (!request.abort_batch.aborted && !isDone) {
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
    storage,
    checkpoint,
    bucketsToFetch,
    dataBuckets,
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
    const filteredBuckets = new Map(
      bucketsToFetch.map((bucket) => [bucket.bucket, dataBuckets.get(bucket.bucket)?.start_op_id!])
    );
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

      dataBuckets.get(r.bucket)!.start_op_id = r.next_after;

      // Check if syncing bucket data is supposed to stop before fetching more data
      // from storage.
      if (abort_batch.aborted) {
        return;
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
