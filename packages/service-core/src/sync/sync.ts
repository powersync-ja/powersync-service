import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import { BucketDescription, BucketPriority, RequestParameters, SqlSyncRules } from '@powersync/service-sync-rules';
import { Semaphore, withTimeout } from 'async-mutex';

import { AbortError } from 'ix/aborterror.js';

import * as auth from '../auth/auth-index.js';
import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import { logger } from '@powersync/lib-services-framework';
import { BucketChecksumState } from './BucketChecksumState.js';
import { mergeAsyncIterables } from './merge.js';
import { RequestTracker } from './RequestTracker.js';
import { acquireSemaphoreAbortable, settledPromise, tokenStream, TokenStreamOptions } from './util.js';

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

export type BucketSyncState = {
  description?: BucketDescription; // Undefined if the bucket has not yet been resolved by us.
  start_op_id: util.InternalOpId;
};

async function* streamResponseInner(
  bucketStorage: storage.SyncRulesBucketStorage,
  syncRules: SqlSyncRules,
  params: util.StreamingSyncRequest,
  syncParams: RequestParameters,
  tracker: RequestTracker,
  signal: AbortSignal
): AsyncGenerator<util.StreamingSyncLine | string | null> {
  const { raw_data, binary_data } = params;

  const checkpointUserId = util.checkpointUserId(syncParams.token_parameters.user_id as string, params.client_id);

  const checksumState = new BucketChecksumState({
    bucketStorage,
    syncRules,
    syncParams,
    initialBucketPositions: params.buckets?.map((bucket) => ({
      name: bucket.name,
      after: BigInt(bucket.after)
    }))
  });
  const stream = bucketStorage.watchWriteCheckpoint({
    user_id: checkpointUserId,
    signal
  });
  const newCheckpoints = stream[Symbol.asyncIterator]();

  try {
    let nextCheckpointPromise:
      | Promise<PromiseSettledResult<IteratorResult<storage.StorageCheckpointUpdate>>>
      | undefined;

    do {
      if (!nextCheckpointPromise) {
        // Wrap in a settledPromise, so that abort errors after the parent stopped iterating
        // does not result in uncaught errors.
        nextCheckpointPromise = settledPromise(newCheckpoints.next());
      }
      const next = await nextCheckpointPromise;
      nextCheckpointPromise = undefined;
      if (next.status == 'rejected') {
        throw next.reason;
      }
      if (next.value.done) {
        break;
      }
      const line = await checksumState.buildNextCheckpointLine(next.value.value);
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
      let syncedOperations = 0;

      const abortCheckpointSignal = AbortSignal.any([abortCheckpointController.signal, signal]);

      const bucketsByPriority = [...Map.groupBy(bucketsToFetch, (bucket) => bucket.priority).entries()];
      bucketsByPriority.sort((a, b) => a[0] - b[0]); // Sort from high to lower priorities
      const lowestPriority = bucketsByPriority.at(-1)?.[0];

      // Ensure that we have at least one priority batch: After sending the checkpoint line, clients expect to
      // receive a sync complete message after the synchronization is done (which happens in the last
      // bucketDataInBatches iteration). Without any batch, the line is missing and clients might not complete their
      // sync properly.
      const priorityBatches: [BucketPriority | null, BucketDescription[]][] = bucketsByPriority;
      if (priorityBatches.length == 0) {
        priorityBatches.push([null, []]);
      }

      function maybeRaceForNewCheckpoint() {
        if (syncedOperations >= 1000 && nextCheckpointPromise === undefined) {
          nextCheckpointPromise = (async () => {
            const next = await settledPromise(newCheckpoints.next());
            if (next.status == 'rejected') {
              abortCheckpointController.abort();
            } else if (!next.value.done) {
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
      for (const [priority, buckets] of priorityBatches) {
        const isLast = priority === lowestPriority;
        if (abortCheckpointSignal.aborted) {
          break;
        }

        yield* bucketDataInBatches({
          bucketStorage: bucketStorage,
          checkpoint: next.value.value.base.checkpoint,
          bucketsToFetch: buckets,
          checksumState,
          raw_data,
          binary_data,
          onRowsSent: markOperationsSent,
          abort_connection: signal,
          abort_batch: abortCheckpointSignal,
          user_id: syncParams.user_id,
          // Passing null here will emit a full sync complete message at the end. If we pass a priority, we'll emit a partial
          // sync complete message instead.
          forPriority: !isLast ? priority : null
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
  bucketStorage: storage.SyncRulesBucketStorage;
  checkpoint: util.InternalOpId;
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
  forPriority: BucketPriority | null;
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
    const filteredBuckets = checksumState.getFilteredBucketPositions(bucketsToFetch);
    const dataBatches = storage.getBucketDataBatch(checkpoint, filteredBuckets);

    let has_more = false;

    for await (let { batch: r, targetOp } of dataBatches) {
      // Abort in current batch if the connection is closed
      if (abort_connection.aborted) {
        return;
      }
      if (r.has_more) {
        has_more = true;
      }
      if (targetOp != null && targetOp > checkpoint) {
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

      checksumState.updateBucketPosition({ bucket: r.bucket, nextAfter: BigInt(r.next_after), hasMore: r.has_more });

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
        if (request.forPriority != null) {
          const line: util.StreamingSyncCheckpointPartiallyComplete = {
            partial_checkpoint_complete: {
              last_op_id: util.internalToExternalOpId(checkpoint),
              priority: request.forPriority
            }
          };
          yield { data: line, done: true };
        } else {
          const line: util.StreamingSyncCheckpointComplete = {
            checkpoint_complete: {
              last_op_id: util.internalToExternalOpId(checkpoint)
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
