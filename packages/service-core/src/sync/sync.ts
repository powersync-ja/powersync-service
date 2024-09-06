import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import { RequestParameters } from '@powersync/service-sync-rules';
import { Semaphore } from 'async-mutex';
import { AbortError } from 'ix/aborterror.js';

import * as auth from '../auth/auth-index.js';
import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import { logger } from '@powersync/lib-services-framework';
import { mergeAsyncIterables } from './merge.js';
import { RequestTracker } from './RequestTracker.js';
import { TokenStreamOptions, tokenStream } from './util.js';

/**
 * Maximum number of connections actively fetching data.
 */
const MAX_ACTIVE_CONNECTIONS = 10;
const syncSemaphore = new Semaphore(MAX_ACTIVE_CONNECTIONS);

export interface SyncStreamParameters {
  storage: storage.BucketStorageFactory;
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
  const { storage, params, syncParams, token, tokenStreamOptions, tracker, signal } = options;
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
  const stream = streamResponseInner(storage, params, syncParams, tracker, controller.signal);
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

async function* streamResponseInner(
  storage: storage.BucketStorageFactory,
  params: util.StreamingSyncRequest,
  syncParams: RequestParameters,
  tracker: RequestTracker,
  signal: AbortSignal
): AsyncGenerator<util.StreamingSyncLine | string | null> {
  // Bucket state of bucket id -> op_id.
  // This starts with the state from the client. May contain buckets that the user do not have access to (anymore).
  let dataBuckets = new Map<string, string>();

  let lastChecksums: util.ChecksumMap | null = null;
  let lastWriteCheckpoint: bigint | null = null;

  const { raw_data, binary_data } = params;

  if (params.buckets) {
    for (let { name, after: start } of params.buckets) {
      dataBuckets.set(name, start);
    }
  }

  const checkpointUserId = util.checkpointUserId(syncParams.token_parameters.user_id as string, params.client_id);
  const stream = storage.watchWriteCheckpoint(checkpointUserId, signal);
  for await (const next of stream) {
    const { base, writeCheckpoint } = next;
    const checkpoint = base.checkpoint;

    const storage = await base.getBucketStorage();
    if (storage == null) {
      // Sync rules deleted in the meantime - try again with the next checkpoint.
      continue;
    }
    const sync_rules = storage.sync_rules;

    const allBuckets = await sync_rules.queryBucketIds({
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

    let dataBucketsNew = new Map<string, string>();
    for (let bucket of allBuckets) {
      dataBucketsNew.set(bucket, dataBuckets.get(bucket) ?? '0');
    }
    dataBuckets = dataBucketsNew;

    const bucketList = [...dataBuckets.keys()];
    const checksumMap = await storage.getChecksums(checkpoint, bucketList);
    // Subset of buckets for which there may be new data in this batch.
    let bucketsToFetch: string[];

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
      bucketsToFetch = diff.updatedBuckets.map((c) => c.bucket);

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
          updated_buckets: diff.updatedBuckets
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
          buckets: [...checksumMap.values()]
        }
      };
      yield checksum_line;
    }
    lastChecksums = checksumMap;
    lastWriteCheckpoint = writeCheckpoint;

    // This incrementally updates dataBuckets with each individual bucket position.
    // At the end of this, we can be sure that all buckets have data up to the checkpoint.
    yield* bucketDataInBatches({
      storage,
      checkpoint,
      bucketsToFetch,
      dataBuckets,
      raw_data,
      binary_data,
      signal,
      tracker,
      user_id: syncParams.user_id
    });

    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

interface BucketDataRequest {
  storage: storage.SyncRulesBucketStorage;
  checkpoint: string;
  bucketsToFetch: string[];
  /** Bucket data position, modified by the request.  */
  dataBuckets: Map<string, string>;
  raw_data: boolean | undefined;
  binary_data: boolean | undefined;
  tracker: RequestTracker;
  signal: AbortSignal;
  user_id?: string;
}

async function* bucketDataInBatches(request: BucketDataRequest) {
  let isDone = false;
  while (!request.signal.aborted && !isDone) {
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
  const { storage, checkpoint, bucketsToFetch, dataBuckets, raw_data, binary_data, tracker, signal } = request;

  const checkpointOp = BigInt(checkpoint);
  let checkpointInvalidated = false;

  if (syncSemaphore.isLocked()) {
    logger.info('Sync concurrency limit reached, waiting for lock', { user_id: request.user_id });
  }
  const [value, release] = await syncSemaphore.acquire();
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
    const filteredBuckets = new Map(bucketsToFetch.map((bucket) => [bucket, dataBuckets.get(bucket)!]));
    const data = storage.getBucketDataBatch(checkpoint, filteredBuckets);

    let has_more = false;

    for await (let { batch: r, targetOp } of data) {
      if (signal.aborted) {
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
        // Data is a raw string - we can use the more efficient JSON.stringify.
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
      tracker.addOperationsSynced(r.data.length);

      dataBuckets.set(r.bucket, r.next_after);
    }

    if (!has_more) {
      if (checkpointInvalidated) {
        // Checkpoint invalidated by a CLEAR or MOVE op.
        // Don't send the checkpoint_complete line in this case.
        // More data should be available immediately for a new checkpoint.
        yield { data: null, done: true };
      } else {
        const line: util.StreamingSyncCheckpointComplete = {
          checkpoint_complete: {
            last_op_id: checkpoint
          }
        };
        yield { data: line, done: true };
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

function limitedBuckets(buckets: string[] | util.BucketChecksum[], limit: number) {
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
