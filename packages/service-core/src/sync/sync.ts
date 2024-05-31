import * as micro from '@journeyapps-platform/micro';
import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import { SyncParameters } from '@powersync/service-sync-rules';
import { Semaphore } from 'async-mutex';
import { AbortError } from 'ix/aborterror.js';

import * as auth from '@/auth/auth-index.js';
import * as storage from '@/storage/storage-index.js';
import * as util from '@/util/util-index.js';

import { mergeAsyncIterables } from './merge.js';
import { TokenStreamOptions, tokenStream } from './util.js';
import { Metrics } from '@/metrics/Metrics.js';

/**
 * Maximum number of connections actively fetching data.
 */
const MAX_ACTIVE_CONNECTIONS = 10;
const syncSemaphore = new Semaphore(MAX_ACTIVE_CONNECTIONS);

export interface SyncStreamParameters {
  storage: storage.BucketStorageFactory;
  params: util.StreamingSyncRequest;
  syncParams: SyncParameters;
  token: auth.JwtPayload;
  /**
   * If this signal is aborted, the stream response ends as soon as possible, without error.
   */
  signal?: AbortSignal;
  tokenStreamOptions?: Partial<TokenStreamOptions>;
}

export async function* streamResponse(
  options: SyncStreamParameters
): AsyncIterable<util.StreamingSyncLine | string | null> {
  const { storage, params, syncParams, token, tokenStreamOptions, signal } = options;
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
  const stream = streamResponseInner(storage, params, syncParams, controller.signal);
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
  syncParams: SyncParameters,
  signal: AbortSignal
): AsyncGenerator<util.StreamingSyncLine | string | null> {
  // Bucket state of bucket id -> op_id.
  // This starts with the state from the client. May contain buckets that the user do not have access to (anymore).
  let dataBuckets = new Map<string, string>();

  let last_checksums: util.BucketChecksum[] | null = null;
  let last_write_checkpoint: bigint | null = null;

  const { raw_data, binary_data } = params;

  if (params.buckets) {
    for (let { name, after: start } of params.buckets) {
      dataBuckets.set(name, start);
    }
  }

  const stream = storage.watchWriteCheckpoint(syncParams.token_parameters.user_id as string, signal);
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
      // TODO: Limit number of buckets even before we get to this point
      throw new Error(`Too many buckets: ${allBuckets.length}`);
    }

    let checksums: util.BucketChecksum[] | undefined = undefined;

    let dataBucketsNew = new Map<string, string>();
    for (let bucket of allBuckets) {
      dataBucketsNew.set(bucket, dataBuckets.get(bucket) ?? '0');
    }
    dataBuckets = dataBucketsNew;

    checksums = await storage.getChecksums(checkpoint, [...dataBuckets.keys()]);

    if (last_checksums) {
      const diff = util.checksumsDiff(last_checksums, checksums);

      if (
        last_write_checkpoint == writeCheckpoint &&
        diff.removed_buckets.length == 0 &&
        diff.updated_buckets.length == 0
      ) {
        // No changes - don't send anything to the client
        continue;
      }

      let message = `Updated checkpoint: ${checkpoint} | write: ${writeCheckpoint} | `;
      message += `buckets: ${allBuckets.length} | `;
      message += `updated: ${limitedBuckets(diff.updated_buckets, 20)} | `;
      message += `removed: ${limitedBuckets(diff.removed_buckets, 20)} | `;
      micro.logger.info(message);

      const checksum_line: util.StreamingSyncCheckpointDiff = {
        checkpoint_diff: {
          last_op_id: checkpoint,
          write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
          ...diff
        }
      };

      yield checksum_line;
    } else {
      let message = `New checkpoint: ${checkpoint} | write: ${writeCheckpoint} | `;
      message += `buckets: ${allBuckets.length} ${limitedBuckets(allBuckets, 20)}`;
      micro.logger.info(message);
      const checksum_line: util.StreamingSyncCheckpoint = {
        checkpoint: {
          last_op_id: checkpoint,
          write_checkpoint: writeCheckpoint ? String(writeCheckpoint) : undefined,
          buckets: checksums
        }
      };
      yield checksum_line;
    }

    last_checksums = checksums;
    last_write_checkpoint = writeCheckpoint;

    yield* bucketDataInBatches(storage, checkpoint, dataBuckets, raw_data, binary_data, signal);

    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

async function* bucketDataInBatches(
  storage: storage.SyncRulesBucketStorage,
  checkpoint: string,
  dataBuckets: Map<string, string>,
  raw_data: boolean | undefined,
  binary_data: boolean | undefined,
  signal: AbortSignal
) {
  let isDone = false;
  while (!signal.aborted && !isDone) {
    // The code below is functionally the same as this for-await loop below.
    // However, the for-await loop appears to have a memory leak, so we avoid it.
    // for await (const { done, data } of bucketDataBatch(storage, checkpoint, dataBuckets, raw_data, signal)) {
    //   yield data;
    //   if (done) {
    //     isDone = true;
    //   }
    //   break;
    // }
    const iter = bucketDataBatch(storage, checkpoint, dataBuckets, raw_data, binary_data, signal);
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

/**
 * Extracted as a separate internal function just to avoid memory leaks.
 */
async function* bucketDataBatch(
  storage: storage.SyncRulesBucketStorage,
  checkpoint: string,
  dataBuckets: Map<string, string>,
  raw_data: boolean | undefined,
  binary_data: boolean | undefined,
  signal: AbortSignal
) {
  const [_, release] = await syncSemaphore.acquire();
  try {
    const data = storage.getBucketDataBatch(checkpoint, dataBuckets);

    let has_more = false;

    for await (let r of data) {
      if (signal.aborted) {
        return;
      }
      if (r.has_more) {
        has_more = true;
      }
      if (r.data.length == 0) {
        continue;
      }
      micro.logger.debug(`Sending data for ${r.bucket}`);

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
      Metrics.getInstance().operations_synced_total.add(r.data.length);

      dataBuckets.set(r.bucket, r.next_after);
    }

    if (!has_more) {
      const line: util.StreamingSyncCheckpointComplete = {
        checkpoint_complete: {
          last_op_id: checkpoint
        }
      };
      yield { data: line, done: true };
    }
  } finally {
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
