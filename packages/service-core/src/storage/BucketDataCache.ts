import { JSONBig } from '@powersync/service-jsonbig';
import { LRUCache } from 'lru-cache/min';
import { OplogEntry } from '../util/protocol-types.js';
import { InternalOpId } from '../util/utils.js';
import { BucketDataBatchOptions, SyncBucketDataChunk } from './SyncRulesBucketStorage.js';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { first } from 'ix/iterable/first.js';

export interface FetchBucketDataArgs {
  bucket: string;
  /**
   * Starting point, inclusive.
   *
   * Set to 0n to start from the beginning.
   */
  start: InternalOpId;
  /**
   * Ending point, inclusive.
   */
  end: InternalOpId;
}

export type FetchData = (batch: FetchBucketDataArgs[]) => Promise<SyncBucketDataChunk[]>;

export interface BucketDataCacheOptions {
  /**
   * Upstream checksum implementation.
   *
   * This fetches a batch of either entire bucket checksums, or a partial range.
   */
  fetchData: FetchData;

  /**
   * Maximum number of cached data.
   */
  maxSize?: number;
}

const DEFAULT_MAX_SIZE_BYTES = 128 * 1024 * 1024;

/**
 * The time to live for cached data.
 *
 * The primary purposes of the cache is to de-duplicate in-progress requests for the same bucket and checkpoint,
 * when many clients are syncing the same buckets. For this, we don't need a long TTL.
 *
 * To a smaller extent the cache can also help to reduce data lookups for many initial syncs shortly after each
 * other, but this is not the primary use case.
 */
const TTL_MS = 60_000;

/**
 * Implement a LRU cache for bucket data requests. Each (bucket, start_end) request is cached separately,
 * while the lookups occur in batches.
 *
 * We use the LRUCache fetchMethod to deduplicate in-progress requests.
 *
 * Generally, we have a number of active clients, each needing to stream data.
 * We can process a number of requests in parallel, and cache the results.
 *
 * We need to optimize processing to:
 * 1. Avoid fetching the same bucket data multiple times.
 * 2. Prioritize using cached data first.
 * 3. Avoid starving certain clients.
 * 4. Avoid over-fetching data - each client can only process one response at a time.
 *
 * General strategy:
 * 1. Track each client with all its requests.
 * 2. For each client, track which requests are in progress.
 * 3. Have a number of workers that can process requests in parallel.
 * 4. On each worker iteration, pick up to 10 requests. Prioritize clients with the least number of active requests.
 * 5. Process these requests, cache the results, and notify each client that results are available.
 */
export class BucketDataCache {
  private cache: LRUCache<string, SyncBucketDataChunk>;
  private inProgress = new Set<string>();
  private sessions = new Set<ClientSession>();

  private fetchData: FetchData;

  constructor(options: BucketDataCacheOptions) {
    this.fetchData = options.fetchData;

    this.cache = new LRUCache<string, SyncBucketDataChunk>({
      max: 10_000,
      maxSize: options.maxSize ?? DEFAULT_MAX_SIZE_BYTES,

      sizeCalculation: (value: SyncBucketDataChunk) => {
        // FIXME: this is not correct
        return value.chunkData.data.length + 10;
      },

      // We use a TTL so that data can eventually be refreshed
      // after a compact. This only has effect if the bucket has
      // not been checked in the meantime.
      ttl: TTL_MS,
      ttlResolution: 1_000,
      allowStale: false
    });
  }

  private getRequestBatch(): FetchBucketDataArgs[] {
    let requests: FetchBucketDataArgs[] = [];

    let firstSession: ClientSession | null = null;

    let numBlocking = 0;
    let blockingMode = true;

    // 1. Check high-priority requests first.
    while ((numBlocking == 0 && requests.length < 100) || (numBlocking > 0 && requests.length < 20)) {
      // 1. Get the next client with pending requests.
      const next = first(this.sessions);
      if (next == null) {
        break;
      }
      if (firstSession == null) {
        firstSession = next;
      } else if (firstSession == next) {
        if (blockingMode) {
          blockingMode = false;
          firstSession = null;
        } else {
          break;
        }
      }
      // Move to end of the queue.
      this.sessions.delete(next);
      this.sessions.add(next);

      const request = blockingMode ? this.getBlockingRequest(next) : this.getNonBlockingRequest(next);
      console.log('request', request);
      if (request == null) {
        // No blocking request, continue to the next client.
        continue;
      }

      // 3. Add the request to the batch.
      const key = makeCacheKey(request);
      this.inProgress.add(key);
      requests.push(request);
    }

    return requests;
  }

  private async triggerWork() {
    // TODO: limit concurrentcy
    await this.workIteration();
  }

  private async workIteration() {
    const requests = this.getRequestBatch();
    console.log('workIteration', requests);
    if (requests.length === 0) {
      return;
    }

    const results = await this.fetchData(requests);
    console.log('workIteration results', results);
    for (const result of results) {
      const key = makeCacheKey({
        bucket: result.chunkData.bucket,
        start: BigInt(result.chunkData.after)
      });
      this.cache.set(
        makeCacheKey({
          bucket: result.chunkData.bucket,
          start: BigInt(result.chunkData.after)
        }),
        result
      );
      this.inProgress.delete(key);
    }

    for (let session of this.sessions) {
      // Notify all sessions that data is available.
      session.notify?.();
    }
  }

  /**
   * Given a set of bucket requests, return an iterator of chunks.
   *
   * The iterator may be cancelled at any time.
   *
   * The iterator may process buckets in any order - we may prioritize cached buckets.
   *
   * Each bucket request may be split into smaller chunks. These chunks will always be in order.
   *
   * @param checkpoint the checkpoint to fetch data for
   * @param dataBuckets current bucket states, used to determine the starting point for each bucket. This may be modified.
   */
  async *getBucketData(
    checkpoint: InternalOpId,
    dataBuckets: Map<string, InternalOpId>,
    options?: BucketDataBatchOptions
  ): AsyncIterableIterator<SyncBucketDataChunk> {
    const request = new ClientSession(checkpoint, dataBuckets, options);
    console.log('getBucketData', request);
    this.sessions.add(request);

    while (dataBuckets.size > 0) {
      console.log('getting next batch', request);
      const chunk = await this.getNextBatch(request);
      console.log('got chunk', chunk);
      if (chunk == null) {
        break;
      }

      if (chunk.chunkData.has_more) {
        request.bucketState.set(chunk.chunkData.bucket, BigInt(chunk.chunkData.next_after));
      } else {
        request.bucketState.delete(chunk.chunkData.bucket);
      }
      // TODO: sanitize chunk (remove data past the checkpoint)
      yield chunk;
    }
  }

  private getCachedData(session: ClientSession): SyncBucketDataChunk | undefined {
    for (let [bucket, start] of session.bucketState.entries()) {
      const key = makeCacheKey({ bucket, start });
      if (this.cache.has(key)) {
        return this.cache.get(key)!;
      }
    }
    return undefined;
  }

  private hasPendingData(session: ClientSession): boolean {
    for (let [bucket, start] of session.bucketState.entries()) {
      const key = makeCacheKey({ bucket, start });
      if (this.hasPendingRequest(key)) {
        return true;
      }
    }
    return false;
  }

  private hasPendingRequest(key: string): boolean {
    if (this.cache.has(key)) {
      return true;
    } else if (this.inProgress.has(key)) {
      return true;
    }
    return false;
  }

  private getBlockingRequest(session: ClientSession): FetchBucketDataArgs | undefined {
    if (this.hasPendingData(session)) {
      // Not blocking
      return undefined;
    }
    console.log('state', session.bucketState);

    for (let [bucket, start] of session.bucketState.entries()) {
      const key = makeCacheKey({ bucket, start });
      if (this.hasPendingRequest(key)) {
        // This request is already in progress, so we can't block on it.
        continue;
      }
      return { bucket, start, end: session.checkpoint };
    }
  }

  private getNonBlockingRequest(session: ClientSession): FetchBucketDataArgs | undefined {
    for (let [bucket, start] of session.bucketState.entries()) {
      const key = makeCacheKey({ bucket, start });
      if (this.hasPendingRequest(key)) {
        // This request is already in progress, so we can't block on it.
        continue;
      }
      return { bucket, start, end: session.checkpoint };
    }
  }

  private async getNextBatch(session: ClientSession): Promise<SyncBucketDataChunk | undefined> {
    while (session.bucketState.size > 0) {
      // 1. Check if we have cached data for this request.
      const cachedData = this.getCachedData(session);
      console.log('cached?', cachedData);
      if (cachedData) {
        return cachedData;
      }

      const promise = session.wait();
      console.log('triggerWork');

      await this.triggerWork();
      console.log('waiting for promise');
      await promise;
      console.log('got promise');
      // Now it should be in the cache, so check again

      // TODO: Implement timeout
    }
  }
}

function makeCacheKey(request: Pick<FetchBucketDataArgs, 'bucket' | 'start'>) {
  return JSON.stringify({
    b: request.bucket,
    s: request.start.toString()
  });
}

class ClientSession {
  checkpoint: InternalOpId;
  bucketState: Map<string, InternalOpId>;

  public notify: (() => void) | undefined;

  constructor(
    checkpoint: InternalOpId,
    dataBuckets: Map<string, InternalOpId>,
    private options?: BucketDataBatchOptions
  ) {
    this.checkpoint = checkpoint;
    this.bucketState = new Map(dataBuckets);
  }

  wait() {
    return new Promise<void>((resolve) => {
      this.notify = () => {
        resolve();
      };
    });
  }
}
