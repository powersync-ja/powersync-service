import { OrderedSet } from '@js-sdsl/ordered-set';
import { LRUCache } from 'lru-cache/min';
import { BucketChecksum } from '../util/protocol-types.js';
import { addBucketChecksums, ChecksumMap, InternalOpId, PartialChecksum } from '../util/utils.js';

interface ChecksumFetchContext {
  fetch(bucket: string): Promise<BucketChecksum>;
  checkpoint: InternalOpId;
}

export interface FetchPartialBucketChecksum {
  bucket: string;
  start?: InternalOpId;
  end: InternalOpId;
}

export type PartialOrFullChecksum = PartialChecksum | BucketChecksum;
export type PartialChecksumMap = Map<string, PartialOrFullChecksum>;

export type FetchChecksums = (batch: FetchPartialBucketChecksum[]) => Promise<PartialChecksumMap>;

export interface ChecksumCacheOptions {
  /**
   * Upstream checksum implementation.
   *
   * This fetches a batch of either entire bucket checksums, or a partial range.
   */
  fetchChecksums: FetchChecksums;

  /**
   * Maximum number of cached checksums.
   */
  maxSize?: number;
}

// Approximately 5MB of memory, if we assume 50 bytes per entry
const DEFAULT_MAX_SIZE = 100_000;

const TTL_MS = 3_600_000;

/**
 * Implement a LRU cache for checksum requests. Each (bucket, checkpoint) request is cached separately,
 * while the lookups occur in batches.
 *
 * For each bucket, we keep a separate OrderedSet of cached checkpoints.
 * This allows us to do incrementally update checksums by using the last cached checksum for the same bucket.
 *
 * We use the LRUCache fetchMethod to deduplicate in-progress requests.
 */
export class ChecksumCache {
  /**
   * The primary checksum cache, with key of `${checkpoint}/${bucket}`.
   */
  private cache: LRUCache<string, BucketChecksum, ChecksumFetchContext>;
  /**
   * For each bucket, an ordered set of cached checkpoints.
   */
  private bucketCheckpoints = new Map<string, OrderedSet<bigint>>();

  private fetchChecksums: FetchChecksums;

  constructor(options: ChecksumCacheOptions) {
    this.fetchChecksums = options.fetchChecksums;

    this.cache = new LRUCache<string, BucketChecksum, ChecksumFetchContext>({
      max: options.maxSize ?? DEFAULT_MAX_SIZE,
      fetchMethod: async (cacheKey, _staleValue, options) => {
        // Called when this checksum hasn't been cached yet.
        // Pass the call back to the request, which implements batch fetching.
        const { bucket } = parseCacheKey(cacheKey);
        const result = await options.context.fetch(bucket);

        // Add to the set of cached checkpoints for the bucket.
        let checkpointSet = this.bucketCheckpoints.get(bucket);
        if (checkpointSet == null) {
          checkpointSet = new OrderedSet();
          this.bucketCheckpoints.set(bucket, checkpointSet);
        }
        checkpointSet.insert(options.context.checkpoint);
        return result;
      },

      dispose: (value, key) => {
        // Remove from the set of cached checkpoints for the bucket
        const { checkpoint } = parseCacheKey(key);
        const checkpointSet = this.bucketCheckpoints.get(value.bucket);
        if (checkpointSet == null) {
          return;
        }
        checkpointSet.eraseElementByKey(checkpoint);
        if (checkpointSet.length == 0) {
          this.bucketCheckpoints.delete(value.bucket);
        }
      },

      noDisposeOnSet: true,

      // When we have more fetches than the cache size, complete the fetches instead
      // of failing with Error('evicted').
      ignoreFetchAbort: true,

      // We use a TTL so that counts can eventually be refreshed
      // after a compact. This only has effect if the bucket has
      // not been checked in the meantime.
      ttl: TTL_MS,
      ttlResolution: 1_000,
      allowStale: false
    });
  }

  clear() {
    this.cache.clear();
    this.bucketCheckpoints.clear();
  }

  async getChecksums(checkpoint: InternalOpId, buckets: string[]): Promise<BucketChecksum[]> {
    const checksums = await this.getChecksumMap(checkpoint, buckets);
    // Return results in the same order as the request
    return buckets.map((bucket) => checksums.get(bucket)!);
  }

  /**
   * Get bucket checksums for a checkpoint.
   *
   * Any checksums not found upstream are returned as zero checksums.
   *
   * @returns a Map with exactly one entry for each bucket requested
   */
  async getChecksumMap(checkpoint: InternalOpId, buckets: string[]): Promise<ChecksumMap> {
    // Buckets that don't have a cached checksum for this checkpoint yet
    let toFetch = new Set<string>();

    // Newly fetched results
    let fetchResults = new Map<string, BucketChecksum>();

    // Promise for the bactch new fetch requests
    let resolveFetch!: () => void;
    let rejectFetch!: (err: any) => void;
    let fetchPromise = new Promise<void>((resolve, reject) => {
      resolveFetch = resolve;
      rejectFetch = reject;
    });

    // Accumulated results - both from cached checksums, and fetched checksums
    let finalResults = new Map<string, BucketChecksum>();

    const context: ChecksumFetchContext = {
      async fetch(bucket) {
        await fetchPromise;
        if (!toFetch.has(bucket)) {
          // Should never happen
          throw new Error(`Expected to fetch ${bucket}`);
        }
        const checksum = fetchResults.get(bucket);
        if (checksum == null) {
          // Should never happen
          throw new Error(`Failed to fetch checksum for bucket ${bucket}`);
        }
        return checksum;
      },
      checkpoint: BigInt(checkpoint)
    };

    // One promise to await to ensure all fetch requests completed.
    let settledPromise: Promise<PromiseSettledResult<void>[]> | null = null;

    try {
      // Individual cache fetch promises
      let cacheFetchPromises: Promise<void>[] = [];

      for (let bucket of buckets) {
        const cacheKey = makeCacheKey(checkpoint, bucket);
        let status: LRUCache.Status<BucketChecksum> = {};
        const p = this.cache.fetch(cacheKey, { context: context, status: status }).then((checksums) => {
          if (checksums == null) {
            // Should never happen
            throw new Error(`Failed to get checksums for ${cacheKey}`);
          }
          finalResults.set(bucket, checksums);
        });
        cacheFetchPromises.push(p);
        if (status.fetch == 'hit' || status.fetch == 'inflight') {
          // The checksums is either cached already (hit), or another request is busy
          // fetching (inflight).
          // In either case, we don't need to fetch a new checksum.
        } else {
          // We need a new request for this checksum.
          toFetch.add(bucket);
        }
      }
      // We do this directly after creating the promises, otherwise
      // we could end up with weird uncaught rejection errors.
      settledPromise = Promise.allSettled(cacheFetchPromises);

      if (toFetch.size == 0) {
        // Nothing to fetch, but resolve in case
        resolveFetch();
      } else {
        let bucketRequests: FetchPartialBucketChecksum[] = [];
        // Partial checksum (previously cached) to add to the partial fetch
        let add = new Map<string, BucketChecksum>();

        for (let bucket of toFetch) {
          let bucketRequest: FetchPartialBucketChecksum | null = null;
          const checkpointSet = this.bucketCheckpoints.get(bucket);
          if (checkpointSet != null) {
            // Find smaller checkpoints, sorted in descending order
            let iter = checkpointSet.reverseUpperBound(context.checkpoint);
            const begin = checkpointSet.begin();
            while (iter.isAccessible()) {
              const cp = iter.pointer;
              const cacheKey = makeCacheKey(cp, bucket);
              // peek to avoid refreshing the key
              const cached = this.cache.peek(cacheKey);
              // As long as dispose() works correctly, the checkpointset should
              // match up with the cache, and `cached` should also have a value here.
              // However, we handle caces where it's not present either way.
              // Test by disabling the `dispose()` callback.
              if (cached != null) {
                // Partial checksum found - make a partial checksum request
                bucketRequest = {
                  bucket,
                  start: cp,
                  end: checkpoint
                };
                add.set(bucket, cached);
                break;
              }

              if (iter.equals(begin)) {
                // Cannot iterate further
                break;
              }
              // Iterate backwards
              iter = iter.pre();
            }
          }

          if (bucketRequest == null) {
            // No partial checksum found - make a new full checksum request
            bucketRequest = {
              bucket,
              end: checkpoint
            };
            add.set(bucket, {
              bucket,
              checksum: 0,
              count: 0
            });
          }
          bucketRequests.push(bucketRequest);
        }

        // Fetch partial checksums from upstream
        const results = await this.fetchChecksums(bucketRequests);

        for (let bucket of toFetch) {
          const result = results.get(bucket);
          const toAdd = add.get(bucket);
          if (toAdd == null) {
            // Should never happen
            throw new Error(`toAdd null for ${bucket}`);
          }
          // Compute the full checksum from the two partials.
          // No results returned are treated the same as a zero result.
          const added = addBucketChecksums(toAdd, result ?? null);
          fetchResults.set(bucket, added);
        }

        // fetchResults is fully populated, so we resolve the Promise
        resolveFetch();
      }
    } catch (e) {
      // Failure when fetching checksums - reject the Promise.
      // This will reject all individual cache fetch requests, and each will be retried
      // on the next request.
      rejectFetch(e);

      // Wait for the above rejection to propagate, otherwise we end up with "uncaught" errors.
      // This promise never throws.
      await settledPromise;

      throw e;
    }

    // Wait for all cache fetch reqeusts to complete
    const settledResults = (await settledPromise) ?? [];
    // Check if any of them failed
    for (let result of settledResults) {
      if (result.status == 'rejected') {
        throw result.reason;
      }
    }

    if (finalResults.size != buckets.length) {
      // Should not happen
      throw new Error(`Bucket results mismatch: ${finalResults.size} != ${buckets.length}`);
    }
    return finalResults;
  }
}

function makeCacheKey(checkpoint: InternalOpId | string, bucket: string) {
  return `${checkpoint}/${bucket}`;
}

function parseCacheKey(key: string) {
  const index = key.indexOf('/');
  return { checkpoint: BigInt(key.substring(0, index)), bucket: key.substring(index + 1) };
}
