import { BucketChecksum, ChecksumMap, OpId } from '@/util/protocol-types.js';
import { addBucketChecksums } from '@/util/utils.js';
import { LRUCache } from 'lru-cache/min';
import { OrderedSet } from '@js-sdsl/ordered-set';
import { ChecksumCacheInterface } from './ChecksumCache.js';

interface ChecksumFetchContext {
  fetch(bucket: string): Promise<BucketChecksum>;
  checkpoint: bigint;
}

export interface FetchPartialBucketChecksum {
  bucket: string;
  start?: OpId;
  end: OpId;
}

export type FetchChecksums = (batch: FetchPartialBucketChecksum[]) => Promise<ChecksumMap>;

export interface ChecksumCacheOptions {
  fetchChecksums: FetchChecksums;
  maxSize?: number;
}

// Approximately 5MB of memory, if we assume 50 bytes per entry
const DEFAULT_MAX_SIZE = 100_000;

/**
 * Implement a LRU cache for checksum requests. Each (bucket, checkpoint) request is cached separately,
 * while the lookups occur in batches.
 *
 * For each bucket, we keep a separate OrderedSet of cached checkpoints.
 * This allows us to do incrementally update checksums by using the last cached checksum for the same bucket.
 *
 * We use the LRUCache fetchMethod to deduplicate in-progress requests.
 */
export class ChecksumCache implements ChecksumCacheInterface {
  /**
   * The primary checksum cache, with key of `${checkpoint}/${bucket}`.
   */
  private cache: LRUCache<string, BucketChecksum, ChecksumFetchContext>;

  private bucketCheckpoints = new Map<string, OrderedSet<bigint>>();
  private fetchChecksums: FetchChecksums;

  constructor(options: ChecksumCacheOptions) {
    this.fetchChecksums = options.fetchChecksums;

    this.cache = new LRUCache<string, BucketChecksum, ChecksumFetchContext>({
      max: options.maxSize ?? DEFAULT_MAX_SIZE,
      fetchMethod: async (cacheKey, _staleValue, options) => {
        const { bucket } = parseCacheKey(cacheKey);
        const result = await options.context.fetch(bucket);

        let checkpointSet = this.bucketCheckpoints.get(bucket);
        if (checkpointSet == null) {
          checkpointSet = new OrderedSet();
          this.bucketCheckpoints.set(bucket, checkpointSet);
        }
        checkpointSet.insert(options.context.checkpoint);
        return result;
      },

      dispose: (value, key) => {
        const { checkpointString } = parseCacheKey(key);
        const checkpoint = BigInt(checkpointString);
        const checkpointSet = this.bucketCheckpoints.get(value.bucket);
        if (checkpointSet == null) {
          return;
        }
        checkpointSet.eraseElementByKey(checkpoint);
        if (checkpointSet.length == 0) {
          this.bucketCheckpoints.delete(value.bucket);
        }
      },

      noDisposeOnSet: true
    });
  }

  async getChecksums(checkpoint: OpId, buckets: string[]): Promise<BucketChecksum[]> {
    const checksums = await this.getChecksumMap(checkpoint, buckets);
    // Return results in the same order as the request
    return buckets.map((bucket) => checksums.get(bucket)!);
  }

  async getChecksumMap(checkpoint: OpId, buckets: string[]): Promise<Map<string, BucketChecksum>> {
    let toFetch = new Set<string>();
    let fetchResults = new Map<string, BucketChecksum>();
    let resolveFetch!: () => void;
    let rejectFetch!: (err: any) => void;
    let fetchPromise = new Promise<void>((resolve, reject) => {
      resolveFetch = resolve;
      rejectFetch = reject;
    });

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

    let promises: Promise<void>[] = [];

    try {
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
        promises.push(p);
        if (status.fetch == 'hit' || status.fetch == 'inflight') {
          // No need to fetch now
        } else {
          toFetch.add(bucket);
        }
      }

      if (toFetch.size == 0) {
        // Nothing to fetch, but resolve in case
        resolveFetch();
      } else {
        // Find smaller checkpoints, sorted in descending order

        let bucketRequests: FetchPartialBucketChecksum[] = [];
        let add = new Map<string, BucketChecksum>();

        for (let bucket of toFetch) {
          let bucketRequest: FetchPartialBucketChecksum | null = null;
          const checkpointSet = this.bucketCheckpoints.get(bucket);
          if (checkpointSet != null) {
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
                bucketRequest = {
                  bucket,
                  start: cp.toString(),
                  end: checkpoint
                };
                add.set(bucket, cached);
                break;
              }

              if (iter.equals(begin)) {
                break;
              }
              iter = iter.pre();
            }
          }

          if (bucketRequest == null) {
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

        const results = await this.fetchChecksums(bucketRequests);
        for (let bucket of toFetch) {
          const result = results.get(bucket);
          const toAdd = add.get(bucket);
          if (toAdd == null) {
            // Should never happen
            throw new Error(`toAdd null for ${bucket}`);
          }
          const added = addBucketChecksums(toAdd, result ?? null);
          fetchResults.set(bucket, added);
        }
        resolveFetch();
      }
    } catch (e) {
      rejectFetch(e);

      // Wait for the above rejection to propagate, otherwise we end up with "uncaught" errors.
      await Promise.all(promises).catch((_e) => {});

      throw e;
    }

    await Promise.all(promises);
    if (finalResults.size != buckets.length) {
      throw new Error(`Bucket results mismatch: ${finalResults.size} != ${buckets.length}`);
    }
    return finalResults;
  }
}

function makeCacheKey(checkpoint: bigint | string, bucket: string) {
  return `${checkpoint}/${bucket}`;
}

function parseCacheKey(key: string) {
  const index = key.indexOf('/');
  return { checkpointString: key.substring(0, index), bucket: key.substring(index + 1) };
}
