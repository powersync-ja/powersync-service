import { BucketChecksum, OpId } from '@/util/protocol-types.js';
import { addBucketChecksums } from '@/util/utils.js';
import { LRUCache } from 'lru-cache/min';

interface CheckpointEntry {
  refs: Set<number>;
  cache: LRUCache<string, BucketChecksum, ChecksumFetchContext>;
}

interface ChecksumFetchContext {
  fetch(bucket: string): Promise<BucketChecksum>;
}

export interface FetchPartialBucketChecksum {
  bucket: string;
  start?: OpId;
  end: OpId;
}

export type FetchChecksums = (batch: FetchPartialBucketChecksum[]) => Promise<Map<string, BucketChecksum>>;

export class ChecksumCache {
  private nextRefId = 1;
  private checkpoints = new Map<OpId, CheckpointEntry>();

  constructor(private fetchChecksums: FetchChecksums) {}

  async lock(checkpoint: OpId) {
    const ref = this.nextRefId++;

    const existing = this.checkpoints.get(checkpoint);
    if (existing != null) {
      existing.refs.add(ref);
    } else {
      const entry: CheckpointEntry = {
        refs: new Set([ref]),
        cache: new LRUCache({
          maxSize: 10_000,
          fetchMethod: async (bucket, staleValue, options) => {
            return options.context.fetch(bucket);
          }
        })
      };
      this.checkpoints.set(checkpoint, entry);
    }

    return () => {
      const entry = this.checkpoints.get(checkpoint);
      if (entry == null) {
        return;
      }
      entry.refs.delete(ref);
      if (entry.refs.size == 0) {
        this.checkpoints.delete(checkpoint);
      }
    };
  }

  async getChecksums(checkpoint: OpId, buckets: string[]) {
    let toFetch = new Set<string>();
    let fetchResults = new Map<string, BucketChecksum>();
    let resolveFetch!: () => void;
    let rejectFetch!: (err: any) => void;
    let fetchPromise = new Promise<void>((resolve, reject) => {
      resolveFetch = resolve;
      rejectFetch = reject;
    });

    let entry = this.checkpoints.get(checkpoint);
    if (entry == null) {
      // TODO: throw new Error(`No checkpoint cache for ${checkpoint}`);
      // Temporary: auto-create cache
      entry = {
        refs: new Set([]),
        cache: new LRUCache({
          maxSize: 10_000,
          fetchMethod: async (bucket, staleValue, options) => {
            return options.context.fetch(bucket);
          }
        })
      };
      this.checkpoints.set(checkpoint, entry);
    }

    let finalResults: BucketChecksum[] = [];

    const context: ChecksumFetchContext = {
      async fetch(bucket) {
        if (!toFetch.has(bucket)) {
          throw new Error(`Expected to fetch ${bucket}`);
        }
        await fetchPromise;
        const checksum = fetchResults.get(bucket);
        if (checksum == null) {
          throw new Error(`Failed to fetch checksum for bucket ${bucket}`);
        }
        return checksum;
      }
    };

    let promises: Promise<void>[] = [];

    try {
      for (let bucket of buckets) {
        let status: LRUCache.Status<BucketChecksum> = {};
        const p = entry.cache.fetch(bucket, { context: context, status: status }).then((checksums) => {
          if (checksums == null) {
            throw new Error(`Failed to get checksums for ${bucket}`);
          }
          finalResults.push(checksums);
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
        const checkpoints = [...this.checkpoints.keys()]
          .filter((other) => BigInt(other) < BigInt(checkpoint))
          .sort((a, b) => {
            if (a == b) {
              return 0;
            } else if (BigInt(a) < BigInt(b)) {
              return 1;
            } else {
              return -1;
            }
          });

        let bucketRequests: FetchPartialBucketChecksum[] = [];
        let add = new Map<string, BucketChecksum>();

        for (let bucket of toFetch) {
          let bucketRequest: FetchPartialBucketChecksum | null = null;
          for (let cp of checkpoints) {
            const entry = this.checkpoints.get(cp);
            if (entry == null) {
              throw new Error(`Cannot find cached checkpoint ${cp}`);
            }

            const cached = entry.cache.get(bucket);
            if (cached != null) {
              bucketRequest = {
                bucket,
                start: cp,
                end: checkpoint
              };
              add.set(bucket, cached);
              break;
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
            throw new Error(`toAdd null for ${bucket}`);
          }
          const added = addBucketChecksums(toAdd, result ?? null);
          fetchResults.set(bucket, added);
        }
        resolveFetch();
      }
    } catch (e) {
      rejectFetch(e);
      throw e;
    }

    await Promise.all(promises);
    if (finalResults.length != buckets.length) {
      throw new Error(`Bucket results mismatch: ${finalResults.length} != ${buckets.length}`);
    }
    return finalResults;
  }
}
