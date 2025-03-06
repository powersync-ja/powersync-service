import { ChecksumCache, FetchChecksums, FetchPartialBucketChecksum, PartialChecksum } from '@/storage/ChecksumCache.js';
import { addChecksums, InternalOpId } from '@/util/util-index.js';
import * as crypto from 'node:crypto';
import { describe, expect, it } from 'vitest';

/**
 * Create a deterministic BucketChecksum based on the bucket name and checkpoint for testing purposes.
 */
function testHash(bucket: string, checkpoint: InternalOpId) {
  const key = `${checkpoint}/${bucket}`;
  const hash = crypto.createHash('sha256').update(key).digest().readInt32LE(0);
  return hash;
}

function testPartialHash(request: FetchPartialBucketChecksum): PartialChecksum {
  if (request.start) {
    const a = testHash(request.bucket, request.start);
    const b = testHash(request.bucket, request.end);
    return {
      bucket: request.bucket,
      partialCount: Number(request.end) - Number(request.start),
      partialChecksum: addChecksums(b, -a),
      isFullChecksum: false
    };
  } else {
    return {
      bucket: request.bucket,
      partialChecksum: testHash(request.bucket, request.end),
      partialCount: Number(request.end),
      isFullChecksum: true
    };
  }
}

const TEST_123 = {
  bucket: 'test',
  count: 123,
  checksum: 1104081737
};

const TEST_1234 = {
  bucket: 'test',
  count: 1234,
  checksum: -1593864957
};

const TEST2_123 = {
  bucket: 'test2',
  count: 123,
  checksum: 1741377449
};

const TEST3_123 = {
  bucket: 'test3',
  count: 123,
  checksum: -2085080402
};

function fetchTestChecksums(batch: FetchPartialBucketChecksum[]) {
  return new Map(
    batch.map((v) => {
      return [v.bucket, testPartialHash(v)];
    })
  );
}

describe('checksum cache', function () {
  const factory = (fetch: FetchChecksums) => {
    return new ChecksumCache({ fetchChecksums: fetch });
  };

  it('should handle a sequential lookups (a)', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch);
    });

    expect(await cache.getChecksums(123n, ['test'])).toEqual([TEST_123]);

    expect(await cache.getChecksums(1234n, ['test'])).toEqual([TEST_1234]);

    expect(await cache.getChecksums(123n, ['test2'])).toEqual([TEST2_123]);

    expect(lookups).toEqual([
      [{ bucket: 'test', end: 123n }],
      // This should use the previous lookup
      [{ bucket: 'test', start: 123n, end: 1234n }],
      [{ bucket: 'test2', end: 123n }]
    ]);
  });

  it('should handle a sequential lookups (b)', async function () {
    // Reverse order of the above
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch);
    });

    expect(await cache.getChecksums(123n, ['test2'])).toEqual([TEST2_123]);

    expect(await cache.getChecksums(1234n, ['test'])).toEqual([TEST_1234]);

    expect(await cache.getChecksums(123n, ['test'])).toEqual([TEST_123]);

    expect(lookups).toEqual([
      // With this order, there is no option for a partial lookup
      [{ bucket: 'test2', end: 123n }],
      [{ bucket: 'test', end: 1234n }],
      [{ bucket: 'test', end: 123n }]
    ]);
  });

  it('should handle a concurrent lookups (a)', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch);
    });

    const p1 = cache.getChecksums(123n, ['test']);
    const p2 = cache.getChecksums(1234n, ['test']);
    const p3 = cache.getChecksums(123n, ['test2']);

    expect(await p1).toEqual([TEST_123]);
    expect(await p2).toEqual([TEST_1234]);
    expect(await p3).toEqual([TEST2_123]);

    // Concurrent requests, so we can't do a partial lookup for 123 -> 1234
    expect(lookups).toEqual([
      [{ bucket: 'test', end: 123n }],
      [{ bucket: 'test', end: 1234n }],
      [{ bucket: 'test2', end: 123n }]
    ]);
  });

  it('should handle a concurrent lookups (b)', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch);
    });

    const p1 = cache.getChecksums(123n, ['test']);
    const p2 = cache.getChecksums(123n, ['test']);

    expect(await p1).toEqual([TEST_123]);

    expect(await p2).toEqual([TEST_123]);

    // The lookup should be deduplicated, even though it's in progress
    expect(lookups).toEqual([[{ bucket: 'test', end: 123n }]]);
  });

  it('should handle serial + concurrent lookups', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch);
    });

    expect(await cache.getChecksums(123n, ['test'])).toEqual([TEST_123]);

    const p2 = cache.getChecksums(1234n, ['test']);
    const p3 = cache.getChecksums(1234n, ['test']);

    expect(await p2).toEqual([TEST_1234]);
    expect(await p3).toEqual([TEST_1234]);

    expect(lookups).toEqual([
      [{ bucket: 'test', end: 123n }],
      // This lookup is deduplicated
      [{ bucket: 'test', start: 123n, end: 1234n }]
    ]);
  });

  it('should handle multiple buckets', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch);
    });

    expect(await cache.getChecksums(123n, ['test', 'test2'])).toEqual([TEST_123, TEST2_123]);

    expect(lookups).toEqual([
      [
        // Both lookups in the same request
        { bucket: 'test', end: 123n },
        { bucket: 'test2', end: 123n }
      ]
    ]);
  });

  it('should handle multiple buckets with partial caching (a)', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch);
    });

    expect(await cache.getChecksums(123n, ['test'])).toEqual([TEST_123]);
    expect(await cache.getChecksums(123n, ['test', 'test2'])).toEqual([TEST_123, TEST2_123]);

    expect(lookups).toEqual([
      // Request 1
      [{ bucket: 'test', end: 123n }],
      // Request 2
      [{ bucket: 'test2', end: 123n }]
    ]);
  });

  it('should handle multiple buckets with partial caching (b)', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch);
    });

    const a = cache.getChecksums(123n, ['test', 'test2']);
    const b = cache.getChecksums(123n, ['test2', 'test3']);

    expect(await a).toEqual([TEST_123, TEST2_123]);
    expect(await b).toEqual([TEST2_123, TEST3_123]);

    expect(lookups).toEqual([
      // Request A
      [
        { bucket: 'test', end: 123n },
        { bucket: 'test2', end: 123n }
      ],
      // Request B (re-uses the checksum for test2 from request a)
      [{ bucket: 'test3', end: 123n }]
    ]);
  });

  it('should handle out-of-order requests', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch);
    });

    expect(await cache.getChecksums(123n, ['test'])).toEqual([TEST_123]);

    expect(await cache.getChecksums(125n, ['test'])).toEqual([
      {
        bucket: 'test',
        checksum: -1865121912,
        count: 125
      }
    ]);

    expect(await cache.getChecksums(124n, ['test'])).toEqual([
      {
        bucket: 'test',
        checksum: 1887460431,
        count: 124
      }
    ]);
    expect(lookups).toEqual([
      [{ bucket: 'test', end: 123n }],
      [{ bucket: 'test', start: 123n, end: 125n }],
      [{ bucket: 'test', start: 123n, end: 124n }]
    ]);
  });

  it('should handle errors', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const TEST_ERROR = new Error('Simulated error');
    const cache = factory(async (batch) => {
      lookups.push(batch);
      if (lookups.length == 1) {
        throw new Error('Simulated error');
      }
      return fetchTestChecksums(batch);
    });

    const a = cache.getChecksums(123n, ['test', 'test2']);
    const b = cache.getChecksums(123n, ['test2', 'test3']);

    await expect(a).rejects.toEqual(TEST_ERROR);
    await expect(b).rejects.toEqual(TEST_ERROR);

    const a2 = cache.getChecksums(123n, ['test', 'test2']);
    const b2 = cache.getChecksums(123n, ['test2', 'test3']);

    expect(await a2).toEqual([TEST_123, TEST2_123]);
    expect(await b2).toEqual([TEST2_123, TEST3_123]);

    expect(lookups).toEqual([
      // Request A (fails)
      [
        { bucket: 'test', end: 123n },
        { bucket: 'test2', end: 123n }
      ],
      // Request B (re-uses the checksum for test2 from request a)
      // Even thought the full request fails, this batch succeeds
      [{ bucket: 'test3', end: 123n }],
      // Retry request A
      [
        { bucket: 'test', end: 123n },
        { bucket: 'test2', end: 123n }
      ]
    ]);
  });

  it('should handle missing checksums (a)', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch.filter((b) => b.bucket != 'test'));
    });

    expect(await cache.getChecksums(123n, ['test'])).toEqual([{ bucket: 'test', checksum: 0, count: 0 }]);
    expect(await cache.getChecksums(123n, ['test', 'test2'])).toEqual([
      { bucket: 'test', checksum: 0, count: 0 },
      TEST2_123
    ]);
  });

  it('should handle missing checksums (b)', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      return fetchTestChecksums(batch.filter((b) => b.bucket != 'test' || b.end != 123n));
    });

    expect(await cache.getChecksums(123n, ['test'])).toEqual([{ bucket: 'test', checksum: 0, count: 0 }]);
    expect(await cache.getChecksums(1234n, ['test'])).toEqual([
      {
        bucket: 'test',
        checksum: 1597020602,
        count: 1111
      }
    ]);

    expect(lookups).toEqual([[{ bucket: 'test', end: 123n }], [{ bucket: 'test', start: 123n, end: 1234n }]]);
  });

  it('should use maxSize', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = new ChecksumCache({
      fetchChecksums: async (batch) => {
        lookups.push(batch);
        return fetchTestChecksums(batch);
      },
      maxSize: 2
    });

    expect(await cache.getChecksums(123n, ['test'])).toEqual([TEST_123]);
    expect(await cache.getChecksums(124n, ['test'])).toEqual([
      {
        bucket: 'test',
        checksum: 1887460431,
        count: 124
      }
    ]);

    expect(await cache.getChecksums(125n, ['test'])).toEqual([
      {
        bucket: 'test',
        checksum: -1865121912,
        count: 125
      }
    ]);
    expect(await cache.getChecksums(126n, ['test'])).toEqual([
      {
        bucket: 'test',
        checksum: -1720007310,
        count: 126
      }
    ]);
    expect(await cache.getChecksums(124n, ['test'])).toEqual([
      {
        bucket: 'test',
        checksum: 1887460431,
        count: 124
      }
    ]);
    expect(await cache.getChecksums(123n, ['test'])).toEqual([TEST_123]);

    expect(lookups).toEqual([
      [{ bucket: 'test', end: 123n }],
      [{ bucket: 'test', start: 123n, end: 124n }],
      [{ bucket: 'test', start: 124n, end: 125n }],
      [{ bucket: 'test', start: 125n, end: 126n }],
      [{ bucket: 'test', end: 124n }],
      [{ bucket: 'test', end: 123n }]
    ]);
  });

  it('should handle concurrent requests greater than cache size', async function () {
    // This will not be cached efficiently, but we test that we don't get errors at least.
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = new ChecksumCache({
      fetchChecksums: async (batch) => {
        lookups.push(batch);
        return fetchTestChecksums(batch);
      },
      maxSize: 2
    });

    const p3 = cache.getChecksums(123n, ['test3']);
    const p4 = cache.getChecksums(123n, ['test4']);
    const p1 = cache.getChecksums(123n, ['test']);
    const p2 = cache.getChecksums(123n, ['test2']);

    expect(await p1).toEqual([TEST_123]);
    expect(await p2).toEqual([TEST2_123]);
    expect(await p3).toEqual([TEST3_123]);
    expect(await p4).toEqual([
      {
        bucket: 'test4',
        checksum: 1004797863,
        count: 123
      }
    ]);

    // The lookup should be deduplicated, even though it's in progress
    expect(lookups).toEqual([
      [{ bucket: 'test3', end: 123n }],
      [{ bucket: 'test4', end: 123n }],
      [{ bucket: 'test', end: 123n }],
      [{ bucket: 'test2', end: 123n }]
    ]);
  });

  it('should handle CLEAR/isFullChecksum checksums', async function () {
    let lookups: FetchPartialBucketChecksum[][] = [];
    const cache = factory(async (batch) => {
      lookups.push(batch);
      // This forces a `isFullChecksum: true` result
      delete batch[0].start;
      return fetchTestChecksums(batch);
    });

    expect(await cache.getChecksums(123n, ['test'])).toEqual([TEST_123]);
    expect(await cache.getChecksums(1234n, ['test'])).toEqual([TEST_1234]);
  });
});
