export interface RollingBucketMaxOptions {
  bucketSizeMs?: number;
  windowSizeMs?: number;
}

interface Bucket {
  // Absolute bucket id derived from floor(timestamp / bucketSizeMs).
  id: number;
  // Maximum reported value seen within this bucket.
  max: number | undefined;
}

/**
 * Tracks a rolling max over a fixed number of time buckets.
 *
 * The window is bucket-aligned: with the default 30s window and 5s buckets,
 * the rolling max covers the current 5s bucket plus the previous 5 buckets.
 */
export class RollingBucketMax {
  private readonly bucketSizeMs: number;
  private readonly bucketCount: number;
  // Fixed-size ring buffer keyed by bucket id modulo bucketCount.
  private readonly buckets: Bucket[];

  constructor(options: RollingBucketMaxOptions = {}) {
    this.bucketSizeMs = options.bucketSizeMs ?? 5_000;
    const windowSizeMs = options.windowSizeMs ?? 30_000;

    if (!Number.isInteger(this.bucketSizeMs) || this.bucketSizeMs <= 0) {
      throw new Error('bucketSizeMs must be a positive integer.');
    }

    if (!Number.isInteger(windowSizeMs) || windowSizeMs <= 0) {
      throw new Error('windowSizeMs must be a positive integer.');
    }

    if (windowSizeMs % this.bucketSizeMs !== 0) {
      throw new Error('windowSizeMs must be an exact multiple of bucketSizeMs.');
    }

    this.bucketCount = windowSizeMs / this.bucketSizeMs;
    this.buckets = Array.from({ length: this.bucketCount }, () => ({
      id: Number.NaN,
      max: undefined
    }));
  }

  /**
   * Reports a new observed value into the bucket for the provided timestamp.
   */
  report(value: number | undefined, timestampMs = Date.now()): void {
    if (value == null) {
      return;
    }
    this.assertFiniteNumber(value, 'value');
    this.assertFiniteNumber(timestampMs, 'timestampMs');

    const bucket = this.getBucket(this.getBucketId(timestampMs));
    bucket.max = bucket.max === undefined ? value : Math.max(bucket.max, value);
  }

  /**
   * Returns the maximum value across the current bucket and prior buckets still
   * inside the rolling window, or undefined when the window has no samples.
   */
  getRollingMax(timestampMs = Date.now()): number | undefined {
    this.assertFiniteNumber(timestampMs, 'timestampMs');

    const currentBucketId = this.getBucketId(timestampMs);
    const minimumBucketId = currentBucketId - this.bucketCount + 1;

    let rollingMax: number | undefined;
    for (const bucket of this.buckets) {
      if (bucket.max === undefined) {
        continue;
      }

      if (bucket.id < minimumBucketId || bucket.id > currentBucketId) {
        continue;
      }

      rollingMax = rollingMax === undefined ? bucket.max : Math.max(rollingMax, bucket.max);
    }

    return rollingMax;
  }

  private getBucketId(timestampMs: number): number {
    return Math.floor(timestampMs / this.bucketSizeMs);
  }

  private getBucket(bucketId: number): Bucket {
    const index = ((bucketId % this.bucketCount) + this.bucketCount) % this.bucketCount;
    const bucket = this.buckets[index];

    if (bucket.id !== bucketId) {
      bucket.id = bucketId;
      bucket.max = undefined;
    }

    return bucket;
  }

  private assertFiniteNumber(value: number, name: string): void {
    if (!Number.isFinite(value)) {
      throw new Error(`${name} must be a finite number.`);
    }
  }
}
