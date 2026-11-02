import type { mongo } from '@powersync/lib-service-mongodb';
import type { Logger } from '@powersync/lib-services-framework';
import { performance } from 'node:perf_hooks';

const COUNT_TIMEOUT_MS = 10_000;
/**
 * Keep below 5% for MongoDB's random-cursor optimization on large collections.
 * https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/
 */
const SAMPLE_FRACTION = 0.01;
const MAX_SAMPLE_SIZE = 10_000;

interface SampleCounts {
  sampled: { count: number }[];
  matches: { count: number }[];
}

/**
 * Try an exact filtered count, then estimate selectivity from a bounded sample.
 * Each attempt has a separate 10-second budget. Failures leave progress unknown (-1).
 */
export async function estimateFilteredSnapshotCount(options: {
  collection: mongo.Collection;
  filter: mongo.Filter<mongo.Document>;
  signal: AbortSignal;
  logger: Logger;
}): Promise<number> {
  const { collection, filter, signal, logger } = options;
  signal.throwIfAborted();
  try {
    return await collection.countDocuments(filter, {
      collation: { locale: 'simple' },
      readConcern: 'majority',
      timeoutMS: COUNT_TIMEOUT_MS,
      signal
    });
  } catch (error) {
    signal.throwIfAborted();
    logger.debug(`Filtered snapshot count unavailable for ${collection.namespace}; trying a sample strategy`, error);
  }

  const sampleStartedAt = performance.now();
  try {
    const metadataOptions = { timeoutMS: COUNT_TIMEOUT_MS, signal };
    const totalCount = await collection.estimatedDocumentCount(metadataOptions);
    signal.throwIfAborted();
    if (totalCount <= 0) {
      // Metadata alone cannot establish that a filtered result is empty after the count failed.
      return -1;
    }
    const remainingTimeMS = Math.floor(COUNT_TIMEOUT_MS - (performance.now() - sampleStartedAt));
    if (remainingTimeMS <= 0) {
      return -1;
    }
    // Keep $sample first and below 5% for MongoDB's random-cursor optimization on large collections.
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/
    const sampleSize = Math.min(MAX_SAMPLE_SIZE, Math.max(1, Math.floor(totalCount * SAMPLE_FRACTION)));
    const cursor = collection.aggregate<SampleCounts>(
      [
        { $sample: { size: sampleSize } },
        {
          $facet: {
            sampled: [{ $count: 'count' }],
            matches: [{ $match: filter }, { $count: 'count' }]
          }
        }
      ],
      {
        collation: { locale: 'simple' },
        readConcern: 'majority',
        timeoutMS: remainingTimeMS,
        signal,
        allowDiskUse: false
      }
    );
    try {
      const sample = await cursor.next();
      const sampledCount = sample?.sampled[0]?.count ?? 0;
      const matchedCount = sample?.matches[0]?.count ?? 0;
      if (sampledCount == 0 || matchedCount == 0) {
        // A rare predicate can miss the whole sample; that does not establish an empty result.
        return -1;
      }
      return Math.round((matchedCount / sampledCount) * totalCount);
    } finally {
      await cursor.close();
    }
  } catch (error) {
    signal.throwIfAborted();
    logger.debug(`Filtered snapshot sample unavailable for ${collection.namespace}; total remains unknown`, error);
    return -1;
  }
}
