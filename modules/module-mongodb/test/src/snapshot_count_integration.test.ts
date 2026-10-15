import { estimateFilteredSnapshotCount } from '@module/replication/estimateFilteredSnapshotCount.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { createLogger } from '@powersync/lib-services-framework';
import { randomUUID } from 'node:crypto';
import { describe, expect, test, vi } from 'vitest';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { connectMongoData } from './util.js';

const LOGGER = createLogger({ silent: true });

describe.skipIf(DATABASE_TYPE == DatabaseType.DOCUMENTDB)('filtered snapshot count integration', () => {
  test('counts with simple collation even when the collection uses case-insensitive collation', async () => {
    const { client, db } = await connectMongoData();
    const collection = await db.createCollection(`snapshot_count_${randomUUID()}`, {
      collation: { locale: 'en', strength: 2 }
    });
    try {
      await collection.insertMany([{ label: 'live' }, { label: 'LIVE' }, { label: 'other' }]);
      expect(
        await estimateFilteredSnapshotCount({
          collection,
          filter: { $expr: { $eq: ['$label', 'live'] } },
          signal: new AbortController().signal,
          logger: LOGGER
        })
      ).toBe(1);
    } finally {
      await collection.drop();
      await client.close();
    }
  });

  test('executes the sample pipeline after a failed count and preserves binary filter semantics', async () => {
    const { client, db } = await connectMongoData();
    const collection = await db.createCollection(`snapshot_count_${randomUUID()}`, {
      collation: { locale: 'en', strength: 2 }
    });
    try {
      // For this small collection the sample contains one document. All rows have the same label,
      // making the sampled result deterministic while exercising the real $sample/$facet pipeline.
      await collection.insertMany(Array.from({ length: 50 }, () => ({ label: 'live' })));
      const count = vi
        .spyOn(collection, 'countDocuments')
        .mockRejectedValue(new mongo.MongoOperationTimeoutError('timeout'));
      try {
        expect(
          await estimateFilteredSnapshotCount({
            collection,
            filter: { $and: [{ label: 'live' }, { $expr: { $eq: ['$label', 'live'] } }] },
            signal: new AbortController().signal,
            logger: LOGGER
          })
        ).toBe(50);
        expect(
          await estimateFilteredSnapshotCount({
            collection,
            filter: { $expr: { $eq: ['$label', 'LIVE'] } },
            signal: new AbortController().signal,
            logger: LOGGER
          })
        ).toBe(-1);
      } finally {
        count.mockRestore();
      }
    } finally {
      await collection.drop();
      await client.close();
    }
  });
});
