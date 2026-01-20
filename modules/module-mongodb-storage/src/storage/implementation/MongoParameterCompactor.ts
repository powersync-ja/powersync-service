import { logger } from '@powersync/lib-services-framework';
import { bson, CompactOptions, InternalOpId } from '@powersync/service-core';
import { LRUCache } from 'lru-cache';
import { PowerSyncMongo } from './db.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { BucketParameterDocument } from './models.js';
import { BucketDefinitionMapping } from './BucketDefinitionMapping.js';

/**
 * Compacts parameter lookup data (the bucket_parameters collection).
 *
 * This scans through the entire collection to find data to compact.
 *
 * For background, see the `/docs/parameters-lookups.md` file.
 */
export class MongoParameterCompactor {
  constructor(
    private db: PowerSyncMongo,
    private group_id: number,
    private checkpoint: InternalOpId,
    private options: CompactOptions
  ) {}

  async compact() {
    logger.info(`Compacting parameters for group ${this.group_id} up to checkpoint ${this.checkpoint}`);
    // This is the currently-active checkpoint.
    // We do not remove any data that may be used by this checkpoint.
    // snapshot queries ensure that if any clients are still using older checkpoints, they would
    // not be affected by this compaction.
    const checkpoint = this.checkpoint;

    // Index on {'key.g': 1, lookup: 1, _id: 1}
    // In theory, we could let MongoDB do more of the work here, by grouping by (key, lookup)
    // in MongoDB already. However, that risks running into cases where MongoDB needs to process
    // very large amounts of data before returning results, which could lead to timeouts.
    const cursor = this.db.bucket_parameters.find(
      {
        'key.g': this.group_id
      },
      {
        sort: { lookup: 1, _id: 1 },
        batchSize: 10_000,
        projection: { _id: 1, key: 1, lookup: 1, bucket_parameters: 1 }
      }
    );

    // The index doesn't cover sorting by key, so we keep our own cache of the last seen key.
    let lastByKey = new LRUCache<string, InternalOpId>({
      max: this.options.compactParameterCacheLimit ?? 10_000
    });
    let removeIds: InternalOpId[] = [];
    let removeDeleted: mongo.AnyBulkWriteOperation<BucketParameterDocument>[] = [];

    const flush = async (force: boolean) => {
      if (removeIds.length >= 1000 || (force && removeIds.length > 0)) {
        const results = await this.db.bucket_parameters.deleteMany({ _id: { $in: removeIds } });
        logger.info(`Removed ${results.deletedCount} (${removeIds.length}) superseded parameter entries`);
        removeIds = [];
      }

      if (removeDeleted.length > 10 || (force && removeDeleted.length > 0)) {
        const results = await this.db.bucket_parameters.bulkWrite(removeDeleted);
        logger.info(`Removed ${results.deletedCount} (${removeDeleted.length}) deleted parameter entries`);
        removeDeleted = [];
      }
    };

    while (await cursor.hasNext()) {
      const batch = cursor.readBufferedDocuments();
      for (let doc of batch) {
        if (doc._id >= checkpoint) {
          continue;
        }
        const uniqueKey = (
          bson.serialize({
            k: doc.key,
            l: doc.lookup
          }) as Buffer
        ).toString('base64');
        const previous = lastByKey.get(uniqueKey);
        if (previous != null && previous < doc._id) {
          // We have a newer entry for the same key, so we can remove the old one.
          removeIds.push(previous);
        }
        lastByKey.set(uniqueKey, doc._id);

        if (doc.bucket_parameters?.length == 0) {
          // This is a delete operation, so we can remove it completely.
          // For this we cannot remove the operation itself only: There is a possibility that
          // there is still an earlier operation with the same key and lookup, that we don't have
          // in the cache due to cache size limits. So we need to explicitly remove all earlier operations.
          removeDeleted.push({
            deleteMany: {
              filter: { 'key.g': doc.key.g, lookup: doc.lookup, _id: { $lte: doc._id }, key: doc.key }
            }
          });
        }
      }

      await flush(false);
    }

    await flush(true);
    logger.info('Parameter compaction completed');
  }
}
