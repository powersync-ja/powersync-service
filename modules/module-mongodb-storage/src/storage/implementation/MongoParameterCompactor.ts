import { logger } from '@powersync/lib-services-framework';
import { bson, InternalOpId } from '@powersync/service-core';
import { LRUCache } from 'lru-cache';
import { PowerSyncMongo } from './db.js';

export class MongoParameterCompactor {
  constructor(
    private db: PowerSyncMongo,
    private group_id: number,
    private checkpoint: InternalOpId
  ) {}

  async compact() {
    logger.info(`Compacting parameters for group ${this.group_id} up to checkpoint ${this.checkpoint}`);
    // This is the currently-active checkpoint.
    // We do not remove any data that may be used by this checkpoint.
    // snapshot queries ensure that if any clients are still using older checkpoints, they would
    // not be affected by this compaction.
    const checkpoint = await this.checkpoint;
    if (checkpoint == null) {
      return;
    }

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
        projection: { _id: 1, key: 1, lookup: 1 }
      }
    );

    // The index doesn't cover sorting by key, so we keep our own cache of the last seen key.
    let lastByKey = new LRUCache<string, InternalOpId>({
      max: 10_000
    });
    let removeIds: InternalOpId[] = [];

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
      }

      if (removeIds.length >= 1000) {
        await this.db.bucket_parameters.deleteMany({ _id: { $in: removeIds } });
        logger.info(`Removed ${removeIds.length} stale parameter entries`);
        removeIds = [];
      }
    }

    if (removeIds.length > 0) {
      await this.db.bucket_parameters.deleteMany({ _id: { $in: removeIds } });
      logger.info(`Removed ${removeIds.length} stale parameter entries`);
    }
    logger.info('Parameter compaction completed');
  }
}
