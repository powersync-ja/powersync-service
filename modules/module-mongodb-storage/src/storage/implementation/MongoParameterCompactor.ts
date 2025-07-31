import { logger } from '@powersync/lib-services-framework';
import { bson, InternalOpId } from '@powersync/service-core';
import { PowerSyncMongo } from './db.js';

export class MongoParameterCompactor {
  constructor(
    private db: PowerSyncMongo,
    private group_id: number,
    private checkpoint: InternalOpId
  ) {}

  /**
   * This is the oldest checkpoint that we consider safe to still use. We cleanup old parameter
   * but no data that would be used by this checkpoint.
   *
   * Specifically, we return a checkpoint that has been available for at least 5 minutes, then
   * we can delete data only used for checkpoints older than that.
   *
   * @returns null if there is no safe checkpoint available.
   */
  async getActiveCheckpoint(): Promise<InternalOpId | null> {
    const syncRules = await this.db.sync_rules.findOne({ _id: this.group_id });
    if (syncRules == null) {
      return null;
    }

    return syncRules.last_checkpoint;
  }

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

    let lastDoc: RawParameterData | null = null;
    let removeIds: InternalOpId[] = [];

    while (await cursor.hasNext()) {
      const batch = cursor.readBufferedDocuments();
      for (let doc of batch) {
        if (doc._id >= checkpoint) {
          continue;
        }
        const rawDoc: RawParameterData = {
          _id: doc._id,
          // Serializing to a Buffer is an easy way to check for exact equality of arbitrary BSON values.
          data: bson.serialize({
            key: doc.key,
            lookup: doc.lookup
          }) as Buffer
        };
        if (lastDoc != null && lastDoc.data.equals(rawDoc.data) && lastDoc._id < doc._id) {
          removeIds.push(lastDoc._id);
        }

        lastDoc = rawDoc;
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

interface RawParameterData {
  _id: InternalOpId;
  data: Buffer;
}
