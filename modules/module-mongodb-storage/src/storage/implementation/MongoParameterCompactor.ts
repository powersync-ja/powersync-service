import { mongo } from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
import { bson, CompactOptions, InternalOpId } from '@powersync/service-core';
import type { VersionedPowerSyncMongo } from './db.js';

type ParameterCompactionReadDocument = {
  _id: InternalOpId;
  key: mongo.Document;
  lookup: unknown;
  bucket_parameters?: unknown[] | null;
};

export type ParameterCompactionResult = {
  collections: number;
  scannedEntries: number;
  distinctIdentities: number;
  deletedEntries: number;
};

const PARAMETER_COMPACTION_BATCH_SIZE = 10_000;
const PARAMETER_COMPACTION_DELETE_BATCH_SIZE = 1_000;

/**
 * Compacts parameter lookup data (the bucket_parameters collection).
 *
 * V1 scans the eligible operation-id range and filters its shared collection by stream in code.
 * V3 supplies a persisted stream cursor so this common compaction model scans only entries in
 * its un-compacted operation-id range.
 *
 * For background, see the `/docs/storage/parameter-lookups.md` file.
 */
export abstract class MongoParameterCompactor {
  constructor(
    protected readonly db: VersionedPowerSyncMongo,
    protected readonly group_id: number,
    protected readonly checkpoint: InternalOpId,
    protected readonly options: CompactOptions,
    protected readonly getCollectionsCb?: () => Promise<mongo.Collection<mongo.Document>[]>
  ) {}

  async compact() {
    const startedAt = Date.now();
    const compactedBefore = await this.readCompactedBefore();
    logger.info(
      `Incrementally compacting parameters for sync config ${this.group_id} from ${compactedBefore} up to checkpoint ${this.checkpoint}`
    );

    const result = await this.compactCollections(compactedBefore);

    // Persist only after every collection has completed. Implementations use $max so an
    // overlapping compactor cannot move the cursor backwards.
    await this.persistCompactedBefore(this.checkpoint);

    const durationSeconds = (Date.now() - startedAt) / 1000;
    logger.info(
      `Incremental parameter compaction completed for sync config ${this.group_id}: ` +
        `collections=${result.collections}, scanned=${result.scannedEntries}, distinct=${result.distinctIdentities}, ` +
        `deleted=${result.deletedEntries}, cursor=${compactedBefore}->${this.checkpoint}, duration=${durationSeconds.toFixed(1)}s`
    );
  }

  protected abstract readCompactedBefore(): Promise<InternalOpId>;
  protected abstract persistCompactedBefore(compactedBefore: InternalOpId): Promise<void>;

  protected async getCollections(): Promise<mongo.Collection<mongo.Document>[]> {
    if (this.getCollectionsCb == null) {
      throw new Error('getCollections callback not provided');
    }
    const collections = await this.getCollectionsCb();
    // Cast from the version-specific collection type to the generic Document type
    // used by the parameter compactor base class.
    return collections.map((collection) => collection as unknown as mongo.Collection<mongo.Document>);
  }

  /**
   * V1 and V3 both scan in operation-id order. V3 additionally supplies a persisted lower
   * cursor boundary.
   */
  protected compactionSort(): mongo.Document {
    return { _id: 1 };
  }

  /**
   * V3 overrides this with its persisted half-open operation-id range. V1 scans from the start
   * of the eligible operation-id range using MongoDB's default `_id` index.
   */
  protected compactionFilter(_compactedBefore?: InternalOpId): mongo.Document {
    return { _id: { $lt: this.checkpoint } };
  }

  /**
   * V1 shares its parameter collection across streams, so it filters `key.g` after the
   * `_id`-ordered MongoDB scan. V3's collection is already scoped to one stream.
   */
  protected shouldCompactDocument(doc: ParameterCompactionReadDocument): boolean {
    return doc._id < this.checkpoint;
  }

  protected deleteFilter(doc: ParameterCompactionReadDocument): mongo.Document {
    return {
      lookup: doc.lookup,
      _id: this.deleteIdFilter(doc._id),
      key: doc.key
    };
  }

  protected deleteTombstoneFilter(doc: ParameterCompactionReadDocument): mongo.Document {
    return {
      lookup: doc.lookup,
      _id: doc._id,
      key: doc.key
    };
  }

  protected async compactCollections(compactedBefore?: InternalOpId): Promise<ParameterCompactionResult> {
    let scannedEntries = 0;
    let distinctIdentities = 0;
    let deletedEntries = 0;
    let collections = 0;

    for (const collection of await this.getCollections()) {
      collections++;
      const result = await this.compactCollection(collection, compactedBefore);
      scannedEntries += result.scannedEntries;
      distinctIdentities += result.distinctIdentities;
      deletedEntries += result.deletedEntries;
    }

    return { collections, scannedEntries, distinctIdentities, deletedEntries };
  }

  protected async compactCollection(
    collection: mongo.Collection<mongo.Document>,
    compactedBefore?: InternalOpId
  ): Promise<Omit<ParameterCompactionResult, 'collections'>> {
    const cursor = collection.find(this.compactionFilter(compactedBefore), {
      sort: this.compactionSort(),
      batchSize: this.parameterCompactionBatchSize,
      projection: { _id: 1, key: 1, lookup: 1, bucket_parameters: { $slice: 1 } }
    });

    let scannedEntries = 0;
    let distinctIdentities = 0;
    let deletedEntries = 0;
    let deleteOperations: mongo.AnyBulkWriteOperation<mongo.Document>[] = [];
    let tombstoneDeleteOperations: mongo.AnyBulkWriteOperation<mongo.Document>[] = [];

    const flushDeletes = async (force: boolean) => {
      // Tombstone cleanup has two phases. Delete its preceding history while the tombstone is
      // still present, then delete the tombstone itself. This avoids leaving an older value
      // visible if a non-transactional deleteMany is interrupted after deleting the tombstone.
      const flushTombstones = force || tombstoneDeleteOperations.length >= PARAMETER_COMPACTION_DELETE_BATCH_SIZE;
      const flushHistory =
        force || deleteOperations.length >= PARAMETER_COMPACTION_DELETE_BATCH_SIZE || flushTombstones;

      if (flushHistory && deleteOperations.length > 0) {
        const result = await collection.bulkWrite(deleteOperations, { ordered: false });
        deletedEntries += result.deletedCount;
        deleteOperations = [];
      }

      if (flushTombstones && tombstoneDeleteOperations.length > 0) {
        const result = await collection.bulkWrite(tombstoneDeleteOperations, { ordered: false });
        deletedEntries += result.deletedCount;
        tombstoneDeleteOperations = [];
      }
    };

    try {
      while (await cursor.hasNext()) {
        const batch = cursor.readBufferedDocuments() as unknown as ParameterCompactionReadDocument[];
        scannedEntries += batch.length;

        const newestByIdentity = new Map<string, ParameterCompactionReadDocument>();
        for (const document of batch) {
          if (!this.shouldCompactDocument(document)) {
            continue;
          }
          const identity = (bson.serialize({ k: document.key, l: document.lookup }) as Buffer).toString('base64');
          const previous = newestByIdentity.get(identity);
          if (previous == null || previous._id < document._id) {
            newestByIdentity.set(identity, document);
          }
        }

        distinctIdentities += newestByIdentity.size;
        for (const document of newestByIdentity.values()) {
          const filter = this.deleteFilter(document);
          if (document.bucket_parameters?.length == 0) {
            deleteOperations.push({ deleteMany: { filter } });
            tombstoneDeleteOperations.push({
              deleteOne: { filter: this.deleteTombstoneFilter(document) }
            });
          } else {
            deleteOperations.push({ deleteMany: { filter } });
          }
        }

        await flushDeletes(false);
      }
      await flushDeletes(true);
    } finally {
      await cursor.close();
    }

    return { scannedEntries, distinctIdentities, deletedEntries };
  }

  protected get parameterCompactionBatchSize(): number {
    return PARAMETER_COMPACTION_BATCH_SIZE;
  }

  private deleteIdFilter(operationId: InternalOpId): mongo.Document {
    // The scan normally guarantees operationId < checkpoint. Keep this calculation explicit so
    // the delete remains safe if a future scan starts returning entries at the checkpoint.
    return { $lt: operationId >= this.checkpoint ? this.checkpoint : operationId };
  }
}
