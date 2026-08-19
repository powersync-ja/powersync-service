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
    protected readonly replicationStreamId: number,
    protected readonly checkpoint: InternalOpId,
    protected readonly options: CompactOptions,
    protected readonly parameterCompactionBatchSize = PARAMETER_COMPACTION_BATCH_SIZE
  ) {}

  async compact() {
    const startedAt = Date.now();
    const compactedBefore = await this.readCompactedBefore();
    logger.info(
      `Incrementally compacting parameters for sync config ${this.replicationStreamId} from ${compactedBefore} up to checkpoint ${this.checkpoint}`
    );

    const result = await this.compactCollections(compactedBefore);

    // Persist only after every collection has completed. Implementations use $max so an
    // overlapping compactor cannot move the cursor backwards.
    await this.persistCompactedBefore(this.checkpoint);

    const durationSeconds = (Date.now() - startedAt) / 1000;
    logger.info(
      `Incremental parameter compaction completed for sync config ${this.replicationStreamId}: ` +
        `collections=${result.collections}, scanned=${result.scannedEntries}, distinct=${result.distinctIdentities}, ` +
        `deleted=${result.deletedEntries}, cursor=${compactedBefore}->${this.checkpoint}, duration=${durationSeconds.toFixed(1)}s`
    );
  }

  protected abstract readCompactedBefore(): Promise<InternalOpId>;
  protected abstract persistCompactedBefore(compactedBefore: InternalOpId): Promise<void>;

  protected abstract getCollections(): Promise<mongo.Collection<mongo.Document>[]>;

  protected abstract compactionFilter(compactedBefore: InternalOpId): mongo.Document;

  protected abstract shouldCompactDocument(doc: ParameterCompactionReadDocument): boolean;

  protected abstract deleteFilter(doc: ParameterCompactionReadDocument): mongo.Document;

  protected abstract deleteTombstoneFilter(doc: ParameterCompactionReadDocument): mongo.Document;

  protected async compactCollections(compactedBefore: InternalOpId): Promise<ParameterCompactionResult> {
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
    compactedBefore: InternalOpId
  ): Promise<Omit<ParameterCompactionResult, 'collections'>> {
    const cursor = collection.find(this.compactionFilter(compactedBefore), {
      sort: { _id: 1 },
      batchSize: this.parameterCompactionBatchSize,
      projection: { _id: 1, key: 1, lookup: 1, bucket_parameters: { $slice: 1 } }
    });
    await using _ = { [Symbol.asyncDispose]: () => cursor.close() };

    let scannedEntries = 0;
    let distinctIdentities = 0;
    let deletedEntries = 0;

    while (await cursor.hasNext()) {
      const batch = cursor.readBufferedDocuments() as unknown as ParameterCompactionReadDocument[];
      scannedEntries += batch.length;

      // Optimization: Only keep the latest doc in each batch
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

      let deleteOperations: mongo.AnyBulkWriteOperation<mongo.Document>[] = [];
      let tombstoneDeleteOperations: mongo.AnyBulkWriteOperation<mongo.Document>[] = [];
      for (const document of newestByIdentity.values()) {
        const filter = this.deleteFilter(document);
        deleteOperations.push({ deleteMany: { filter } });
        if (document.bucket_parameters?.length == 0) {
          tombstoneDeleteOperations.push({
            deleteOne: { filter: this.deleteTombstoneFilter(document) }
          });
        }
      }

      // Tombstone cleanup has two phases. Delete its preceding history while the tombstone is
      // still present, then delete the tombstone itself. This avoids leaving an older value
      // visible if a non-transactional deleteMany is interrupted after deleting the tombstone.
      if (deleteOperations.length > 0) {
        const result = await collection.bulkWrite(deleteOperations, { ordered: false });
        deletedEntries += result.deletedCount;
        deleteOperations = [];
      }

      if (tombstoneDeleteOperations.length > 0) {
        const result = await collection.bulkWrite(tombstoneDeleteOperations, { ordered: false });
        deletedEntries += result.deletedCount;
        tombstoneDeleteOperations = [];
      }
    }

    return { scannedEntries, distinctIdentities, deletedEntries };
  }
}
