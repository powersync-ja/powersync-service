import { mongo } from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
import { bson, CompactOptions, InternalOpId } from '@powersync/service-core';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { ReplicationStreamDocumentV3 } from './models.js';
import type { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

type ParameterCompactionReadDocument = {
  _id: InternalOpId;
  key: mongo.Document;
  lookup: unknown;
  bucket_parameters?: unknown[] | null;
};

const PARAMETER_COMPACTION_BATCH_SIZE = 10_000;
const PARAMETER_COMPACTION_DELETE_BATCH_SIZE = 1_000;

/**
 * Incrementally compacts V3 parameter indexes using the stream operation sequence as a work
 * cursor. The cursor is advanced only after every parameter index has completed the same range.
 */
export class MongoParameterCompactorV3 extends MongoParameterCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV3;

  constructor(
    db: VersionedPowerSyncMongoV3,
    group_id: number,
    checkpoint: InternalOpId,
    options: CompactOptions,
    getCollectionsCb: () => Promise<mongo.Collection<mongo.Document>[]>,
    private readonly batchSize = PARAMETER_COMPACTION_BATCH_SIZE
  ) {
    super(db, group_id, checkpoint, options, getCollectionsCb);
  }

  override async compact() {
    const startedAt = Date.now();
    const stream = (await this.db.sync_rules.findOne(
      { _id: this.group_id },
      { projection: { parameter_compaction: 1 } }
    )) as ReplicationStreamDocumentV3 | null;
    const compactedBefore =
      stream?.parameter_compaction?.compacted_before == null
        ? 0n
        : BigInt(stream.parameter_compaction.compacted_before);

    logger.info(
      `Incrementally compacting parameters for sync config ${this.group_id} from ${compactedBefore} up to checkpoint ${this.checkpoint}`
    );

    let scannedEntries = 0;
    let distinctIdentities = 0;
    let deletedEntries = 0;
    let collectionCount = 0;

    for (const collection of await this.getCollections()) {
      collectionCount++;
      const result = await this.compactCollectionIncrementally(collection, compactedBefore);
      scannedEntries += result.scannedEntries;
      distinctIdentities += result.distinctIdentities;
      deletedEntries += result.deletedEntries;
    }

    // This update is deliberately after all collections have completed. $max makes overlapping
    // compactors safe when a slower invocation finishes after a faster one.
    await this.db.sync_rules.updateOne({ _id: this.group_id }, {
      $max: { 'parameter_compaction.compacted_before': this.checkpoint }
    } as any);

    const durationSeconds = (Date.now() - startedAt) / 1000;
    logger.info(
      `Incremental parameter compaction completed for sync config ${this.group_id}: ` +
        `collections=${collectionCount}, scanned=${scannedEntries}, distinct=${distinctIdentities}, ` +
        `deleted=${deletedEntries}, cursor=${compactedBefore}->${this.checkpoint}, duration=${durationSeconds.toFixed(1)}s`
    );
  }

  private async compactCollectionIncrementally(
    collection: mongo.Collection<mongo.Document>,
    compactedBefore: InternalOpId
  ): Promise<{ scannedEntries: number; distinctIdentities: number; deletedEntries: number }> {
    if (compactedBefore >= this.checkpoint) {
      return { scannedEntries: 0, distinctIdentities: 0, deletedEntries: 0 };
    }

    const cursor = collection.find(
      {
        _id: {
          $gte: compactedBefore,
          $lt: this.checkpoint
        }
      } as any,
      {
        sort: { _id: 1 },
        batchSize: this.batchSize,
        projection: { _id: 1, key: 1, lookup: 1, bucket_parameters: { $slice: 1 } }
      }
    );

    let scannedEntries = 0;
    let distinctIdentities = 0;
    let deletedEntries = 0;
    let deleteOperations: mongo.AnyBulkWriteOperation<mongo.Document>[] = [];
    let tombstoneDeleteOperations: mongo.AnyBulkWriteOperation<mongo.Document>[] = [];

    const flushDeletes = async (force: boolean) => {
      // Tombstone deletes remove the tombstone and all preceding history. Keep them in a
      // separate phase so they cannot run before ordinary superseded-entry deletes.
      const flushTombstones = force || tombstoneDeleteOperations.length >= PARAMETER_COMPACTION_DELETE_BATCH_SIZE;
      const flushOrdinary =
        force || deleteOperations.length >= PARAMETER_COMPACTION_DELETE_BATCH_SIZE || flushTombstones;

      if (flushOrdinary && deleteOperations.length > 0) {
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

        // Optimization: Only track the latest document by (key, lookup)
        const newestByIdentity = new Map<string, ParameterCompactionReadDocument>();
        for (const document of batch) {
          const identity = (bson.serialize({ k: document.key, l: document.lookup }) as Buffer).toString('base64');
          const previous = newestByIdentity.get(identity);
          if (previous == null || previous._id < document._id) {
            newestByIdentity.set(identity, document);
          }
        }

        distinctIdentities += newestByIdentity.size;
        for (const document of newestByIdentity.values()) {
          const tombstone = document.bucket_parameters?.length == 0;
          const operation = {
            deleteMany: {
              filter: {
                key: document.key,
                lookup: document.lookup,
                // Apply the checkpoint bound in code even though document._id is in the range.
                _id: this.deleteIdFilter(document._id, tombstone)
              } as any
            }
          } satisfies mongo.AnyBulkWriteOperation<mongo.Document>;
          (tombstone ? tombstoneDeleteOperations : deleteOperations).push(operation);
        }

        await flushDeletes(false);
      }
      await flushDeletes(true);
    } finally {
      await cursor.close();
    }

    return { scannedEntries, distinctIdentities, deletedEntries };
  }

  private deleteIdFilter(operationId: InternalOpId, tombstone: boolean): mongo.Document {
    // The scan already guarantees operationId < checkpoint. Keep this calculation explicit so
    // the delete remains safe if the scan bounds change later, without asking MongoDB to combine
    // two predicates on _id.
    if (operationId >= this.checkpoint) {
      return { $lt: this.checkpoint };
    }
    return tombstone ? { $lte: operationId } : { $lt: operationId };
  }
}
