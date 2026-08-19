import { mongo } from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
import { bson, CompactOptions, InternalOpId } from '@powersync/service-core';
import { LRUCache } from 'lru-cache';
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
const PARAMETER_COMPACTION_CACHE_SIZE = 50_000;

type CachedIdentity = {
  retainedId: InternalOpId | null;
};

type LeadingHistoryDelete = {
  lookup: unknown;
  keys: mongo.Document[];
};

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

  protected abstract shouldCompactDocument(doc: ParameterCompactionReadDocument): boolean;

  /** Deletes history preceding a batch for several identities sharing a lookup. */
  protected abstract leadingHistoryDeleteFilter(
    lookup: unknown,
    keys: mongo.Document[],
    before: InternalOpId
  ): mongo.Document;

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
    let scannedEntries = 0;
    let distinctIdentities = 0;
    let deletedEntries = 0;
    let lastId: InternalOpId | undefined;
    // This is used to optimize deletes. It is safe for items to be evicted: That just
    // changes deletes from "delete by _id" to "delete by range filter".
    const previousByIdentity = new LRUCache<string, CachedIdentity>({
      max: this.options.compactParameterCacheLimit ?? PARAMETER_COMPACTION_CACHE_SIZE
    });

    while (true) {
      const filter: mongo.Document = {
        _id: {
          ...(lastId == null ? { $gte: compactedBefore } : { $gt: lastId }),
          $lt: this.checkpoint
        }
      };
      const batch = (await collection
        .find(filter, {
          sort: { _id: 1 },
          limit: this.parameterCompactionBatchSize,
          batchSize: this.parameterCompactionBatchSize + 1,
          projection: { _id: 1, key: 1, lookup: 1, bucket_parameters: { $slice: 1 } }
        })
        .toArray()) as unknown as ParameterCompactionReadDocument[];
      if (batch.length == 0) {
        break;
      }
      lastId = batch.at(-1)!._id;
      scannedEntries += batch.length;

      // Keep the latest document for each identity and remove all earlier documents from this
      // batch by _id, avoiding a range query for documents that have already been read.
      const newestByIdentity = new Map<string, ParameterCompactionReadDocument>();
      const supersededIds: InternalOpId[] = [];
      for (const document of batch) {
        if (!this.shouldCompactDocument(document)) {
          continue;
        }
        const identity = (bson.serialize({ k: document.key, l: document.lookup }) as Buffer).toString('base64');
        const previous = newestByIdentity.get(identity);
        if (previous != null) {
          supersededIds.push(previous._id);
        }
        newestByIdentity.set(identity, document);
      }

      distinctIdentities += newestByIdentity.size;

      const leadingHistoryDeletes = new Map<string, LeadingHistoryDelete>();
      const tombstoneIds: InternalOpId[] = [];
      for (const [identity, document] of newestByIdentity) {
        const previous = previousByIdentity.get(identity);
        if (previous == null) {
          // Have not seen this (key, lookup) before, or it has been evicted from the cache.
          // Delete the entire leading range.
          // This should have decent performance on V3 storage; can be slow in some cases on V1.
          const lookupIdentity = (bson.serialize({ l: document.lookup }) as Buffer).toString('base64');
          const existing = leadingHistoryDeletes.get(lookupIdentity);
          if (existing == null) {
            leadingHistoryDeletes.set(lookupIdentity, { lookup: document.lookup, keys: [document.key] });
          } else {
            existing.keys.push(document.key);
          }
        } else if (previous.retainedId != null) {
          // We have already deleted the leading range for this (key, lookup). Only delete the last remaining
          // one by _id. This is always fast.
          supersededIds.push(previous.retainedId);
        }

        if (document.bucket_parameters?.length == 0) {
          tombstoneIds.push(document._id);
        }
      }

      // Phase 1: Delete documents read in this batch, plus retained documents from a prior batch.
      for (const ids of chunk(supersededIds, PARAMETER_COMPACTION_DELETE_BATCH_SIZE)) {
        const result = await collection.deleteMany({ _id: { $in: ids as any[] } });
        deletedEntries += result.deletedCount;
      }

      // Phase 2: Delete leading history once per lookup group. The batch start is always below
      // the checkpoint; min() nevertheless keeps the range explicitly checkpoint-bounded.
      const deleteBefore = batch[0]._id < this.checkpoint ? batch[0]._id : this.checkpoint;
      for (const { lookup, keys } of leadingHistoryDeletes.values()) {
        for (const keyBatch of chunk(keys, PARAMETER_COMPACTION_DELETE_BATCH_SIZE)) {
          const result = await collection.deleteMany(this.leadingHistoryDeleteFilter(lookup, keyBatch, deleteBefore));
          deletedEntries += result.deletedCount;
        }
      }

      // Phase 3: A tombstone is removed only after all preceding history has been removed.
      for (const ids of chunk(tombstoneIds, PARAMETER_COMPACTION_DELETE_BATCH_SIZE)) {
        const result = await collection.deleteMany({ _id: { $in: ids } } as any);
        deletedEntries += result.deletedCount;
      }

      // Update the LRU only after all phases succeed. An evicted identity safely falls back to a
      // grouped leading-history delete when it appears again.
      for (const [identity, document] of newestByIdentity) {
        previousByIdentity.set(identity, {
          retainedId: document.bucket_parameters?.length == 0 ? null : document._id
        });
      }
    }

    return { scannedEntries, distinctIdentities, deletedEntries };
  }
}

function* chunk<T>(items: T[], size: number): Iterable<T[]> {
  for (let offset = 0; offset < items.length; offset += size) {
    yield items.slice(offset, offset + size);
  }
}
