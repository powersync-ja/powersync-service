import { mongo } from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
import { bson, CompactOptions, InternalOpId } from '@powersync/service-core';
import { LRUCache } from 'lru-cache';
import type { VersionedPowerSyncMongo } from './db.js';

type ParameterCompactionReadDocument = {
  _id: InternalOpId;
  key: mongo.Document;
  lookup: bson.Binary;
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
  /**
   * The `_id` of the document retained for this identity in a previous batch, or null if that
   * document was a tombstone - in which case it has been deleted along with all its history, and
   * nothing remains to delete for the identity.
   */
  retainedId: InternalOpId | null;
};

type LeadingHistoryDelete = {
  lookup: bson.Binary;
  keys: mongo.Document[];
};

/**
 * Compacts parameter lookup data (the bucket_parameters collection).
 *
 * Both storage versions persist a per-stream compaction cursor, so a run only scans entries in the
 * un-compacted operation-id range. V1 scans its shared collection using only the `_id` index, so it
 * additionally filters by stream in code.
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

  /**
   * Set once the invalidation fence for this pass has been persisted. See
   * {@link ensureInvalidationFence}.
   */
  #invalidationFencePersisted = false;

  async compact() {
    const startedAt = Date.now();
    const compactedBefore = await this.readCompactedBefore();
    logger.info(
      `Incrementally compacting parameters for sync config ${this.replicationStreamId} from ${compactedBefore} up to checkpoint ${this.checkpoint}`
    );

    const result = await this.compactCollections(compactedBefore);

    // Persist only after every collection has completed. This uses $max so an overlapping
    // compactor cannot move the cursor backwards.
    await this.persistCompactedBefore(this.checkpoint);

    const durationSeconds = (Date.now() - startedAt) / 1000;
    logger.info(
      `Incremental parameter compaction completed for sync config ${this.replicationStreamId}: ` +
        `collections=${result.collections}, scanned=${result.scannedEntries}, distinct=${result.distinctIdentities}, ` +
        `deleted=${result.deletedEntries}, cursor=${compactedBefore}->${this.checkpoint}, ` +
        `fence=${this.#invalidationFencePersisted ? this.checkpoint : 'unchanged'}, duration=${durationSeconds.toFixed(1)}s`
    );
  }

  /**
   * The exclusive operation-id boundary through which this stream's parameter indexes have all
   * been compacted.
   */
  protected async readCompactedBefore(): Promise<InternalOpId> {
    const stream = await this.db.sync_rules.findOne(
      { _id: this.replicationStreamId },
      { projection: { parameter_compaction: 1 } }
    );
    return stream?.parameter_compaction?.compacted_before == null
      ? 0n
      : BigInt(stream.parameter_compaction.compacted_before);
  }

  protected async persistCompactedBefore(compactedBefore: InternalOpId): Promise<void> {
    await this.db.sync_rules.updateOne(
      { _id: this.replicationStreamId },
      {
        $max: { 'parameter_compaction.compacted_before': compactedBefore }
      }
    );
  }

  /**
   * Commits the checkpoint-change invalidation fence before the first delete of this pass.
   *
   * Checkpoint change detection finds changed lookups by querying parameter entries in
   * (lastCheckpoint, nextCheckpoint]. Compaction physically removes entries in that range, so a
   * checkpoint that can no longer see the full history must instead invalidate all parameter
   * buckets. The fence records the boundary below which that history may be missing.
   *
   * The fence must be committed before the first delete: MongoDB snapshot ordering then
   * guarantees that a checkpoint snapshot which observes a deletion also observes the fence.
   *
   * This deliberately isn't the same value as the compaction cursor. If the pass fails halfway,
   * the fence only causes conservative invalidation, while an advanced cursor would skip
   * deletion work that never completed.
   */
  private async ensureInvalidationFence(): Promise<void> {
    // We can consider incrementally updating the fence based on the current cursor position instead
    // of the checkpoint. That would result in lower risk of triggering invalidations, but it would
    // result in a higher number of updates to the `sync_rules` collection, which can make it
    // counter-productive.
    // Another option is to introduce an artificial delay of a couple of seconds before writing the fence,
    // giving some chance for every API process to catch up. Note that the delay would have to apply
    // to both the deletes and the fence - the fence write must still happen before we do any deletes.
    if (this.#invalidationFencePersisted) {
      return;
    }
    await this.db.sync_rules.updateOne(
      { _id: this.replicationStreamId },
      {
        $max: { 'parameter_compaction.checkpoint_changes_invalid_before': this.checkpoint }
      }
    );
    this.#invalidationFencePersisted = true;
  }

  protected abstract getCollections(): Promise<mongo.Collection<mongo.Document>[]>;

  protected abstract shouldCompactDocument(doc: ParameterCompactionReadDocument): boolean;

  /** Deletes history preceding a batch for several identities sharing a lookup. */
  protected abstract leadingHistoryDeleteFilter(
    lookup: bson.Binary,
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
          const lookupIdentity = document.lookup.toString('base64');
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
      deletedEntries += await this.deleteByIds(collection, supersededIds);

      // Phase 2: Delete leading history once per lookup group. The batch is read with
      // `_id < checkpoint`, so this range is checkpoint-bounded.
      const deleteBefore = batch[0]._id;
      // The deletes are collected into bulkWrite commands: With high lookup cardinality there is a
      // group per identity, and a command per group would mean a round trip per identity.
      let deleteOperations: mongo.AnyBulkWriteOperation[] = [];
      let pendingKeys = 0;
      const flushDeleteOperations = async () => {
        if (deleteOperations.length == 0) {
          return;
        }
        await this.ensureInvalidationFence();
        const result = await collection.bulkWrite(deleteOperations, { ordered: false });
        deletedEntries += result.deletedCount;
        deleteOperations = [];
        pendingKeys = 0;
      };
      for (const { lookup, keys } of leadingHistoryDeletes.values()) {
        for (const keyBatch of chunk(keys, PARAMETER_COMPACTION_DELETE_BATCH_SIZE)) {
          deleteOperations.push({
            deleteMany: { filter: this.leadingHistoryDeleteFilter(lookup, keyBatch, deleteBefore) }
          });
          // Bound the command size by the total number of keys it covers, not by the number of
          // operations: a single group may already cover the entire batch.
          pendingKeys += keyBatch.length;
          if (pendingKeys >= PARAMETER_COMPACTION_DELETE_BATCH_SIZE) {
            await flushDeleteOperations();
          }
        }
      }
      // Phase 3 requires all leading history to be deleted first.
      await flushDeleteOperations();

      // Phase 3: A tombstone is removed only after all preceding history has been removed.
      deletedEntries += await this.deleteByIds(collection, tombstoneIds);

      // Update the LRU only after all phases succeed. An evicted identity safely falls back to a
      // grouped leading-history delete when it appears again.
      for (const [identity, document] of newestByIdentity) {
        // Tombstones are recorded as `retainedId: null`: phases 2 and 3 removed the entire history
        // for the identity, including the tombstone, so a later sighting needs neither delete.
        previousByIdentity.set(identity, {
          retainedId: document.bucket_parameters?.length == 0 ? null : document._id
        });
      }
    }

    return { scannedEntries, distinctIdentities, deletedEntries };
  }

  /** Deletes documents by `_id`, chunked to bound the command size. Returns the number deleted. */
  private async deleteByIds(collection: mongo.Collection<mongo.Document>, ids: InternalOpId[]): Promise<number> {
    let deletedEntries = 0;
    for (const idBatch of chunk(ids, PARAMETER_COMPACTION_DELETE_BATCH_SIZE)) {
      await this.ensureInvalidationFence();
      // Cast: `_id` here is an InternalOpId (bigint), not the driver's default ObjectId.
      const result = await collection.deleteMany({ _id: { $in: idBatch } } as any);
      deletedEntries += result.deletedCount;
    }
    return deletedEntries;
  }
}

function* chunk<T>(items: T[], size: number): Iterable<T[]> {
  for (let offset = 0; offset < items.length; offset += size) {
    yield items.slice(offset, offset + size);
  }
}
