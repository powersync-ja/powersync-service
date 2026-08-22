import { mongo } from '@powersync/lib-service-mongodb';
import { logger as defaultLogger, Logger } from '@powersync/lib-services-framework';
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
  deletedEntries: number;
};

const PARAMETER_COMPACTION_BATCH_SIZE = 10_000;
const PARAMETER_COMPACTION_DELETE_BATCH_SIZE = 1_000;
const PARAMETER_COMPACTION_CACHE_SIZE = 50_000;
/**
 * How often progress is persisted during a pass.
 *
 * Kept coarse: replication also updates the `sync_rules` document on every commit.
 */
const PARAMETER_COMPACTION_PERSIST_INTERVAL_MS = 60_000;

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
 * One collection being compacted, and how far this pass has processed it.
 */
type CompactionScope = {
  collection: mongo.Collection<mongo.Document>;
  /**
   * Exclusive boundary: every entry below this has been processed in this collection.
   *
   * Set to the target checkpoint once the collection has no more entries in range.
   */
  position: InternalOpId;
  scannedEntries: number;
  deletedEntries: number;
};

/**
 * Compacts parameter lookup data (the bucket_parameters collection).
 *
 * Both storage versions persist a per-stream compaction cursor, so a run only scans entries in the
 * un-compacted operation-id range. V1 scans its shared collection using only the `_id` index, so it
 * additionally filters by stream in code.
 *
 * The cursor is a single value covering every collection of the stream, so it can only be advanced
 * to a boundary that all collections have passed. To keep it moving during a long pass, collections
 * are processed in lock-step rather than one after another - see {@link compactCollections}.
 *
 * For background, see the `/docs/storage/parameter-lookups.md` file.
 */
export abstract class MongoParameterCompactor {
  protected readonly logger: Logger;
  protected readonly signal?: AbortSignal;

  constructor(
    protected readonly db: VersionedPowerSyncMongo,
    protected readonly replicationStreamId: number,
    protected readonly checkpoint: InternalOpId,
    protected readonly options: CompactOptions,
    protected readonly parameterCompactionBatchSize = PARAMETER_COMPACTION_BATCH_SIZE,
    protected readonly parameterCompactionPersistIntervalMs = PARAMETER_COMPACTION_PERSIST_INTERVAL_MS
  ) {
    this.logger = options.logger ?? defaultLogger;
    this.signal = options.signal;
  }

  /**
   * Set once the invalidation fence for this pass has been persisted. See
   * {@link ensureInvalidationFence}.
   */
  #invalidationFencePersisted = false;

  async compact() {
    const startedAt = Date.now();
    this.signal?.throwIfAborted();
    const compactedBefore = await this.readCompactedBefore();
    this.logger.info(`Incrementally compacting parameters from ${compactedBefore} up to checkpoint ${this.checkpoint}`);

    const result = await this.compactCollections(compactedBefore);

    // Persist only after every collection has completed. This uses $max so an overlapping
    // compactor cannot move the cursor backwards.
    await this.persistCompactedBefore(this.checkpoint);

    const durationSeconds = (Date.now() - startedAt) / 1000;
    this.logger.info(
      `Incremental parameter compaction completed: ` +
        `collections=${result.collections}, scanned=${result.scannedEntries}, ` +
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

  /**
   * Processes every collection of the stream, in lock-step: each turn takes one batch from the
   * collection that has processed the least so far.
   *
   * The persisted cursor is the minimum position over all collections, which is exactly the
   * boundary that all of them have passed, so it can be advanced periodically during the pass.
   * Always picking the collection that is furthest behind also keeps each of them within one batch
   * of that boundary, bounding the work an interrupted pass has to repeat.
   *
   * V1 storage always has a single collection; V3 has a collection per defined index. So in V1 the
   * ame process collapses to compacting the single collection in order, while V3 can alternate between
   * collections.
   */
  private async compactCollections(compactedBefore: InternalOpId): Promise<ParameterCompactionResult> {
    const scopes: CompactionScope[] = (await this.getCollections()).map((collection) => ({
      collection,
      position: compactedBefore,
      scannedEntries: 0,
      deletedEntries: 0
    }));
    // Shared by all scopes, so the memory bound does not depend on the number of parameter indexes.
    // It is safe for items to be evicted: that just changes deletes from "delete by _id" to
    // the more expensive "delete by range filter".
    const previousByIdentity = new LRUCache<string, CachedIdentity>({
      max: this.options.compactParameterCacheLimit ?? PARAMETER_COMPACTION_CACHE_SIZE
    });
    let persistedFrontier = compactedBefore;
    let lastPersistedAt = Date.now();

    while (true) {
      // Interrupting between batches is equivalent to a crash: deletes are idempotent, and the
      // cursor never covers a batch that did not complete.
      this.signal?.throwIfAborted();
      const { frontier, scope } = this.frontier(scopes);

      if (frontier > persistedFrontier && Date.now() - lastPersistedAt >= this.parameterCompactionPersistIntervalMs) {
        await this.persistCompactedBefore(frontier);
        persistedFrontier = frontier;
        lastPersistedAt = Date.now();
        this.logger.info(`Parameter compaction progress: ` + `cursor=${frontier}, target=${this.checkpoint}`);
      }

      if (scope == null) {
        // All scopes have been processed up to the target checkpoint.
        break;
      }

      // The scope on the frontier has processed the least, so taking its next batch is what keeps
      // every scope within one batch of the cursor.
      await this.compactBatch(scope, previousByIdentity);

      if (scope.position >= this.checkpoint && scope.scannedEntries > 0) {
        this.logger.info(
          `Parameter compaction completed for ${scope.collection.collectionName}: ` +
            `scanned=${scope.scannedEntries}, deleted=${scope.deletedEntries}`
        );
      }
    }

    return {
      collections: scopes.length,
      scannedEntries: scopes.reduce((total, scope) => total + scope.scannedEntries, 0),
      deletedEntries: scopes.reduce((total, scope) => total + scope.deletedEntries, 0)
    };
  }

  /**
   * The boundary that every scope has processed past, capped at the target checkpoint, and the
   * scope sitting on it.
   *
   * The frontier is the furthest the cursor may be advanced. The scope is the one that has
   * processed the least, or null once all of them have reached the target checkpoint.
   */
  private frontier(scopes: CompactionScope[]): { frontier: InternalOpId; scope: CompactionScope | null } {
    let frontier = this.checkpoint;
    let scope: CompactionScope | null = null;
    for (const candidate of scopes) {
      if (candidate.position < frontier) {
        frontier = candidate.position;
        scope = candidate;
      }
    }
    return { frontier, scope };
  }

  /**
   * Reads and processes one batch from the scope, and advances its position past that batch.
   */
  private async compactBatch(scope: CompactionScope, previousByIdentity: LRUCache<string, CachedIdentity>) {
    const batchStartedAt = Date.now();
    const collection = scope.collection;
    // Typed as Document: `_id` here is an InternalOpId (bigint), not the driver's default ObjectId.
    const filter: mongo.Document = { _id: { $gte: scope.position, $lt: this.checkpoint } };
    const batch = (await collection
      .find(filter, {
        sort: { _id: 1 },
        limit: this.parameterCompactionBatchSize,
        batchSize: this.parameterCompactionBatchSize + 1,
        projection: { _id: 1, key: 1, lookup: 1, bucket_parameters: { $slice: 1 } }
      })
      .toArray()) as unknown as ParameterCompactionReadDocument[];

    if (batch.length < this.parameterCompactionBatchSize) {
      // Fewer documents than we asked for: this collection has nothing left in the range.
      scope.position = this.checkpoint;
    } else {
      scope.position = batch.at(-1)!._id + 1n;
    }
    if (batch.length == 0) {
      return;
    }
    scope.scannedEntries += batch.length;
    const deletedBeforeBatch = scope.deletedEntries;

    // Keep the latest document for each identity and remove all earlier documents from this
    // batch by _id, avoiding a range query for documents that have already been read.
    const newestByIdentity = new Map<string, ParameterCompactionReadDocument>();
    const supersededIds: InternalOpId[] = [];
    for (const document of batch) {
      if (!this.shouldCompactDocument(document)) {
        continue;
      }
      const identity = identityKey(scope, document);
      const previous = newestByIdentity.get(identity);
      if (previous != null) {
        supersededIds.push(previous._id);
      }
      newestByIdentity.set(identity, document);
    }

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
    scope.deletedEntries += await this.deleteByIds(collection, supersededIds);

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
      // Safe to stop here: an interrupted batch leaves phase 3 tombstones in place, and the
      // remaining deletes are repeated by the next pass.
      this.signal?.throwIfAborted();
      await this.ensureInvalidationFence();
      const result = await collection.bulkWrite(deleteOperations, { ordered: false });
      scope.deletedEntries += result.deletedCount;
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
    scope.deletedEntries += await this.deleteByIds(collection, tombstoneIds);

    // Update the LRU only after all phases succeed. An evicted identity safely falls back to a
    // grouped leading-history delete when it appears again.
    for (const [identity, document] of newestByIdentity) {
      // Tombstones are recorded as `retainedId: null`: phases 2 and 3 removed the entire history
      // for the identity, including the tombstone, so a later sighting needs neither delete.
      previousByIdentity.set(identity, {
        retainedId: document.bucket_parameters?.length == 0 ? null : document._id
      });
    }

    const batchDurationSeconds = (Date.now() - batchStartedAt) / 1000;
    this.logger.info(
      `Compacted parameter batch in ${collection.collectionName}: ` +
        `_id ${batch[0]._id}..${batch.at(-1)!._id}, scanned=${batch.length} (${scope.scannedEntries} total), ` +
        `batchIdentities=${newestByIdentity.size}, exactIds=${supersededIds.length + tombstoneIds.length}, ` +
        `lookupGroups=${leadingHistoryDeletes.size}, deleted=${scope.deletedEntries - deletedBeforeBatch}, ` +
        `duration=${batchDurationSeconds.toFixed(1)}s`
    );
  }

  /** Deletes documents by `_id`, chunked to bound the command size. Returns the number deleted. */
  private async deleteByIds(collection: mongo.Collection<mongo.Document>, ids: InternalOpId[]): Promise<number> {
    let deletedEntries = 0;
    for (const idBatch of chunk(ids, PARAMETER_COMPACTION_DELETE_BATCH_SIZE)) {
      this.signal?.throwIfAborted();
      await this.ensureInvalidationFence();
      // Cast: `_id` here is an InternalOpId (bigint), not the driver's default ObjectId.
      const result = await collection.deleteMany({ _id: { $in: idBatch } } as any);
      deletedEntries += result.deletedCount;
    }
    return deletedEntries;
  }
}

/**
 * Identifies a (key, lookup) pair within one collection.
 *
 * The collection is part of the identity: V3 keeps the parameter index id in the collection name
 * rather than in the lookup, so the same (key, lookup) can appear in multiple collections meaning
 * different things. Deleting the history of one says nothing about the other.
 */
function identityKey(scope: CompactionScope, document: ParameterCompactionReadDocument): string {
  const serialized = bson.serialize({
    c: scope.collection.collectionName,
    k: document.key,
    l: document.lookup
  }) as Buffer;
  return serialized.toString('base64');
}

function* chunk<T>(items: T[], size: number): Iterable<T[]> {
  for (let offset = 0; offset < items.length; offset += size) {
    yield items.slice(offset, offset + size);
  }
}
