import * as lib_postgres from '@powersync/lib-service-postgres';
import { logger as defaultLogger, Logger } from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import { LRUCache } from 'lru-cache';
import { sql } from '../utils/db.js';

/**
 * One `bucket_parameters` row, with just enough of it to decide what to delete.
 *
 * `bucket_parameters` itself is not read - only whether it is a tombstone.
 */
type ParameterCompactionRow = {
  id: InternalOpId;
  source_table: string;
  source_key: Uint8Array;
  lookup: Uint8Array;
  tombstone: boolean;
};

export type ParameterCompactionResult = {
  scannedEntries: number;
  deletedEntries: number;
};

const PARAMETER_COMPACTION_BATCH_SIZE = 10_000;
const PARAMETER_COMPACTION_DELETE_BATCH_SIZE = 1_000;
const PARAMETER_COMPACTION_CACHE_SIZE = 50_000;
/**
 * How often progress is persisted during a pass.
 *
 * Kept coarse: replication also updates the `sync_rules` row on every commit.
 */
const PARAMETER_COMPACTION_PERSIST_INTERVAL_MS = 60_000;

type CachedIdentity = {
  /**
   * The `id` of the row retained for this identity in a previous batch, or null if that row was a
   * tombstone - in which case it has been deleted along with all its history, and nothing remains
   * to delete for the identity.
   */
  retainedId: InternalOpId | null;
};

/** Identifies a row within a lookup: the source row that produced the parameter entry. */
type ParameterSourceKey = {
  source_table: string;
  /** Hex-encoded, for `json_to_recordset()`. */
  source_key: string;
};

type LeadingHistoryDelete = {
  lookup: Uint8Array;
  keys: ParameterSourceKey[];
};

/**
 * Compacts parameter lookup data (the `bucket_parameters` table).
 *
 * This is the Postgres counterpart of MongoParameterCompactor, and follows the same approach: a
 * per-stream compaction cursor is persisted, so a run only scans entries in the un-compacted
 * operation-id range, and within each batch only the newest entry per identity is retained.
 *
 * The two differences from MongoDB are both about what the boundaries protect:
 *
 * 1. All parameter indexes of a stream live in the single `bucket_parameters` table, so there is
 *    one scan to keep track of rather than one per index, and the cursor is just the position of
 *    that scan.
 * 2. The fence guards parameter *reads*, not checkpoint change detection. Postgres change detection
 *    always invalidates all parameter buckets (`getCheckpointChanges()`), so it never queries the
 *    `(lastCheckpoint, nextCheckpoint]` history that MongoDB's
 *    `checkpoint_changes_invalid_before` protects. What it lacks instead is MongoDB's
 *    snapshot-pinned parameter reads, so a checkpoint older than the compaction target could
 *    otherwise be served with incomplete parameter history - see {@link ensureReadFence}.
 *
 * For background, see the `/docs/storage/parameter-lookups.md` and
 * `/docs/storage/incremental-parameter-compaction.md` files.
 */
export class PostgresParameterCompactor {
  protected readonly logger: Logger;
  protected readonly signal?: AbortSignal;

  constructor(
    protected readonly db: lib_postgres.DatabaseClient,
    protected readonly group_id: number,
    protected readonly checkpoint: InternalOpId,
    protected readonly options: storage.CompactOptions,
    protected readonly parameterCompactionBatchSize = PARAMETER_COMPACTION_BATCH_SIZE,
    protected readonly parameterCompactionPersistIntervalMs = PARAMETER_COMPACTION_PERSIST_INTERVAL_MS
  ) {
    this.logger = options.logger ?? defaultLogger;
    this.signal = options.signal;
  }

  /**
   * Set once the read fence for this pass has been persisted. See {@link ensureReadFence}.
   */
  #readFencePersisted = false;

  async compact(): Promise<ParameterCompactionResult> {
    const startedAt = Date.now();
    this.signal?.throwIfAborted();
    const compactedBefore = await this.readCompactedBefore();
    this.logger.info(`Incrementally compacting parameters from ${compactedBefore} up to checkpoint ${this.checkpoint}`);

    const result = await this.compactRange(compactedBefore);

    // Persist only after the entire range has completed. This uses GREATEST so an overlapping
    // compactor cannot move the cursor backwards.
    await this.persistCompactedBefore(this.checkpoint);

    const durationSeconds = (Date.now() - startedAt) / 1000;
    this.logger.info(
      `Incremental parameter compaction completed: ` +
        `scanned=${result.scannedEntries}, deleted=${result.deletedEntries}, ` +
        `cursor=${compactedBefore}->${this.checkpoint}, ` +
        `fence=${this.#readFencePersisted ? this.checkpoint : 'unchanged'}, ` +
        `duration=${durationSeconds.toFixed(1)}s`
    );
    return result;
  }

  /**
   * The exclusive operation-id boundary through which this stream's parameter entries have all
   * been compacted.
   *
   * Clearing a stream does not have to reset this: `op_id_sequence` is never restarted, so entries
   * written after a clear are still above the persisted boundary.
   */
  protected async readCompactedBefore(): Promise<InternalOpId> {
    const row = await this.db.sql`
      SELECT
        parameter_compacted_before
      FROM
        sync_rules
      WHERE
        id = ${{ type: 'int4', value: this.group_id }}
    `.first<{ parameter_compacted_before: bigint | null }>();
    return row?.parameter_compacted_before == null ? 0n : BigInt(row.parameter_compacted_before);
  }

  protected async persistCompactedBefore(compactedBefore: InternalOpId): Promise<void> {
    await this.db.sql`
      UPDATE sync_rules
      SET
        parameter_compacted_before = GREATEST(
          COALESCE(parameter_compacted_before, 0),
          ${{
        type: 'int8',
        value: compactedBefore
      }}
        )
      WHERE
        id = ${{ type: 'int4', value: this.group_id }}
    `.execute();
  }

  /**
   * Raises the parameter read fence before the first delete of this pass.
   *
   * MongoDB evaluates parameter queries in a snapshot pinned to the checkpoint, so a pass that
   * deletes entries afterwards cannot affect a reader on an older checkpoint. Postgres has no
   * pinned snapshot: `getParameterSets()` only filters `id <= checkpoint`, so removing the entry
   * that was newest at an older checkpoint C leaves a reader at C with incomplete history.
   *
   * The fence records the boundary below which that history may be missing.
   * {@link PostgresSyncRulesStorage.getParameterSets} reads it in the same statement as the
   * parameter entries - one statement is one snapshot - and refuses to serve a checkpoint below it.
   * Committing the fence before the first delete is what makes that check sound: a snapshot that
   * observes a deletion also observes the fence.
   *
   * The fence is deliberately not the same value as the compaction cursor. If the pass fails
   * halfway, the fence only causes conservative rejection of stale checkpoints, while an advanced
   * cursor would skip deletion work that never completed.
   *
   * In steady state the fence equals the checkpoint readers are already on - compaction targets the
   * active checkpoint - so it rejects nothing. It is therefore raised for any pass that issues a
   * delete, without first establishing that the delete matches anything: distinguishing those would
   * cost a read per lookup group to save a rejection that only a lagging reader can hit.
   */
  private async ensureReadFence(): Promise<void> {
    if (this.#readFencePersisted) {
      return;
    }
    await this.db.sql`
      UPDATE sync_rules
      SET
        parameter_reads_invalid_before = GREATEST(
          COALESCE(parameter_reads_invalid_before, 0),
          ${{
        type: 'int8',
        value: this.checkpoint
      }}
        )
      WHERE
        id = ${{ type: 'int4', value: this.group_id }}
    `.execute();
    this.#readFencePersisted = true;
  }

  /**
   * Processes the stream's parameter entries from `compactedBefore` up to the target checkpoint,
   * one batch at a time.
   *
   * Interrupting between batches is equivalent to a crash: deletes are idempotent, and the cursor
   * never covers a batch that did not complete.
   */
  private async compactRange(compactedBefore: InternalOpId): Promise<ParameterCompactionResult> {
    // It is safe for items to be evicted: that just changes deletes from "delete by id" to the
    // more expensive "delete by range filter".
    const previousByIdentity = new LRUCache<string, CachedIdentity>({
      max: this.options.compactParameterCacheLimit ?? PARAMETER_COMPACTION_CACHE_SIZE
    });
    const result: ParameterCompactionResult = { scannedEntries: 0, deletedEntries: 0 };
    let position = compactedBefore;
    let persistedPosition = compactedBefore;
    let lastPersistedAt = Date.now();

    while (position < this.checkpoint) {
      this.signal?.throwIfAborted();
      position = await this.compactBatch(position, previousByIdentity, result);

      if (position > persistedPosition && Date.now() - lastPersistedAt >= this.parameterCompactionPersistIntervalMs) {
        await this.persistCompactedBefore(position);
        persistedPosition = position;
        lastPersistedAt = Date.now();
        this.logger.info(`Parameter compaction progress: cursor=${position}, target=${this.checkpoint}`);
      }
    }

    return result;
  }

  /**
   * Reads and processes one batch, and returns the position past that batch.
   */
  private async compactBatch(
    position: InternalOpId,
    previousByIdentity: LRUCache<string, CachedIdentity>,
    result: ParameterCompactionResult
  ): Promise<InternalOpId> {
    const batchStartedAt = Date.now();
    // The primary key on `id` provides the range scan and the ordering; `group_id` is a residual
    // filter, since no index covers it together with `id`. Other streams' entries in the range are
    // therefore scanned but not returned - the same trade-off the MongoDB V1 compactor makes, and
    // the reason a new stream seeds its cursor with the current sequence head.
    const batch = await this.db.queryRows<ParameterCompactionRow>(sql`
      SELECT
        id,
        source_table,
        source_key,
        lookup,
        bucket_parameters = '[]' AS tombstone
      FROM
        bucket_parameters
      WHERE
        group_id = ${{ type: 'int4', value: this.group_id }}
        AND id >= ${{ type: 'int8', value: position }}
        AND id < ${{ type: 'int8', value: this.checkpoint }}
      ORDER BY
        id ASC
      LIMIT
        ${{ type: 'int4', value: this.parameterCompactionBatchSize }}
    `);

    // The stream filter is part of the query, so a short batch means the range is exhausted.
    const nextPosition =
      batch.length < this.parameterCompactionBatchSize ? this.checkpoint : batch[batch.length - 1].id + 1n;
    if (batch.length == 0) {
      return nextPosition;
    }
    result.scannedEntries += batch.length;
    const deletedBeforeBatch = result.deletedEntries;

    // Keep the latest row for each identity and remove all earlier rows from this batch by id,
    // avoiding a range query for rows that have already been read.
    const newestByIdentity = new Map<string, ParameterCompactionRow>();
    const supersededIds: InternalOpId[] = [];
    for (const row of batch) {
      const identity = identityKey(row);
      const previous = newestByIdentity.get(identity);
      if (previous != null) {
        supersededIds.push(previous.id);
      }
      newestByIdentity.set(identity, row);
    }

    const leadingHistoryDeletes = new Map<string, LeadingHistoryDelete>();
    const tombstoneIds: InternalOpId[] = [];
    for (const [identity, row] of newestByIdentity) {
      const previous = previousByIdentity.get(identity);
      if (previous == null) {
        // Have not seen this (source row, lookup) before, or it has been evicted from the cache.
        // Delete the entire leading range.
        const lookupIdentity = hex(row.lookup);
        const key: ParameterSourceKey = { source_table: row.source_table, source_key: hex(row.source_key) };
        const existing = leadingHistoryDeletes.get(lookupIdentity);
        if (existing == null) {
          leadingHistoryDeletes.set(lookupIdentity, { lookup: row.lookup, keys: [key] });
        } else {
          existing.keys.push(key);
        }
      } else if (previous.retainedId != null) {
        // We have already deleted the leading range for this (source row, lookup). Only delete the
        // last remaining one by id. This is always fast.
        supersededIds.push(previous.retainedId);
      }

      if (row.tombstone) {
        tombstoneIds.push(row.id);
      }
    }

    // Phase 1: Delete rows read in this batch, plus retained rows from a prior batch.
    result.deletedEntries += await this.deleteByIds(supersededIds);

    // Phase 2: Delete leading history once per lookup group. The batch is read with
    // `id < checkpoint`, so this range is checkpoint-bounded.
    const deleteBefore = batch[0].id;
    // The deletes are pipelined into a single command: with high lookup cardinality there is a
    // group per identity, and a command per group would mean a round trip per identity.
    let deleteStatements: pgwire.Statement[] = [];
    let pendingKeys = 0;
    const flushDeleteStatements = async () => {
      if (deleteStatements.length == 0) {
        return;
      }
      // Safe to stop here: an interrupted batch leaves phase 3 tombstones in place, and the
      // remaining deletes are repeated by the next pass.
      result.deletedEntries += await this.executeDeletes(deleteStatements);
      deleteStatements = [];
      pendingKeys = 0;
    };
    for (const { lookup, keys } of leadingHistoryDeletes.values()) {
      for (const keyBatch of chunk(keys, PARAMETER_COMPACTION_DELETE_BATCH_SIZE)) {
        deleteStatements.push(this.leadingHistoryDeleteStatement(lookup, keyBatch, deleteBefore));
        // Bound the command size by the total number of keys it covers, not by the number of
        // statements: a single group may already cover the entire batch.
        pendingKeys += keyBatch.length;
        if (pendingKeys >= PARAMETER_COMPACTION_DELETE_BATCH_SIZE) {
          await flushDeleteStatements();
        }
      }
    }
    // Phase 3 requires all leading history to be deleted first.
    await flushDeleteStatements();

    // Phase 3: A tombstone is removed only after all preceding history has been removed.
    result.deletedEntries += await this.deleteByIds(tombstoneIds);

    // Update the LRU only after all phases succeed. An evicted identity safely falls back to a
    // grouped leading-history delete when it appears again.
    for (const [identity, row] of newestByIdentity) {
      // Tombstones are recorded as `retainedId: null`: phases 2 and 3 removed the entire history
      // for the identity, including the tombstone, so a later sighting needs neither delete.
      previousByIdentity.set(identity, { retainedId: row.tombstone ? null : row.id });
    }

    const batchDurationSeconds = (Date.now() - batchStartedAt) / 1000;
    this.logger.info(
      `Compacted parameter batch: ` +
        `id ${batch[0].id}..${batch[batch.length - 1].id}, scanned=${batch.length} ` +
        `(${result.scannedEntries} total), batchIdentities=${newestByIdentity.size}, ` +
        `exactIds=${supersededIds.length + tombstoneIds.length}, lookupGroups=${leadingHistoryDeletes.size}, ` +
        `deleted=${result.deletedEntries - deletedBeforeBatch}, duration=${batchDurationSeconds.toFixed(1)}s`
    );

    return nextPosition;
  }

  /** Deletes rows by `id`, chunked to bound the statement size. Returns the number deleted. */
  private async deleteByIds(ids: InternalOpId[]): Promise<number> {
    let deletedEntries = 0;
    for (const idBatch of chunk(ids, PARAMETER_COMPACTION_DELETE_BATCH_SIZE)) {
      deletedEntries += await this.executeDeletes([
        sql`
          DELETE FROM bucket_parameters
          WHERE
            group_id = ${{ type: 'int4', value: this.group_id }}
            AND id IN (
              SELECT
                deleted.id::int8
              FROM
                json_array_elements_text(${{ type: 'json', value: idBatch.map(String) }}::json) AS deleted (id)
            )
        `
      ]);
    }
    return deletedEntries;
  }

  /**
   * Deletes all history of the given source rows for a single lookup.
   *
   * Uses the `(group_id, lookup, id DESC)` index to narrow the stream, lookup and operation-id
   * range. The source row is not part of that index, so it is a residual predicate applied to every
   * row the range scan returns.
   *
   * That scan may therefore have to filter through many source rows for the same lookup, but the
   * cost is amortized: a single scan covers up to
   * {@link PARAMETER_COMPACTION_DELETE_BATCH_SIZE} keys, and identities seen again in a later batch
   * skip the scan entirely - they are deleted by `id`.
   */
  private leadingHistoryDeleteStatement(
    lookup: Uint8Array,
    keys: ParameterSourceKey[],
    before: InternalOpId
  ): pgwire.Statement {
    return sql`
      DELETE FROM bucket_parameters
      WHERE
        group_id = ${{ type: 'int4', value: this.group_id }}
        AND lookup = ${{ type: 'bytea', value: lookup }}
        AND id < ${{ type: 'int8', value: before }}
        AND (source_table, source_key) IN (
          SELECT
            k.source_table,
            decode(k.source_key, 'hex')
          FROM
            json_to_recordset(${{ type: 'json', value: keys }}::json) AS k (source_table text, source_key text)
        )
    `;
  }

  /** Runs delete statements in a single command, and returns the total number of rows deleted. */
  private async executeDeletes(statements: pgwire.Statement[]): Promise<number> {
    if (statements.length == 0) {
      return 0;
    }
    this.signal?.throwIfAborted();
    // Every delete of this pass goes through here, so this is the only place the fence has to be
    // committed. It is a separate statement, so it commits strictly before the deletes.
    await this.ensureReadFence();
    const result = await this.db.query(...statements);
    // `DatabaseClient.query()` prepends a `SET search_path` statement, which contributes 0.
    return result.results.reduce((total, sub) => total + deletedRowCount(sub.status), 0);
  }
}

/**
 * Identifies a (source row, lookup) pair.
 *
 * Hex encoding keeps the parts unambiguous: `source_table` is the only part that may contain the
 * separator.
 */
function identityKey(row: ParameterCompactionRow): string {
  return `${hex(row.lookup)}|${hex(row.source_key)}|${row.source_table}`;
}

function hex(value: Uint8Array): string {
  return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString('hex');
}

/** Number of rows affected, from a command tag such as `DELETE 5`. */
function deletedRowCount(status: string | null): number {
  const [tag, count] = status?.split(' ') ?? [];
  return tag == 'DELETE' ? Number(count) : 0;
}

function* chunk<T>(items: T[], size: number): Iterable<T[]> {
  for (let offset = 0; offset < items.length; offset += size) {
    yield items.slice(offset, offset + size);
  }
}
