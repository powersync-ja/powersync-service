import * as lib_postgres from '@powersync/lib-service-postgres';
import { container, DisposableObserver, errors, logger } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';
import * as timers from 'timers/promises';
import * as t from 'ts-codec';
import { CurrentBucket, CurrentData, CurrentDataDecoded } from '../../types/models/CurrentData.js';
import { models, RequiredOperationBatchLimits } from '../../types/types.js';
import { NOTIFICATION_CHANNEL, sql } from '../../utils/db.js';
import { pick } from '../../utils/ts-codec.js';
import { batchCreateCustomWriteCheckpoints } from '../checkpoints/PostgresWriteCheckpointAPI.js';
import { cacheKey, encodedCacheKey, OperationBatch, RecordOperation } from './OperationBatch.js';
import { PostgresPersistedBatch } from './PostgresPersistedBatch.js';

export interface PostgresBucketBatchOptions {
  db: lib_postgres.DatabaseClient;
  sync_rules: sync_rules.SqlSyncRules;
  group_id: number;
  slot_name: string;
  last_checkpoint_lsn: string | null;
  no_checkpoint_before_lsn: string;
  store_current_data: boolean;
  keep_alive_op?: string | null;
  /**
   * Set to true for initial replication.
   */
  skip_existing_rows: boolean;
  batch_limits: RequiredOperationBatchLimits;
}

/**
 * Intermediate type which helps for only watching the active sync rules
 * via the Postgres NOTIFY protocol.
 */
const StatefulCheckpoint = models.ActiveCheckpoint.and(t.object({ state: t.Enum(storage.SyncRuleState) }));
type StatefulCheckpointDecoded = t.Decoded<typeof StatefulCheckpoint>;

// The limits here are not as strict as MongoDB
const MAX_ROW_SIZE = 40_000_000;

export class PostgresBucketBatch
  extends DisposableObserver<storage.BucketBatchStorageListener>
  implements storage.BucketStorageBatch
{
  public last_flushed_op: bigint | null = null;

  protected db: lib_postgres.DatabaseClient;
  protected group_id: number;
  protected last_checkpoint_lsn: string | null;
  protected no_checkpoint_before_lsn: string;

  protected persisted_op: bigint | null;

  protected write_checkpoint_batch: storage.CustomWriteCheckpointOptions[];
  protected readonly sync_rules: sync_rules.SqlSyncRules;
  protected batch: OperationBatch | null;
  private lastWaitingLogThrottled = 0;

  constructor(protected options: PostgresBucketBatchOptions) {
    super();
    this.db = options.db;
    this.group_id = options.group_id;
    this.last_checkpoint_lsn = options.last_checkpoint_lsn;
    this.no_checkpoint_before_lsn = options.no_checkpoint_before_lsn;
    this.write_checkpoint_batch = [];
    this.sync_rules = options.sync_rules;
    this.batch = null;
    this.persisted_op = null;
    if (options.keep_alive_op) {
      this.persisted_op = BigInt(options.keep_alive_op);
    }
  }

  get lastCheckpointLsn() {
    return this.last_checkpoint_lsn;
  }

  async save(record: storage.SaveOptions): Promise<storage.FlushedResult | null> {
    // TODO maybe share with abstract class
    const { after, afterReplicaId, before, beforeReplicaId, sourceTable, tag } = record;
    for (const event of this.getTableEvents(sourceTable)) {
      this.iterateListeners((cb) =>
        cb.replicationEvent?.({
          batch: this,
          table: sourceTable,
          data: {
            op: tag,
            after: after && utils.isCompleteRow(this.options.store_current_data, after) ? after : undefined,
            before: before && utils.isCompleteRow(this.options.store_current_data, before) ? before : undefined
          },
          event
        })
      );
    }
    /**
     * Return if the table is just an event table
     */
    if (!sourceTable.syncData && !sourceTable.syncParameters) {
      return null;
    }

    logger.debug(`Saving ${record.tag}:${record.before?.id}/${record.after?.id}`);

    if (!sourceTable.syncData && !sourceTable.syncParameters) {
      return null;
    }

    logger.debug(`Saving ${record.tag}:${record.before?.id}/${record.after?.id}`);

    this.batch ??= new OperationBatch(this.options.batch_limits);
    this.batch.push(new RecordOperation(record));

    if (this.batch.shouldFlush()) {
      const r = await this.flush();
      // HACK: Give other streams a  chance to also flush
      await timers.setTimeout(5);
      return r;
    }
    return null;
  }

  async truncate(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    await this.flush();

    let last_op: bigint | null = null;
    for (let table of sourceTables) {
      last_op = await this.truncateSingle(table);
    }

    if (last_op) {
      this.persisted_op = last_op;
    }

    return {
      flushed_op: String(last_op!)
    };
  }

  protected async truncateSingle(sourceTable: storage.SourceTable) {
    // To avoid too large transactions, we limit the amount of data we delete per transaction.
    // Since we don't use the record data here, we don't have explicit size limits per batch.
    const BATCH_LIMIT = 2000;
    let lastBatchCount = BATCH_LIMIT;
    let processedCount = 0;
    const codec = pick(models.CurrentData, ['buckets', 'lookups', 'source_key']);

    while (lastBatchCount == BATCH_LIMIT) {
      lastBatchCount = 0;
      await this.withReplicationTransaction(async (db) => {
        const persistedBatch = new PostgresPersistedBatch({
          group_id: this.group_id,
          ...this.options.batch_limits
        });

        for await (const rows of db.streamRows<t.Encoded<typeof codec>>(sql`
          SELECT
            buckets,
            lookups,
            source_key
          FROM
            current_data
          WHERE
            group_id = ${{ type: 'int8', value: this.group_id }}
            AND source_table = ${{ type: 'varchar', value: sourceTable.id }}
          LIMIT
            ${{ type: 'int4', value: BATCH_LIMIT }}
        `)) {
          lastBatchCount += rows.length;
          processedCount += rows.length;

          const decodedRows = rows.map((row) => codec.decode(row));
          for (const value of decodedRows) {
            persistedBatch.saveBucketData({
              before_buckets: value.buckets,
              evaluated: [],
              table: sourceTable,
              source_key: value.source_key
            });
            persistedBatch.saveParameterData({
              existing_lookups: value.lookups,
              evaluated: [],
              table: sourceTable,
              source_key: value.source_key
            });
            persistedBatch.deleteCurrentData({
              // This is serialized since we got it from a DB query
              serialized_source_key: value.source_key,
              source_table_id: sourceTable.id
            });
          }
        }
        await persistedBatch.flush(db);
      });
    }
    if (processedCount == 0) {
      // The op sequence should not have progressed
      return null;
    }

    const currentSequence = await this.db.sql`
      SELECT
        LAST_VALUE AS value
      FROM
        op_id_sequence;
    `.first<{ value: bigint }>();
    return currentSequence!.value;
  }

  async drop(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    await this.truncate(sourceTables);
    const result = await this.flush();

    await this.db.transaction(async (db) => {
      for (const table of sourceTables) {
        await db.sql`
          DELETE FROM source_tables
          WHERE
            id = ${{ type: 'varchar', value: table.id }}
        `.execute();
      }
    });
    return result;
  }

  async flush(): Promise<storage.FlushedResult | null> {
    let result: storage.FlushedResult | null = null;
    // One flush may be split over multiple transactions.
    // Each flushInner() is one transaction.
    while (this.batch != null) {
      let r = await this.flushInner();
      if (r) {
        result = r;
      }
    }
    await batchCreateCustomWriteCheckpoints(this.db, this.write_checkpoint_batch);
    this.write_checkpoint_batch = [];
    return result;
  }

  private async flushInner(): Promise<storage.FlushedResult | null> {
    const batch = this.batch;
    if (batch == null) {
      return null;
    }

    let resumeBatch: OperationBatch | null = null;

    const lastOp = await this.withReplicationTransaction(async (db) => {
      resumeBatch = await this.replicateBatch(db, batch);

      const sequence = await db.sql`
        SELECT
          LAST_VALUE AS value
        FROM
          op_id_sequence;
      `.first<{ value: bigint }>();
      return sequence!.value;
    });

    // null if done, set if we need another flush
    this.batch = resumeBatch;

    if (lastOp == null) {
      throw new Error('Unexpected last_op == null');
    }

    this.persisted_op = lastOp;
    this.last_flushed_op = lastOp;
    return { flushed_op: String(lastOp) };
  }

  async commit(lsn: string): Promise<boolean> {
    await this.flush();

    if (this.last_checkpoint_lsn != null && lsn < this.last_checkpoint_lsn) {
      // When re-applying transactions, don't create a new checkpoint until
      // we are past the last transaction.
      logger.info(`Re-applied transaction ${lsn} - skipping checkpoint`);
      return false;
    }

    if (lsn < this.no_checkpoint_before_lsn) {
      if (Date.now() - this.lastWaitingLogThrottled > 5_000) {
        logger.info(
          `Waiting until ${this.no_checkpoint_before_lsn} before creating checkpoint, currently at ${lsn}. Persisted op: ${this.persisted_op}`
        );
        this.lastWaitingLogThrottled = Date.now();
      }

      // Edge case: During initial replication, we have a no_checkpoint_before_lsn set,
      // and don't actually commit the snapshot.
      // The first commit can happen from an implicit keepalive message.
      // That needs the persisted_op to get an accurate checkpoint, so
      // we persist that in keepalive_op.

      await this.db.sql`
        UPDATE sync_rules
        SET
          keepalive_op = ${{ type: 'varchar', value: this.persisted_op == null ? null : String(this.persisted_op) }}
        WHERE
          id = ${{ type: 'int4', value: this.group_id }}
      `.execute();

      return false;
    }
    const now = new Date().toISOString();
    const update: Partial<models.SyncRules> = {
      last_checkpoint_lsn: lsn,
      last_checkpoint_ts: now,
      last_keepalive_ts: now,
      snapshot_done: true,
      last_fatal_error: null,
      keepalive_op: null
    };

    if (this.persisted_op != null) {
      update.last_checkpoint = this.persisted_op.toString();
    }

    const doc = await this.db.sql`
      UPDATE sync_rules
      SET
        keepalive_op = ${{ type: 'varchar', value: update.keepalive_op }},
        last_fatal_error = ${{ type: 'varchar', value: update.last_fatal_error }},
        snapshot_done = ${{ type: 'bool', value: update.snapshot_done }},
        last_keepalive_ts = ${{ type: 1184, value: update.last_keepalive_ts }},
        last_checkpoint = COALESCE(
          ${{ type: 'int8', value: update.last_checkpoint }},
          last_checkpoint
        ),
        last_checkpoint_ts = ${{ type: 1184, value: update.last_checkpoint_ts }},
        last_checkpoint_lsn = ${{ type: 'varchar', value: update.last_checkpoint_lsn }}
      WHERE
        id = ${{ type: 'int4', value: this.group_id }}
      RETURNING
        id,
        state,
        last_checkpoint,
        last_checkpoint_lsn
    `
      .decoded(StatefulCheckpoint)
      .first();

    await notifySyncRulesUpdate(this.db, doc!);

    this.persisted_op = null;
    this.last_checkpoint_lsn = lsn;
    return true;
  }

  async keepalive(lsn: string): Promise<boolean> {
    if (this.last_checkpoint_lsn != null && lsn <= this.last_checkpoint_lsn) {
      // No-op
      return false;
    }

    if (lsn < this.no_checkpoint_before_lsn) {
      return false;
    }

    if (this.persisted_op != null) {
      // The commit may have been skipped due to "no_checkpoint_before_lsn".
      // Apply it now if relevant
      logger.info(`Commit due to keepalive at ${lsn} / ${this.persisted_op}`);
      return await this.commit(lsn);
    }

    const updated = await this.db.sql`
      UPDATE sync_rules
      SET
        snapshot_done = ${{ type: 'bool', value: true }},
        last_checkpoint_lsn = ${{ type: 'varchar', value: lsn }},
        last_fatal_error = ${{ type: 'varchar', value: null }},
        last_keepalive_ts = ${{ type: 1184, value: new Date().toISOString() }}
      WHERE
        id = ${{ type: 'int8', value: this.group_id }}
      RETURNING
        id,
        state,
        last_checkpoint,
        last_checkpoint_lsn
    `
      .decoded(StatefulCheckpoint)
      .first();

    await notifySyncRulesUpdate(this.db, updated!);

    this.last_checkpoint_lsn = lsn;
    return true;
  }

  async markSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn: string
  ): Promise<storage.SourceTable[]> {
    const ids = tables.map((table) => table.id.toString());

    await this.db.transaction(async (db) => {
      await db.sql`
        UPDATE source_tables
        SET
          snapshot_done = ${{ type: 'bool', value: true }}
        WHERE
          id IN (
            SELECT
              (value ->> 0)::TEXT
            FROM
              jsonb_array_elements(${{ type: 'jsonb', value: ids }}) AS value
          );
      `.execute();

      if (no_checkpoint_before_lsn > this.no_checkpoint_before_lsn) {
        this.no_checkpoint_before_lsn = no_checkpoint_before_lsn;

        await db.sql`
          UPDATE sync_rules
          SET
            no_checkpoint_before = ${{ type: 'varchar', value: no_checkpoint_before_lsn }},
            last_keepalive_ts = ${{ type: 1184, value: new Date().toISOString() }}
          WHERE
            id = ${{ type: 'int8', value: this.group_id }}
        `.execute();
      }
    });
    return tables.map((table) => {
      const copy = new storage.SourceTable(
        table.id,
        table.connectionTag,
        table.objectId,
        table.schema,
        table.table,
        table.replicaIdColumns,
        table.snapshotComplete
      );
      copy.syncData = table.syncData;
      copy.syncParameters = table.syncParameters;
      return copy;
    });
  }

  addCustomWriteCheckpoint(checkpoint: storage.BatchedCustomWriteCheckpointOptions): void {
    this.write_checkpoint_batch.push({
      ...checkpoint,
      sync_rules_id: this.group_id
    });
  }

  protected async replicateBatch(db: lib_postgres.WrappedConnection, batch: OperationBatch) {
    let sizes: Map<string, number> | undefined = undefined;
    if (this.options.store_current_data && !this.options.skip_existing_rows) {
      // We skip this step if we don't store current_data, since the sizes will
      // always be small in that case.

      // With skipExistingRows, we don't load the full documents into memory,
      // so we can also skip the size lookup step.

      // Find sizes of current_data documents, to assist in intelligent batching without
      // exceeding memory limits.
      const sizeLookups = batch.batch.map((r) => {
        return {
          source_table: r.record.sourceTable.id.toString(),
          /**
           * Encode to hex in order to pass a jsonb
           */
          source_key: storage.serializeReplicaId(r.beforeId).toString('hex')
        };
      });

      sizes = new Map<string, number>();

      for await (const rows of db.streamRows<{
        source_table: string;
        source_key: storage.ReplicaId;
        data_size: number;
      }>(lib_postgres.sql`
        WITH
          filter_data AS (
            SELECT
              decode(FILTER ->> 'source_key', 'hex') AS source_key, -- Decoding from hex to bytea
              (FILTER ->> 'source_table') AS source_table_id
            FROM
              jsonb_array_elements(${{ type: 'jsonb', value: sizeLookups }}::jsonb) AS FILTER
          )
        SELECT
          pg_column_size(c.data) AS data_size,
          c.source_table,
          c.source_key
        FROM
          current_data c
          JOIN filter_data f ON c.source_table = f.source_table_id
          AND c.source_key = f.source_key
        WHERE
          c.group_id = ${{ type: 'int4', value: this.group_id }}
      `)) {
        for (const row of rows) {
          const key = cacheKey(row.source_table, row.source_key);
          sizes.set(key, row.data_size);
        }
      }
    }

    // If set, we need to start a new transaction with this batch.
    let resumeBatch: OperationBatch | null = null;

    // Now batch according to the sizes
    // This is a single batch if storeCurrentData == false
    for await (const b of batch.batched(sizes)) {
      if (resumeBatch) {
        // These operations need to be completed in a new transaction.
        for (let op of b) {
          resumeBatch.push(op);
        }
        continue;
      }

      const lookups = b.map((r) => {
        return {
          source_table: r.record.sourceTable.id,
          source_key: storage.serializeReplicaId(r.beforeId).toString('hex')
        };
      });

      const current_data_lookup = new Map<string, CurrentDataDecoded>();
      for await (const currentDataRows of db.streamRows<CurrentData>({
        statement: /* sql */ `
          WITH
            filter_data AS (
              SELECT
                decode(FILTER ->> 'source_key', 'hex') AS source_key, -- Decoding from hex to bytea
                (FILTER ->> 'source_table') AS source_table_id
              FROM
                jsonb_array_elements($1::jsonb) AS FILTER
            )
          SELECT
            --- With skipExistingRows, we only need to know whether or not the row exists.
            ${this.options.skip_existing_rows ? `c.source_table, c.source_key` : 'c.*'}
          FROM
            current_data c
            JOIN filter_data f ON c.source_table = f.source_table_id
            AND c.source_key = f.source_key
          WHERE
            c.group_id = $2
        `,
        params: [
          {
            type: 'jsonb',
            value: lookups
          },
          {
            type: 'int8',
            value: this.group_id
          }
        ]
      })) {
        for (const row of currentDataRows) {
          const decoded = this.options.skip_existing_rows
            ? pick(CurrentData, ['source_key', 'source_table']).decode(row)
            : CurrentData.decode(row);
          current_data_lookup.set(
            encodedCacheKey(decoded.source_table, decoded.source_key),
            decoded as CurrentDataDecoded
          );
        }
      }

      let persistedBatch: PostgresPersistedBatch | null = new PostgresPersistedBatch({
        group_id: this.group_id,
        ...this.options.batch_limits
      });

      for (const op of b) {
        // These operations need to be completed in a new transaction
        if (resumeBatch) {
          resumeBatch.push(op);
          continue;
        }

        const currentData = current_data_lookup.get(op.internalBeforeKey) ?? null;
        if (currentData != null) {
          // If it will be used again later, it will be set again using nextData below
          current_data_lookup.delete(op.internalBeforeKey);
        }
        const nextData = await this.saveOperation(persistedBatch!, op, currentData);
        if (nextData != null) {
          // Update our current_data and size cache
          current_data_lookup.set(op.internalAfterKey!, nextData);
          sizes?.set(op.internalAfterKey!, nextData.data.byteLength);
        }

        if (persistedBatch!.shouldFlushTransaction()) {
          await persistedBatch!.flush(db);
          // The operations stored in this batch will be processed in the `resumeBatch`
          persistedBatch = null;
          // Return the remaining entries for the next resume transaction
          resumeBatch = new OperationBatch(this.options.batch_limits);
        }
      }

      if (persistedBatch) {
        /**
         * The operations were less than the max size if here. Flush now.
         * `persistedBatch` will be `null` if the operations should be flushed in a new transaction.
         */
        await persistedBatch.flush(db);
      }
    }
    return resumeBatch;
  }

  protected async saveOperation(
    persistedBatch: PostgresPersistedBatch,
    operation: RecordOperation,
    currentData?: CurrentDataDecoded | null
  ) {
    const record = operation.record;
    // We store bytea colums for source keys
    const beforeId = operation.beforeId;
    const afterId = operation.afterId;
    let after = record.after;
    const sourceTable = record.sourceTable;

    let existingBuckets: CurrentBucket[] = [];
    let newBuckets: CurrentBucket[] = [];
    let existingLookups: Buffer[] = [];
    let newLookups: Buffer[] = [];

    if (this.options.skip_existing_rows) {
      if (record.tag == storage.SaveOperationTag.INSERT) {
        if (currentData != null) {
          // Initial replication, and we already have the record.
          // This may be a different version of the record, but streaming replication
          // will take care of that.
          // Skip the insert here.
          return null;
        }
      } else {
        throw new Error(`${record.tag} not supported with skipExistingRows: true`);
      }
    }

    if (record.tag == storage.SaveOperationTag.UPDATE) {
      const result = currentData;
      if (result == null) {
        // Not an error if we re-apply a transaction
        existingBuckets = [];
        existingLookups = [];
        // Log to help with debugging if there was a consistency issue
        if (this.options.store_current_data) {
          logger.warn(
            `Cannot find previous record for update on ${record.sourceTable.qualifiedName}: ${beforeId} / ${record.before?.id}`
          );
        }
      } else {
        existingBuckets = result.buckets;
        existingLookups = result.lookups;
        if (this.options.store_current_data) {
          const data = storage.deserializeBson(result.data) as sync_rules.SqliteRow;
          after = storage.mergeToast(after!, data);
        }
      }
    } else if (record.tag == storage.SaveOperationTag.DELETE) {
      const result = currentData;
      if (result == null) {
        // Not an error if we re-apply a transaction
        existingBuckets = [];
        existingLookups = [];
        // Log to help with debugging if there was a consistency issue
        if (this.options.store_current_data) {
          logger.warn(
            `Cannot find previous record for delete on ${record.sourceTable.qualifiedName}: ${beforeId} / ${record.before?.id}`
          );
        }
      } else {
        existingBuckets = result.buckets;
        existingLookups = result.lookups;
      }
    }

    let afterData: Buffer | undefined;
    if (afterId != null && !this.options.store_current_data) {
      afterData = storage.serializeBson({});
    } else if (afterId != null) {
      try {
        afterData = storage.serializeBson(after);
        if (afterData!.byteLength > MAX_ROW_SIZE) {
          throw new Error(`Row too large: ${afterData?.byteLength}`);
        }
      } catch (e) {
        // Replace with empty values, equivalent to TOAST values
        after = Object.fromEntries(
          Object.entries(after!).map(([key, value]) => {
            return [key, undefined];
          })
        );
        afterData = storage.serializeBson(after);

        container.reporter.captureMessage(
          `Data too big on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${e.message}`,
          {
            level: errors.ErrorSeverity.WARNING,
            metadata: {
              replication_slot: this.options.slot_name,
              table: record.sourceTable.qualifiedName
            }
          }
        );
      }
    }

    // 2. Save bucket data
    if (beforeId != null && (afterId == null || !storage.replicaIdEquals(beforeId, afterId))) {
      // Source ID updated
      if (sourceTable.syncData) {
        // Delete old record
        persistedBatch.saveBucketData({
          source_key: beforeId,
          table: sourceTable,
          before_buckets: existingBuckets,
          evaluated: []
        });
        // Clear this, so we don't also try to REMOVE for the new id
        existingBuckets = [];
      }

      if (sourceTable.syncParameters) {
        // Delete old parameters
        persistedBatch.saveParameterData({
          source_key: beforeId,
          table: sourceTable,
          evaluated: [],
          existing_lookups: existingLookups
        });
        existingLookups = [];
      }
    }

    // If we re-apply a transaction, we can end up with a partial row.
    //
    // We may end up with toasted values, which means the record is not quite valid.
    // However, it will be valid by the end of the transaction.
    //
    // In this case, we don't save the op, but we do save the current data.
    if (afterId && after && utils.isCompleteRow(this.options.store_current_data, after)) {
      // Insert or update
      if (sourceTable.syncData) {
        const { results: evaluated, errors: syncErrors } = this.sync_rules.evaluateRowWithErrors({
          record: after,
          sourceTable
        });

        for (const error of syncErrors) {
          container.reporter.captureMessage(
            `Failed to evaluate data query on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${error.error}`,
            {
              level: errors.ErrorSeverity.WARNING,
              metadata: {
                replication_slot: this.options.slot_name,
                table: record.sourceTable.qualifiedName
              }
            }
          );
          logger.error(
            `Failed to evaluate data query on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${error.error}`
          );
        }

        // Save new one
        persistedBatch.saveBucketData({
          source_key: afterId,
          evaluated,
          table: sourceTable,
          before_buckets: existingBuckets
        });

        newBuckets = evaluated.map((e) => {
          return {
            bucket: e.bucket,
            table: e.table,
            id: e.id
          };
        });
      }

      if (sourceTable.syncParameters) {
        // Parameters
        const { results: paramEvaluated, errors: paramErrors } = this.sync_rules.evaluateParameterRowWithErrors(
          sourceTable,
          after
        );

        for (let error of paramErrors) {
          container.reporter.captureMessage(
            `Failed to evaluate parameter query on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${error.error}`,
            {
              level: errors.ErrorSeverity.WARNING,
              metadata: {
                replication_slot: this.options.slot_name,
                table: record.sourceTable.qualifiedName
              }
            }
          );
          logger.error(
            `Failed to evaluate parameter query on ${record.sourceTable.qualifiedName}.${after.id}: ${error.error}`
          );
        }

        persistedBatch.saveParameterData({
          source_key: afterId,
          table: sourceTable,
          evaluated: paramEvaluated,
          existing_lookups: existingLookups
        });

        newLookups = paramEvaluated.map((p) => {
          return storage.serializeLookupBuffer(p.lookup);
        });
      }
    }

    let result: CurrentDataDecoded | null = null;

    // 5. TOAST: Update current data and bucket list.
    if (afterId) {
      // Insert or update
      result = {
        source_key: afterId,
        group_id: BigInt(this.group_id),
        data: afterData!,
        source_table: sourceTable.id,
        buckets: newBuckets,
        lookups: newLookups
      };
      persistedBatch.upsertCurrentData(result);
    }

    if (afterId == null || !storage.replicaIdEquals(beforeId, afterId)) {
      // Either a delete (afterId == null), or replaced the old replication id
      persistedBatch.deleteCurrentData({
        source_table_id: record.sourceTable.id,
        source_key: beforeId!
      });
    }

    return result;
  }

  /**
   * Gets relevant {@link SqlEventDescriptor}s for the given {@link SourceTable}
   * TODO maybe share this with an abstract class
   */
  protected getTableEvents(table: storage.SourceTable): sync_rules.SqlEventDescriptor[] {
    return this.sync_rules.event_descriptors.filter((evt) =>
      [...evt.getSourceTables()].some((sourceTable) => sourceTable.matches(table))
    );
  }

  protected async withReplicationTransaction<T>(
    callback: (tx: lib_postgres.WrappedConnection) => Promise<T>
  ): Promise<T> {
    try {
      return await this.db.transaction(async (db) => {
        return await callback(db);
      });
    } finally {
      await this.db.sql`
        UPDATE sync_rules
        SET
          last_keepalive_ts = ${{ type: 1184, value: new Date().toISOString() }}
        WHERE
          id = ${{ type: 'int8', value: this.group_id }}
      `.execute();
    }
  }
}

/**
 * Uses Postgres' NOTIFY functionality to update different processes when the
 * active checkpoint has been updated.
 */
export const notifySyncRulesUpdate = async (db: lib_postgres.DatabaseClient, update: StatefulCheckpointDecoded) => {
  if (update.state != storage.SyncRuleState.ACTIVE) {
    return;
  }

  await db.query({
    statement: `NOTIFY ${NOTIFICATION_CHANNEL}, '${models.ActiveCheckpointNotification.encode({ active_checkpoint: update })}'`
  });
};