import * as lib_postgres from '@powersync/lib-service-postgres';
import { DisposableObserver } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';
import * as uuid from 'uuid';
import { BIGINT_MAX } from '../types/codecs.js';
import { models, RequiredOperationBatchLimits } from '../types/types.js';
import { replicaIdToSubkey } from '../utils/bson.js';
import { mapOpEntry } from '../utils/bucket-data.js';

import { StatementParam } from '@powersync/service-jpgwire';
import { pick } from '../utils/ts-codec.js';
import { PostgresBucketBatch } from './batch/PostgresBucketBatch.js';
import { PostgresWriteCheckpointAPI } from './checkpoints/PostgresWriteCheckpointAPI.js';
import { PostgresBucketStorageFactory } from './PostgresBucketStorageFactory.js';
import { PostgresCompactor } from './PostgresCompactor.js';

export type PostgresSyncRulesStorageOptions = {
  factory: PostgresBucketStorageFactory;
  db: lib_postgres.DatabaseClient;
  sync_rules: storage.PersistedSyncRulesContent;
  write_checkpoint_mode?: storage.WriteCheckpointMode;
  batchLimits: RequiredOperationBatchLimits;
};

export class PostgresSyncRulesStorage
  extends DisposableObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  public readonly group_id: number;
  public readonly sync_rules: storage.PersistedSyncRulesContent;
  public readonly slot_name: string;
  public readonly factory: PostgresBucketStorageFactory;

  protected db: lib_postgres.DatabaseClient;
  protected writeCheckpointAPI: PostgresWriteCheckpointAPI;

  //   TODO we might be able to share this in an abstract class
  private parsedSyncRulesCache: { parsed: sync_rules.SqlSyncRules; options: storage.ParseSyncRulesOptions } | undefined;
  private checksumCache = new storage.ChecksumCache({
    fetchChecksums: (batch) => {
      return this.getChecksumsInternal(batch);
    }
  });

  constructor(protected options: PostgresSyncRulesStorageOptions) {
    super();
    this.group_id = options.sync_rules.id;
    this.db = options.db;
    this.sync_rules = options.sync_rules;
    this.slot_name = options.sync_rules.slot_name;
    this.factory = options.factory;

    this.writeCheckpointAPI = new PostgresWriteCheckpointAPI({
      db: this.db,
      mode: options.write_checkpoint_mode ?? storage.WriteCheckpointMode.MANAGED
    });
  }

  get writeCheckpointMode(): storage.WriteCheckpointMode {
    return this.writeCheckpointAPI.writeCheckpointMode;
  }

  //   TODO we might be able to share this in an abstract class
  getParsedSyncRules(options: storage.ParseSyncRulesOptions): sync_rules.SqlSyncRules {
    const { parsed, options: cachedOptions } = this.parsedSyncRulesCache ?? {};
    /**
     * Check if the cached sync rules, if present, had the same options.
     * Parse sync rules if the options are different or if there is no cached value.
     */
    if (!parsed || options.defaultSchema != cachedOptions?.defaultSchema) {
      this.parsedSyncRulesCache = { parsed: this.sync_rules.parsed(options).sync_rules, options };
    }

    return this.parsedSyncRulesCache!.parsed;
  }

  async reportError(e: any): Promise<void> {
    const message = String(e.message ?? 'Replication failure');
    await this.db.sql`
      UPDATE sync_rules
      SET
        last_fatal_error = ${{ type: 'varchar', value: message }}
      WHERE
        id = ${{ type: 'int8', value: this.group_id }};
    `.execute();
  }

  compact(options?: storage.CompactOptions): Promise<void> {
    return new PostgresCompactor(this.db, this.group_id, options).compact();
  }

  batchCreateCustomWriteCheckpoints(checkpoints: storage.BatchedCustomWriteCheckpointOptions[]): Promise<void> {
    return this.writeCheckpointAPI.batchCreateCustomWriteCheckpoints(
      checkpoints.map((c) => ({ ...c, sync_rules_id: this.group_id }))
    );
  }

  createCustomWriteCheckpoint(checkpoint: storage.BatchedCustomWriteCheckpointOptions): Promise<bigint> {
    return this.writeCheckpointAPI.createCustomWriteCheckpoint({
      ...checkpoint,
      sync_rules_id: this.group_id
    });
  }

  lastWriteCheckpoint(filters: storage.SyncStorageLastWriteCheckpointFilters): Promise<bigint | null> {
    return this.writeCheckpointAPI.lastWriteCheckpoint({
      ...filters,
      sync_rules_id: this.group_id
    });
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    return this.writeCheckpointAPI.setWriteCheckpointMode(mode);
  }

  createManagedWriteCheckpoint(checkpoint: storage.ManagedWriteCheckpointOptions): Promise<bigint> {
    return this.writeCheckpointAPI.createManagedWriteCheckpoint(checkpoint);
  }

  async getCheckpoint(): Promise<storage.ReplicationCheckpoint> {
    const checkpointRow = await this.db.sql`
      SELECT
        last_checkpoint,
        last_checkpoint_lsn
      FROM
        sync_rules
      WHERE
        id = ${{ type: 'int8', value: this.group_id }}
    `
      .decoded(pick(models.SyncRules, ['last_checkpoint', 'last_checkpoint_lsn']))
      .first();

    return {
      checkpoint: utils.timestampToOpId(checkpointRow?.last_checkpoint ?? 0n),
      lsn: checkpointRow?.last_checkpoint_lsn ?? null
    };
  }

  async resolveTable(options: storage.ResolveTableOptions): Promise<storage.ResolveTableResult> {
    const { group_id, connection_id, connection_tag, entity_descriptor } = options;

    const { schema, name: table, objectId, replicationColumns } = entity_descriptor;

    const columns = replicationColumns.map((column) => ({
      name: column.name,
      type: column.type,
      // The PGWire returns this as a BigInt. We want to store this as JSONB
      type_oid: typeof column.typeId !== 'undefined' ? Number(column.typeId) : column.typeId
    }));
    return this.db.transaction(async (db) => {
      let sourceTableRow = await db.sql`
        SELECT
          *
        FROM
          source_tables
        WHERE
          group_id = ${{ type: 'int8', value: group_id }}
          AND connection_id = ${{ type: 'int4', value: connection_id }}
          AND relation_id = ${{ type: 'varchar', value: objectId.toString() }}
          AND schema_name = ${{ type: 'varchar', value: schema }}
          AND table_name = ${{ type: 'varchar', value: table }}
          AND replica_id_columns = ${{ type: 'jsonb', value: columns }}
      `
        .decoded(models.SourceTable)
        .first();

      if (sourceTableRow == null) {
        const row = await db.sql`
          INSERT INTO
            source_tables (
              id,
              group_id,
              connection_id,
              relation_id,
              schema_name,
              table_name,
              replica_id_columns
            )
          VALUES
            (
              ${{ type: 'varchar', value: uuid.v4() }},
              ${{ type: 'int8', value: group_id }},
              ${{ type: 'int4', value: connection_id }},
              --- The objectId can be string | number, we store it as a string and decode when querying
              ${{ type: 'varchar', value: objectId.toString() }},
              ${{ type: 'varchar', value: schema }},
              ${{ type: 'varchar', value: table }},
              ${{ type: 'jsonb', value: columns }}
            )
          RETURNING
            *
        `
          .decoded(models.SourceTable)
          .first();
        sourceTableRow = row;
      }

      const sourceTable = new storage.SourceTable(
        sourceTableRow!.id,
        connection_tag,
        objectId,
        schema,
        table,
        replicationColumns,
        sourceTableRow!.snapshot_done ?? true
      );
      sourceTable.syncEvent = options.sync_rules.tableTriggersEvent(sourceTable);
      sourceTable.syncData = options.sync_rules.tableSyncsData(sourceTable);
      sourceTable.syncParameters = options.sync_rules.tableSyncsParameters(sourceTable);

      const truncatedTables = await db.sql`
        SELECT
          *
        FROM
          source_tables
        WHERE
          group_id = ${{ type: 'int8', value: group_id }}
          AND connection_id = ${{ type: 'int4', value: connection_id }}
          AND id != ${{ type: 'varchar', value: sourceTableRow!.id }}
          AND (
            relation_id = ${{ type: 'varchar', value: objectId.toString() }}
            OR (
              schema_name = ${{ type: 'varchar', value: schema }}
              AND table_name = ${{ type: 'varchar', value: table }}
            )
          )
      `
        .decoded(models.SourceTable)
        .rows();

      return {
        table: sourceTable,
        dropTables: truncatedTables.map(
          (doc) =>
            new storage.SourceTable(
              doc.id,
              connection_tag,
              doc.relation_id ?? 0,
              doc.schema_name,
              doc.table_name,
              doc.replica_id_columns?.map((c) => ({
                name: c.name,
                typeOid: c.typeId,
                type: c.type
              })) ?? [],
              doc.snapshot_done ?? true
            )
        )
      };
    });
  }

  async startBatch(
    options: storage.StartBatchOptions,
    callback: (batch: storage.BucketStorageBatch) => Promise<void>
  ): Promise<storage.FlushedResult | null> {
    const syncRules = await this.db.sql`
      SELECT
        last_checkpoint_lsn,
        no_checkpoint_before,
        keepalive_op
      FROM
        sync_rules
      WHERE
        id = ${{ type: 'int8', value: this.group_id }}
    `
      .decoded(pick(models.SyncRules, ['last_checkpoint_lsn', 'no_checkpoint_before', 'keepalive_op']))
      .first();

    const checkpoint_lsn = syncRules?.last_checkpoint_lsn ?? null;

    await using batch = new PostgresBucketBatch({
      db: this.db,
      sync_rules: this.sync_rules.parsed(options).sync_rules,
      group_id: this.group_id,
      slot_name: this.slot_name,
      last_checkpoint_lsn: checkpoint_lsn,
      keep_alive_op: syncRules?.keepalive_op,
      no_checkpoint_before_lsn: syncRules?.no_checkpoint_before ?? options.zeroLSN,
      store_current_data: options.storeCurrentData,
      skip_existing_rows: options.skipExistingRows ?? false,
      batch_limits: this.options.batchLimits
    });
    this.iterateListeners((cb) => cb.batchStarted?.(batch));

    await callback(batch);
    await batch.flush();
    if (batch.last_flushed_op) {
      return { flushed_op: String(batch.last_flushed_op) };
    } else {
      return null;
    }
  }

  async getParameterSets(
    checkpoint: utils.OpId,
    lookups: sync_rules.SqliteJsonValue[][]
  ): Promise<sync_rules.SqliteJsonRow[]> {
    const rows = await this.db.sql`
      SELECT DISTINCT
        ON (lookup, source_table, source_key) lookup,
        source_table,
        source_key,
        id,
        bucket_parameters
      FROM
        bucket_parameters
      WHERE
        group_id = ${{ type: 'int8', value: this.group_id }}
        AND lookup = ANY (
          SELECT
            decode((FILTER ->> 0)::text, 'hex') -- Decode the hex string to bytea
          FROM
            jsonb_array_elements(${{
        type: 'jsonb',
        value: lookups.map((l) => storage.serializeLookupBuffer(l).toString('hex'))
      }}) AS FILTER
        )
        AND id <= ${{ type: 'int8', value: BigInt(checkpoint) }}
      ORDER BY
        lookup,
        source_table,
        source_key,
        id DESC
    `
      .decoded(pick(models.BucketParameters, ['bucket_parameters']))
      .rows();

    const groupedParameters = rows.map((row) => {
      return row.bucket_parameters;
    });
    return groupedParameters.flat();
  }

  async *getBucketDataBatch(
    checkpoint: utils.OpId,
    dataBuckets: Map<string, string>,
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataBatch> {
    if (dataBuckets.size == 0) {
      return;
    }

    const end = checkpoint ?? BIGINT_MAX;
    const filters = Array.from(dataBuckets.entries()).map(([name, start]) => ({
      bucket_name: name,
      start: start
    }));

    const rowLimit = options?.limit ?? storage.DEFAULT_DOCUMENT_BATCH_LIMIT;
    const sizeLimit = options?.chunkLimitBytes ?? storage.DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES;

    let batchSize = 0;
    let currentBatch: utils.SyncBucketData | null = null;
    let targetOp: bigint | null = null;
    let rowCount = 0;

    /**
     * It is possible to perform this query with JSONB join. e.g.
     * ```sql
     * WITH
     * filter_data AS (
     * SELECT
     * FILTER ->> 'bucket_name' AS bucket_name,
     * (FILTER ->> 'start')::BIGINT AS start_op_id
     * FROM
     * jsonb_array_elements($1::jsonb) AS FILTER
     * )
     * SELECT
     * b.*,
     * octet_length(b.data) AS data_size
     * FROM
     * bucket_data b
     * JOIN filter_data f ON b.bucket_name = f.bucket_name
     * AND b.op_id > f.start_op_id
     * AND b.op_id <= $2
     * WHERE
     * b.group_id = $3
     * ORDER BY
     * b.bucket_name ASC,
     * b.op_id ASC
     * LIMIT
     * $4;
     * ```
     * Which might be better for large volumes of buckets, but in testing the JSON method
     * was significantly slower than the method below. Syncing 2.5 million rows in a single
     * bucket takes 2 minutes and 11 seconds with the method below. With the JSON method
     * 1 million rows were only synced before a 5 minute timeout.
     */
    for await (const rows of this.db.streamRows({
      statement: `
          SELECT
            *
          FROM
            bucket_data 
          WHERE
            group_id = $1
            and op_id <= $2
            and (
            ${filters.map((f, index) => `(bucket_name = $${index * 2 + 4} and op_id > $${index * 2 + 5})`).join(' OR ')}
            ) 
          ORDER BY
            bucket_name ASC,
            op_id ASC
          LIMIT
            $3;`,
      params: [
        { type: 'int8', value: this.group_id },
        { type: 'int8', value: end },
        { type: 'int4', value: rowLimit + 1 },
        ...filters.flatMap((f) => [
          { type: 'varchar' as const, value: f.bucket_name },
          { type: 'int8' as const, value: f.start } satisfies StatementParam
        ])
      ]
    })) {
      const decodedRows = rows.map((r) => models.BucketData.decode(r as any));

      for (const row of decodedRows) {
        const { bucket_name } = row;
        const rowSize = row.data ? row.data.length : 0;

        if (
          currentBatch == null ||
          currentBatch.bucket != bucket_name ||
          batchSize >= sizeLimit ||
          (currentBatch?.data.length && batchSize + rowSize > sizeLimit) ||
          currentBatch.data.length >= rowLimit
        ) {
          let start: string | undefined = undefined;
          if (currentBatch != null) {
            if (currentBatch.bucket == bucket_name) {
              currentBatch.has_more = true;
            }

            const yieldBatch = currentBatch;
            start = currentBatch.after;
            currentBatch = null;
            batchSize = 0;
            yield { batch: yieldBatch, targetOp: targetOp };
            targetOp = null;
            if (rowCount >= rowLimit) {
              // We've yielded all the requested rows
              break;
            }
          }

          start ??= dataBuckets.get(bucket_name);
          if (start == null) {
            throw new Error(`data for unexpected bucket: ${bucket_name}`);
          }
          currentBatch = {
            bucket: bucket_name,
            after: start,
            has_more: false,
            data: [],
            next_after: start
          };
          targetOp = null;
        }

        const entry = mapOpEntry(row);

        if (row.source_table && row.source_key) {
          entry.subkey = replicaIdToSubkey(row.source_table, storage.deserializeReplicaId(row.source_key));
        }

        if (row.target_op != null) {
          // MOVE, CLEAR
          const rowTargetOp = row.target_op;
          if (targetOp == null || rowTargetOp > targetOp) {
            targetOp = rowTargetOp;
          }
        }

        currentBatch.data.push(entry);
        currentBatch.next_after = entry.op_id;

        batchSize += rowSize;

        // Manually track the total rows yielded
        rowCount++;
      }
    }

    if (currentBatch != null) {
      const yieldBatch = currentBatch;
      currentBatch = null;
      yield { batch: yieldBatch, targetOp: targetOp };
      targetOp = null;
    }
  }

  async getChecksums(checkpoint: utils.OpId, buckets: string[]): Promise<utils.ChecksumMap> {
    return this.checksumCache.getChecksumMap(checkpoint, buckets);
  }

  async terminate(options?: storage.TerminateOptions) {
    if (!options || options?.clearStorage) {
      await this.clear();
    }
    await this.db.sql`
      UPDATE sync_rules
      SET
        state = ${{ type: 'varchar', value: storage.SyncRuleState.TERMINATED }},
        snapshot_done = ${{ type: 'bool', value: false }}
      WHERE
        id = ${{ type: 'int8', value: this.group_id }}
    `.execute();
  }

  async getStatus(): Promise<storage.SyncRuleStatus> {
    const syncRulesRow = await this.db.sql`
      SELECT
        snapshot_done,
        last_checkpoint_lsn,
        state
      FROM
        sync_rules
      WHERE
        id = ${{ type: 'int8', value: this.group_id }}
    `
      .decoded(pick(models.SyncRules, ['snapshot_done', 'last_checkpoint_lsn', 'state']))
      .first();

    if (syncRulesRow == null) {
      throw new Error('Cannot find sync rules status');
    }

    return {
      snapshot_done: syncRulesRow.snapshot_done,
      active: syncRulesRow.state == storage.SyncRuleState.ACTIVE,
      checkpoint_lsn: syncRulesRow.last_checkpoint_lsn ?? null
    };
  }

  async clear(): Promise<void> {
    await this.db.sql`
      UPDATE sync_rules
      SET
        snapshot_done = FALSE,
        last_checkpoint_lsn = NULL,
        last_checkpoint = NULL,
        no_checkpoint_before = NULL
      WHERE
        id = ${{ type: 'int8', value: this.group_id }}
    `.execute();

    await this.db.sql`
      DELETE FROM bucket_data
      WHERE
        group_id = ${{ type: 'int8', value: this.group_id }}
    `.execute();

    await this.db.sql`
      DELETE FROM bucket_parameters
      WHERE
        group_id = ${{ type: 'int8', value: this.group_id }}
    `.execute();

    await this.db.sql`
      DELETE FROM current_data
      WHERE
        group_id = ${{ type: 'int8', value: this.group_id }}
    `.execute();

    await this.db.sql`
      DELETE FROM source_tables
      WHERE
        group_id = ${{ type: 'int8', value: this.group_id }}
    `.execute();
  }

  async autoActivate(): Promise<void> {
    await this.db.transaction(async (db) => {
      const syncRulesRow = await db.sql`
        SELECT
          state
        FROM
          sync_rules
        WHERE
          id = ${{ type: 'int8', value: this.group_id }}
      `
        .decoded(pick(models.SyncRules, ['state']))
        .first();

      if (syncRulesRow && syncRulesRow.state == storage.SyncRuleState.PROCESSING) {
        await db.sql`
          UPDATE sync_rules
          SET
            state = ${{ type: 'varchar', value: storage.SyncRuleState.ACTIVE }}
          WHERE
            id = ${{ type: 'int8', value: this.group_id }}
        `.execute();
      }

      await db.sql`
        UPDATE sync_rules
        SET
          state = ${{ type: 'varchar', value: storage.SyncRuleState.STOP }}
        WHERE
          state = ${{ type: 'varchar', value: storage.SyncRuleState.ACTIVE }}
          AND id != ${{ type: 'int8', value: this.group_id }}
      `.execute();
    });
  }

  private async getChecksumsInternal(batch: storage.FetchPartialBucketChecksum[]): Promise<storage.PartialChecksumMap> {
    if (batch.length == 0) {
      return new Map();
    }

    const rangedBatch = batch.map((b) => ({
      ...b,
      start: b.start ?? 0
    }));

    const results = await this.db.sql`
      WITH
        filter_data AS (
          SELECT
            FILTER ->> 'bucket' AS bucket_name,
            (FILTER ->> 'start')::BIGINT AS start_op_id,
            (FILTER ->> 'end')::BIGINT AS end_op_id
          FROM
            jsonb_array_elements(${{ type: 'jsonb', value: rangedBatch }}::jsonb) AS FILTER
        )
      SELECT
        b.bucket_name AS bucket,
        SUM(b.checksum) AS checksum_total,
        COUNT(*) AS total,
        MAX(
          CASE
            WHEN b.op = 'CLEAR' THEN 1
            ELSE 0
          END
        ) AS has_clear_op
      FROM
        bucket_data b
        JOIN filter_data f ON b.bucket_name = f.bucket_name
        AND b.op_id > f.start_op_id
        AND b.op_id <= f.end_op_id
      WHERE
        b.group_id = ${{ type: 'int8', value: this.group_id }}
      GROUP BY
        b.bucket_name;
    `.rows<{ bucket: string; checksum_total: bigint; total: bigint; has_clear_op: number }>();

    return new Map<string, storage.PartialChecksum>(
      results.map((doc) => {
        return [
          doc.bucket,
          {
            bucket: doc.bucket,
            partialCount: Number(doc.total),
            partialChecksum: Number(BigInt(doc.checksum_total) & 0xffffffffn) & 0xffffffff,
            isFullChecksum: doc.has_clear_op == 1
          } satisfies storage.PartialChecksum
        ];
      })
    );
  }
}
