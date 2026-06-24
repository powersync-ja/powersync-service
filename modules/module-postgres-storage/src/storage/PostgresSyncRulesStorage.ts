import * as lib_postgres from '@powersync/lib-service-postgres';
import {
  BroadcastIterable,
  BucketChecksum,
  CHECKPOINT_INVALIDATE_ALL,
  CheckpointChanges,
  GetCheckpointChangesOptions,
  InternalOpId,
  internalToExternalOpId,
  LastValueSink,
  maxLsn,
  ParameterSetLimitExceededError,
  PartialChecksum,
  PopulateChecksumCacheOptions,
  PopulateChecksumCacheResults,
  ReplicationCheckpoint,
  storage,
  StorageVersionConfig,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import * as sync_rules from '@powersync/service-sync-rules';
import * as timers from 'timers/promises';
import { bigint, BIGINT_MAX } from '../types/codecs.js';
import { models, RequiredOperationBatchLimits } from '../types/types.js';
import { replicaIdToSubkey } from '../utils/bson.js';
import { mapOpEntry } from '../utils/bucket-data.js';

import * as framework from '@powersync/lib-services-framework';
import { StatementParam } from '@powersync/service-jpgwire';
import { wrapWithAbort } from 'ix/asynciterable/operators/withabort.js';
import * as t from 'ts-codec';
import { pick } from '../utils/ts-codec.js';
import { PostgresBucketBatch } from './batch/PostgresBucketBatch.js';
import { PostgresWriteCheckpointAPI } from './checkpoints/PostgresWriteCheckpointAPI.js';
import { PostgresCurrentDataStore } from './current-data-store.js';
import { PostgresBucketStorageFactory } from './PostgresBucketStorageFactory.js';
import { PostgresCompactor } from './PostgresCompactor.js';

/** Postgres SQLSTATE raised when a statement is cancelled, e.g. by statement_timeout. */
const POSTGRES_QUERY_CANCELED = '57014';

export type PostgresSyncRulesStorageOptions = {
  factory: PostgresBucketStorageFactory;
  db: lib_postgres.DatabaseClient;
  replicationStream: storage.PersistedReplicationStream;
  write_checkpoint_mode?: storage.WriteCheckpointMode;
  batchLimits: RequiredOperationBatchLimits;
};

export class PostgresSyncRulesStorage
  extends framework.BaseObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  [framework.DO_NOT_LOG] = true;

  public readonly replicationStreamId: number;
  public readonly replicationStream: storage.PersistedReplicationStream;
  public readonly sync_rules: storage.PersistedSyncConfigContent;
  public readonly replicationStreamName: string;
  public readonly factory: PostgresBucketStorageFactory;
  public readonly storageConfig: StorageVersionConfig;
  public readonly logger: framework.Logger;

  private sharedIterator = new BroadcastIterable((signal) => this.watchActiveCheckpoint(signal));

  protected db: lib_postgres.DatabaseClient;
  protected writeCheckpointAPI: PostgresWriteCheckpointAPI;
  private readonly currentDataStore: PostgresCurrentDataStore;

  /**
   * Canonical parsed sync config sets, keyed by defaultSchema. Entries are never evicted,
   * so each parse options value maps to exactly one parsed set for the lifetime of this
   * storage instance.
   *
   * TODO we might be able to share this in an abstract class
   */
  private readonly parsedSyncConfigSets = new Map<string, storage.ParsedSyncConfigSet>();
  private _checksumCache: storage.ChecksumCache | undefined;

  constructor(protected options: PostgresSyncRulesStorageOptions) {
    super();
    this.replicationStream = options.replicationStream;
    this.replicationStreamId = options.replicationStream.replicationStreamId;
    this.db = options.db;
    this.sync_rules = options.replicationStream.syncConfigContent[0];
    this.replicationStreamName = options.replicationStream.replicationStreamName;
    this.factory = options.factory;
    this.storageConfig = options.replicationStream.getStorageConfig();
    this.currentDataStore = new PostgresCurrentDataStore(this.storageConfig);
    this.logger = options.replicationStream.logger;

    this.writeCheckpointAPI = new PostgresWriteCheckpointAPI({
      db: this.db,
      mode: options.write_checkpoint_mode ?? storage.WriteCheckpointMode.MANAGED
    });
  }

  /**
   * Lazy-instantiated cache.
   *
   * This means the cache only allocates memory once it is used for the first time.
   */
  private get checksumCache(): storage.ChecksumCache {
    this._checksumCache ??= new storage.ChecksumCache({
      fetchChecksums: (batch) => {
        return this.getChecksumsInternal(batch);
      }
    });
    return this._checksumCache;
  }

  get writeCheckpointMode(): storage.WriteCheckpointMode {
    return this.writeCheckpointAPI.writeCheckpointMode;
  }

  getParsedSyncConfigSet(options: storage.ParseSyncConfigOptions): storage.ParsedSyncConfigSet {
    let parsed = this.parsedSyncConfigSets.get(options.defaultSchema);
    if (parsed == null) {
      parsed = this.replicationStream.parsed(options);
      this.parsedSyncConfigSets.set(options.defaultSchema, parsed);
    }
    return parsed;
  }

  getParsedSyncRules(options: storage.ParseSyncConfigOptions): sync_rules.HydratedSyncConfig {
    return this.getParsedSyncConfigSet(options).hydratedSyncConfig;
  }

  async reportError(e: any): Promise<void> {
    const message = String(e.message ?? 'Replication failure');
    await this.db.sql`
      UPDATE sync_rules
      SET
        last_fatal_error = ${{ type: 'varchar', value: message }}
      WHERE
        id = ${{ type: 'int4', value: this.replicationStreamId }};
    `.execute();
  }

  async compact(options?: storage.CompactOptions): Promise<void> {
    let maxOpId = options?.maxOpId;
    if (maxOpId == null) {
      const checkpoint = await this.getCheckpoint();
      // Note: If there is no active checkpoint, this will be 0, in which case no compacting is performed
      maxOpId = checkpoint.checkpoint;
    }

    return new PostgresCompactor(this.db, this.replicationStreamId, {
      ...options,
      maxOpId,
      logger: this.logger
    }).compact();
  }

  async getBucketReport(options?: storage.GetBucketReportOptions): Promise<storage.BucketReport> {
    try {
      return await this.collectBucketReport(options);
    } catch (e) {
      // statement_timeout cancels the query with SQLSTATE 57014 (query_canceled). Translate it into a
      // friendly "query timed out" error instead of a raw 500.
      if (e?.cause?.code === POSTGRES_QUERY_CANCELED) {
        throw new framework.DatabaseQueryError(
          framework.ErrorCode.PSYNC_S2501,
          'Query timed out while building the bucket report',
          e
        );
      }
      throw e;
    }
  }

  private async collectBucketReport(options?: storage.GetBucketReportOptions): Promise<storage.BucketReport> {
    // Both queries scan storage (Postgres has no pre-aggregated bucket state), so they run in a transaction
    // with a statement timeout rather than letting an admin request run unbounded on a large instance.
    const { operationStats, rowCounts } = await this.db.transaction(async (db) => {
      await db.query(`SET LOCAL statement_timeout = ${storage.BUCKET_REPORT_TIMEOUT_MS}`);

      // Operations + operation-history bytes per bucket.
      const operationRows = await db.sql`
        SELECT
          bucket_name,
          COUNT(*)::BIGINT AS operations,
          COALESCE(SUM(OCTET_LENGTH(data)), 0)::BIGINT AS operation_bytes
        FROM
          bucket_data
        WHERE
          group_id = ${{ type: 'int4', value: this.replicationStreamId }}
        GROUP BY
          bucket_name;
      `.rows<{ bucket_name: string; operations: bigint; operation_bytes: bigint }>();

      const operationStats = new Map<string, storage.BucketOperationStat>(
        operationRows.map((row) => [
          row.bucket_name,
          { operations: Number(row.operations), operationBytes: Number(row.operation_bytes) }
        ])
      );

      // Distinct live rows per bucket, from each row's bucket memberships. The current-data table is
      // version-specific (current_data for v1/v2, v3_current_data for v3), so the table name is interpolated
      // from the resolved store rather than parameterised.
      const rowCounts = new Map<string, number>();
      for await (const batch of db.streamRows<{ bucket: string; rows: bigint }>({
        statement: `
          SELECT
            elem ->> 'bucket' AS bucket,
            COUNT(*)::BIGINT AS rows
          FROM
            ${this.currentDataStore.table} cd,
            jsonb_array_elements(cd.buckets) AS elem
          WHERE
            cd.group_id = $1
          GROUP BY
            elem ->> 'bucket'
        `,
        params: [{ type: 'int4', value: this.replicationStreamId }]
      })) {
        for (const row of batch) {
          rowCounts.set(row.bucket, Number(row.rows));
        }
      }

      return { operationStats, rowCounts };
    });

    return storage.buildBucketReport(operationStats, rowCounts, options);
  }

  async populatePersistentChecksumCache(_options: PopulateChecksumCacheOptions): Promise<PopulateChecksumCacheResults> {
    // no-op - checksum cache is not implemented for Postgres yet
    return { buckets: 0 };
  }

  lastWriteCheckpoint(filters: storage.SyncStorageLastWriteCheckpointFilters): Promise<bigint | null> {
    return this.writeCheckpointAPI.lastWriteCheckpoint({
      ...filters,
      sync_rules_id: this.replicationStreamId
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
        id = ${{ type: 'int4', value: this.replicationStreamId }}
    `
      .decoded(pick(models.SyncRules, ['last_checkpoint', 'last_checkpoint_lsn']))
      .first();

    return new PostgresReplicationCheckpoint(
      this,
      checkpointRow?.last_checkpoint ?? 0n,
      checkpointRow?.last_checkpoint_lsn ?? null
    );
  }

  async createWriter(options: storage.CreateWriterOptions): Promise<storage.BucketStorageBatch> {
    const syncRules = await this.db.sql`
      SELECT
        last_checkpoint_lsn,
        no_checkpoint_before,
        keepalive_op,
        snapshot_lsn
      FROM
        sync_rules
      WHERE
        id = ${{ type: 'int4', value: this.replicationStreamId }}
    `
      .decoded(pick(models.SyncRules, ['last_checkpoint_lsn', 'no_checkpoint_before', 'keepalive_op', 'snapshot_lsn']))
      .first();

    const checkpoint_lsn = syncRules?.last_checkpoint_lsn ?? null;

    const writer = new PostgresBucketBatch({
      logger: options.logger ?? this.logger,
      db: this.db,
      sync_rules: this.getParsedSyncRules(options),
      replicationStreamId: this.replicationStreamId,
      replicationStreamName: this.replicationStreamName,
      last_checkpoint_lsn: checkpoint_lsn,
      keep_alive_op: syncRules?.keepalive_op,
      resumeFromLsn: maxLsn(syncRules?.snapshot_lsn, checkpoint_lsn),
      store_current_data: options.storeCurrentData,
      skip_existing_rows: options.skipExistingRows ?? false,
      batch_limits: this.options.batchLimits,
      markRecordUnavailable: options.markRecordUnavailable,
      hooks: options.hooks,
      storageConfig: this.storageConfig
    });
    this.iterateListeners((cb) => cb.batchStarted?.(writer));
    return writer;
  }

  /**
   * @deprecated Use `createWriter()` with `await using` instead.
   */
  async startBatch(
    options: storage.CreateWriterOptions,
    callback: (batch: storage.BucketStorageBatch) => Promise<void>
  ): Promise<storage.FlushedResult | null> {
    await using writer = await this.createWriter(options);
    await callback(writer);
    await writer.flush();
    return writer.last_flushed_op != null ? { flushed_op: writer.last_flushed_op } : null;
  }

  async getParameterSets(
    checkpoint: ReplicationCheckpoint,
    lookups: sync_rules.ScopedParameterLookup[],
    limit: number
  ): Promise<sync_rules.ParameterLookupRows[]> {
    const rows = await this.db.sql`
      WITH
        rows AS (
          SELECT DISTINCT
            ON (lookup, source_table, source_key) requested.index - 1 AS index,
            bucket_parameters
          FROM
            bucket_parameters,
            jsonb_array_elements(${{
        type: 'jsonb',
        value: lookups.map((l) => storage.serializeLookupBuffer(l).toString('hex'))
      }}) WITH ORDINALITY AS requested (value, index)
          WHERE
            group_id = ${{ type: 'int4', value: this.replicationStreamId }}
            AND lookup = decode((requested.value ->> 0)::text, 'hex') -- Decode the hex string to bytea
            AND id <= ${{ type: 'int8', value: checkpoint.checkpoint }}
          ORDER BY
            lookup,
            source_table,
            source_key,
            id DESC
        )
      SELECT
        index,
        bucket_parameters
      FROM
        rows
      WHERE
        bucket_parameters != '[]'
      LIMIT
        ${{ type: 'int4', value: limit + 1 }}
    `
      .decoded(parameterSetsRow)
      .rows();

    let totalRows = 0;
    const resultsByLookup = new Map<sync_rules.ScopedParameterLookup, sync_rules.SqliteJsonRow[]>();
    for (const row of rows) {
      const parameterRows = JSONBig.parse(row.bucket_parameters) as sync_rules.SqliteJsonRow[];
      const lookup = lookups[Number(row.index)];
      totalRows += parameterRows.length;

      const existingResults = resultsByLookup.get(lookup);
      if (existingResults != null) {
        existingResults.push(...parameterRows);
      } else {
        resultsByLookup.set(lookup, parameterRows);
      }
    }

    if (totalRows > limit) {
      // Note that the LIMIT in the query allows more rows than parameters (because each row stores an array of
      // parameter results). That array is very small though, and it doesn't allow fewer rows (due to the != []), so
      // the SQL limit is good enough.
      throw new ParameterSetLimitExceededError(limit);
    }

    const results: sync_rules.ParameterLookupRows[] = [];
    resultsByLookup.forEach((rows, lookup) => results.push({ lookup, rows }));
    return results;
  }

  async *getBucketDataBatch(
    checkpoint: InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    if (dataBuckets.length == 0) {
      return;
    }

    // Internal naming:
    // We do a query for one "batch", which may be returend in multiple "chunks".
    // Each chunk is limited to single bucket, and is limited in length and size.
    // There are also overall batch length and size limits.
    // Each batch query batch are streamed in separate sets of rows, which may or may
    // not match up with chunks.

    const end = checkpoint ?? BIGINT_MAX;
    const filters = dataBuckets.map((request) => ({ bucket_name: request.bucket, start: request.start }));
    const startOpByBucket = new Map(dataBuckets.map((request) => [request.bucket, request.start]));

    const batchRowLimit = options?.limit ?? storage.DEFAULT_DOCUMENT_BATCH_LIMIT;
    const chunkSizeLimitBytes = options?.chunkLimitBytes ?? storage.DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES;

    let chunkSizeBytes = 0;
    let currentChunk: utils.SyncBucketData | null = null;
    let targetOp: InternalOpId | null = null;
    let batchRowCount = 0;

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
        { type: 'int4', value: this.replicationStreamId },
        { type: 'int8', value: end },
        { type: 'int4', value: batchRowLimit },
        ...filters.flatMap((f) => [
          { type: 'varchar' as const, value: f.bucket_name },
          { type: 'int8' as const, value: f.start } satisfies StatementParam
        ])
      ]
    })) {
      const decodedRows = rows.map((r) => models.BucketData.decode(r as any));

      for (const row of decodedRows) {
        const { bucket_name } = row;
        const rowSizeBytes = row.data ? row.data.length : 0;

        const sizeExceeded =
          chunkSizeBytes >= chunkSizeLimitBytes ||
          (currentChunk?.data.length && chunkSizeBytes + rowSizeBytes > chunkSizeLimitBytes) ||
          (currentChunk?.data.length ?? 0) >= batchRowLimit;

        if (currentChunk == null || currentChunk.bucket != bucket_name || sizeExceeded) {
          let start: string | undefined = undefined;
          if (currentChunk != null) {
            if (currentChunk.bucket == bucket_name) {
              currentChunk.has_more = true;
              start = currentChunk.next_after;
            }

            const yieldChunk = currentChunk;
            currentChunk = null;
            chunkSizeBytes = 0;
            yield { chunkData: yieldChunk, targetOp: targetOp };
            targetOp = null;
            if (batchRowCount >= batchRowLimit) {
              // We've yielded all the requested rows
              break;
            }
          }

          if (start == null) {
            const startOpId = startOpByBucket.get(bucket_name);
            if (startOpId == null) {
              throw new framework.ServiceAssertionError(`data for unexpected bucket: ${bucket_name}`);
            }
            start = internalToExternalOpId(startOpId);
          }
          currentChunk = {
            bucket: bucket_name,
            after: start,
            // this is updated when we yield the batch
            has_more: false,
            data: [],
            // this is updated incrementally
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

        currentChunk.data.push(entry);
        currentChunk.next_after = entry.op_id;

        chunkSizeBytes += rowSizeBytes;

        // Manually track the total rows yielded
        batchRowCount++;
      }
    }

    if (currentChunk != null) {
      const yieldChunk = currentChunk;
      currentChunk = null;
      // This is the final chunk in the batch.
      // There may be more data if and only if the batch we retrieved isn't complete.
      // If batchRowCount == batchRowLimit, we don't actually know whether there is more data,
      // but it is safe to return true in that case.
      yieldChunk.has_more = batchRowCount >= batchRowLimit;
      yield { chunkData: yieldChunk, targetOp: targetOp };
      targetOp = null;
    }
  }

  async getChecksums(
    checkpoint: utils.InternalOpId,
    buckets: storage.BucketChecksumRequest[]
  ): Promise<utils.ChecksumMap> {
    return this.checksumCache.getChecksumMap(checkpoint, buckets);
  }

  clearChecksumCache() {
    this.checksumCache.clear();
  }

  async terminate(options?: storage.TerminateOptions) {
    if (!options || options?.clearStorage) {
      await this.clear(options);
    }
    await this.db.sql`
      UPDATE sync_rules
      SET
        state = ${{ type: 'varchar', value: storage.SyncRuleState.TERMINATED }},
        snapshot_done = ${{ type: 'bool', value: false }}
      WHERE
        id = ${{ type: 'int4', value: this.replicationStreamId }}
    `.execute();
  }

  async getStatus(): Promise<storage.ReplicationStreamStatus> {
    const syncRulesRow = await this.db.sql`
      SELECT
        snapshot_done,
        snapshot_lsn,
        last_checkpoint_lsn,
        state
      FROM
        sync_rules
      WHERE
        id = ${{ type: 'int4', value: this.replicationStreamId }}
    `
      .decoded(pick(models.SyncRules, ['snapshot_done', 'last_checkpoint_lsn', 'snapshot_lsn']))
      .first();

    if (syncRulesRow == null) {
      throw new Error('Cannot find replication stream status');
    }

    return {
      snapshotDone: syncRulesRow.snapshot_done && syncRulesRow.last_checkpoint_lsn != null,
      resumeLsn: maxLsn(syncRulesRow.snapshot_lsn, syncRulesRow.last_checkpoint_lsn)
    };
  }

  async clear(options?: storage.ClearStorageOptions): Promise<void> {
    // TODO: Cleanly abort the cleanup when the provided signal is aborted.
    await this.db.sql`
      UPDATE sync_rules
      SET
        snapshot_done = FALSE,
        last_checkpoint_lsn = NULL,
        last_checkpoint = NULL,
        no_checkpoint_before = NULL
      WHERE
        id = ${{ type: 'int4', value: this.replicationStreamId }}
    `.execute();

    await this.db.sql`
      DELETE FROM bucket_data
      WHERE
        group_id = ${{ type: 'int4', value: this.replicationStreamId }}
    `.execute();

    await this.db.sql`
      DELETE FROM bucket_parameters
      WHERE
        group_id = ${{ type: 'int4', value: this.replicationStreamId }}
    `.execute();

    await this.currentDataStore.deleteGroupRows(this.db, { groupId: this.replicationStreamId });

    await this.db.sql`
      DELETE FROM source_tables
      WHERE
        group_id = ${{ type: 'int4', value: this.replicationStreamId }}
    `.execute();
  }

  private async getChecksumsInternal(batch: storage.FetchPartialBucketChecksum[]): Promise<storage.PartialChecksumMap> {
    if (batch.length == 0) {
      return new Map();
    }

    const rangedBatch = batch.map((b) => ({
      bucket: b.bucket,
      start: String(b.start ?? 0n),
      end: String(b.end)
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
        b.group_id = ${{ type: 'int4', value: this.replicationStreamId }}
      GROUP BY
        b.bucket_name;
    `.rows<{ bucket: string; checksum_total: bigint; total: bigint; has_clear_op: number }>();

    return new Map<string, storage.PartialOrFullChecksum>(
      results.map((doc) => {
        const checksum = Number(BigInt(doc.checksum_total) & 0xffffffffn) & 0xffffffff;

        return [
          doc.bucket,
          doc.has_clear_op == 1
            ? ({
                // full checksum
                bucket: doc.bucket,
                count: Number(doc.total),
                checksum
              } satisfies BucketChecksum)
            : ({
                bucket: doc.bucket,
                partialCount: Number(doc.total),
                partialChecksum: checksum
              } satisfies PartialChecksum)
        ];
      })
    );
  }

  async getActiveCheckpoint(): Promise<storage.ReplicationCheckpoint> {
    const activeCheckpoint = await this.db.sql`
      SELECT
        id,
        last_checkpoint,
        last_checkpoint_lsn
      FROM
        sync_rules
      WHERE
        state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
        OR state = ${{ value: storage.SyncRuleState.ERRORED, type: 'varchar' }}
      ORDER BY
        id DESC
      LIMIT
        1
    `
      .decoded(models.ActiveCheckpoint)
      .first();

    return this.makeActiveCheckpoint(activeCheckpoint);
  }

  async *watchCheckpointChanges(options: WatchWriteCheckpointOptions): AsyncIterable<storage.StorageCheckpointUpdate> {
    let lastCheckpoint: utils.InternalOpId | null = null;
    let lastWriteCheckpoint: bigint | null = null;

    const { signal, user_id } = options;

    const iter = wrapWithAbort(this.sharedIterator, signal);
    for await (const cp of iter) {
      const { checkpoint, lsn } = cp;

      // lsn changes are not important by itself.
      // What is important is:
      // 1. checkpoint (op_id) changes.
      // 2. write checkpoint changes for the specific user
      const lsnFilters: Record<string, string> = lsn ? { 1: lsn } : {};

      const currentWriteCheckpoint = await this.lastWriteCheckpoint({
        user_id,
        heads: {
          ...lsnFilters
        }
      });

      if (currentWriteCheckpoint == lastWriteCheckpoint && checkpoint == lastCheckpoint) {
        // No change - wait for next one
        // In some cases, many LSNs may be produced in a short time.
        // Add a delay to throttle the write checkpoint lookup a bit.
        await timers.setTimeout(20 + 10 * Math.random());
        continue;
      }

      lastWriteCheckpoint = currentWriteCheckpoint;
      lastCheckpoint = checkpoint;

      yield {
        base: cp,
        writeCheckpoint: currentWriteCheckpoint,
        update: CHECKPOINT_INVALIDATE_ALL
      };
    }
  }

  protected async *watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<storage.ReplicationCheckpoint> {
    const doc = await this.db.sql`
      SELECT
        id,
        last_checkpoint,
        last_checkpoint_lsn
      FROM
        sync_rules
      WHERE
        state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
        OR state = ${{ value: storage.SyncRuleState.ERRORED, type: 'varchar' }}
      LIMIT
        1
    `
      .decoded(models.ActiveCheckpoint)
      .first();

    if (doc == null) {
      // Abort the connections - clients will have to retry later.
      throw new framework.ServiceError(framework.ErrorCode.PSYNC_S2302, 'No active replication stream available');
    }

    const sink = new LastValueSink<string>(undefined);

    const disposeListener = this.db.registerListener({
      notification: (notification) => sink.write(notification.payload)
    });

    signal.addEventListener('aborted', async () => {
      disposeListener();
      sink.end();
    });

    yield this.makeActiveCheckpoint(doc);

    let lastOp: storage.ReplicationCheckpoint | null = null;
    for await (const payload of sink.withSignal(signal)) {
      if (signal.aborted) {
        return;
      }

      const notification = models.ActiveCheckpointNotification.decode(payload);
      if (notification.active_checkpoint == null) {
        continue;
      }
      if (Number(notification.active_checkpoint.id) != doc.id) {
        // Active replication stream changed - abort and restart the stream
        break;
      }

      const activeCheckpoint = this.makeActiveCheckpoint(notification.active_checkpoint);

      if (lastOp == null || activeCheckpoint.lsn != lastOp.lsn || activeCheckpoint.checkpoint != lastOp.checkpoint) {
        lastOp = activeCheckpoint;
        yield activeCheckpoint;
      }
    }
  }

  async getCheckpointChanges(options: GetCheckpointChangesOptions): Promise<CheckpointChanges> {
    // We do not track individual changes yet
    return CHECKPOINT_INVALIDATE_ALL;
  }

  private makeActiveCheckpoint(row: models.ActiveCheckpointDecoded | null) {
    return new PostgresReplicationCheckpoint(this, row?.last_checkpoint ?? 0n, row?.last_checkpoint_lsn ?? null);
  }
}

class PostgresReplicationCheckpoint implements storage.ReplicationCheckpoint {
  constructor(
    private storage: PostgresSyncRulesStorage,
    public readonly checkpoint: utils.InternalOpId,
    public readonly lsn: string | null
  ) {}

  getParameterSets(
    lookups: sync_rules.ScopedParameterLookup[],
    limit: number
  ): Promise<sync_rules.ParameterLookupRows[]> {
    return this.storage.getParameterSets(this, lookups, limit);
  }
}

const parameterSetsRow = t.object({
  index: bigint,
  bucket_parameters: t.string
});
