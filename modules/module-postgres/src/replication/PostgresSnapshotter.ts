import {
  container,
  logger as defaultLogger,
  Logger,
  ReplicationAbortedError,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import {
  getUuidReplicaIdentityBson,
  MetricsEngine,
  RelationCache,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import {
  DatabaseInputRow,
  HydratedSyncRules,
  SqliteInputRow,
  SqliteInputValue,
  SqlSyncRules,
  TablePattern,
  toSyncRulesRow,
  toSyncRulesValue
} from '@powersync/service-sync-rules';

import { ReplicationMetric } from '@powersync/service-types';
import * as timers from 'node:timers/promises';
import pDefer from 'p-defer';
import { PostgresTypeResolver } from '../types/resolver.js';
import { PgManager } from './PgManager.js';
import {
  checkSourceConfiguration,
  checkTableRls,
  ensureStorageCompatibility,
  getReplicationIdentityColumns
} from './replication-utils.js';
import {
  ChunkedSnapshotQuery,
  IdSnapshotQuery,
  PrimaryKeyValue,
  SimpleSnapshotQuery,
  SnapshotQuery
} from './SnapshotQuery.js';
import {
  MissingReplicationSlotError,
  POSTGRES_DEFAULT_SCHEMA,
  PUBLICATION_NAME,
  sendKeepAlive,
  WalStreamOptions,
  ZERO_LSN
} from './WalStream.js';

interface InitResult {
  /** True if initial snapshot is not yet done. */
  needsInitialSync: boolean;
  /** True if snapshot must be started from scratch with a new slot. */
  needsNewSlot: boolean;
}

export class PostgresSnapshotter {
  sync_rules: HydratedSyncRules;
  group_id: number;

  connection_id = 1;

  private logger: Logger;

  private readonly storage: storage.SyncRulesBucketStorage;
  private readonly metrics: MetricsEngine;
  private readonly slot_name: string;

  private connections: PgManager;

  private abortSignal: AbortSignal;

  private snapshotChunkLength: number;

  private relationCache = new RelationCache((relation: number | SourceTable) => {
    if (typeof relation == 'number') {
      return relation;
    }
    return relation.objectId!;
  });

  private queue = new Set<SourceTable>();
  private initialSnapshotDone = pDefer<void>();

  constructor(options: WalStreamOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.storage = options.storage;
    this.metrics = options.metrics;
    this.sync_rules = options.storage.getHydratedSyncRules({ defaultSchema: POSTGRES_DEFAULT_SCHEMA });
    this.group_id = options.storage.group_id;
    this.slot_name = options.storage.slot_name;
    this.connections = options.connections;
    this.snapshotChunkLength = options.snapshotChunkLength ?? 10_000;

    this.abortSignal = options.abort_signal;
  }

  async getQualifiedTableNames(
    batch: storage.BucketStorageBatch,
    db: pgwire.PgConnection,
    tablePattern: TablePattern
  ): Promise<storage.SourceTable[]> {
    const schema = tablePattern.schema;
    if (tablePattern.connectionTag != this.connections.connectionTag) {
      return [];
    }

    let tableRows: any[];
    const prefix = tablePattern.isWildcard ? tablePattern.tablePrefix : undefined;

    {
      let query = `
        SELECT
          c.oid AS relid,
          c.relname AS table_name,
          (SELECT
            json_agg(DISTINCT a.atttypid)
            FROM pg_attribute a
            WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = c.oid) 
          AS column_types
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
        AND c.relkind = 'r'`;

      if (tablePattern.isWildcard) {
        query += ' AND c.relname LIKE $2';
      } else {
        query += ' AND c.relname = $2';
      }

      const result = await db.query({
        statement: query,
        params: [
          { type: 'varchar', value: schema },
          { type: 'varchar', value: tablePattern.tablePattern }
        ]
      });

      tableRows = pgwire.pgwireRows(result);
    }

    let result: storage.SourceTable[] = [];

    for (let row of tableRows) {
      const name = row.table_name as string;
      if (typeof row.relid != 'bigint') {
        throw new ReplicationAssertionError(`Missing relid for ${name}`);
      }
      const relid = Number(row.relid as bigint);

      if (prefix && !name.startsWith(prefix)) {
        continue;
      }

      const rs = await db.query({
        statement: `SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3`,
        params: [
          { type: 'varchar', value: PUBLICATION_NAME },
          { type: 'varchar', value: tablePattern.schema },
          { type: 'varchar', value: name }
        ]
      });
      if (rs.rows.length == 0) {
        this.logger.info(`Skipping ${tablePattern.schema}.${name} - not part of ${PUBLICATION_NAME} publication`);
        continue;
      }

      try {
        const result = await checkTableRls(db, relid);
        if (!result.canRead) {
          // We log the message, then continue anyway, since the check does not cover all cases.
          this.logger.warn(result.message!);
        }
      } catch (e) {
        // It's possible that we just don't have permission to access pg_roles - log the error and continue.
        this.logger.warn(`Could not check RLS access for ${tablePattern.schema}.${name}`, e);
      }

      const cresult = await getReplicationIdentityColumns(db, relid);

      const columnTypes = (JSON.parse(row.column_types) as string[]).map((e) => Number(e));
      const table = await this.handleRelation({
        batch,
        descriptor: {
          name,
          schema,
          objectId: relid,
          replicaIdColumns: cresult.replicationColumns
        } as SourceEntityDescriptor,
        referencedTypeIds: columnTypes
      });

      result.push(table);
    }
    return result;
  }

  async checkSlot(): Promise<InitResult> {
    await checkSourceConfiguration(this.connections.pool, PUBLICATION_NAME);
    await ensureStorageCompatibility(this.connections.pool, this.storage.factory);

    const slotName = this.slot_name;

    const status = await this.storage.getStatus();
    const snapshotDone = status.snapshot_done && status.checkpoint_lsn != null;
    if (snapshotDone) {
      // Snapshot is done, but we still need to check the replication slot status
      this.logger.info(`Initial replication already done`);
    }

    // Check if replication slot exists
    const slot = pgwire.pgwireRows(
      await this.connections.pool.query({
        // We specifically want wal_status and invalidation_reason, but it's not available on older versions,
        // so we just query *.
        statement: 'SELECT * FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      })
    )[0];

    // Previously we also used pg_catalog.pg_logical_slot_peek_binary_changes to confirm that we can query the slot.
    // However, there were some edge cases where the query times out, repeating the query, ultimately
    // causing high load on the source database and never recovering automatically.
    // We now instead jump straight to replication if the wal_status is not "lost", rather detecting those
    // errors during streaming replication, which is a little more robust.

    // We can have:
    //   1. needsInitialSync: true, lost slot -> MissingReplicationSlotError (starts new sync rules version).
    //      Theoretically we could handle this the same as (2).
    //   2. needsInitialSync: true, no slot -> create new slot
    //   3. needsInitialSync: true, valid slot -> resume initial sync
    //   4. needsInitialSync: false, lost slot -> MissingReplicationSlotError (starts new sync rules version)
    //   5. needsInitialSync: false, no slot -> MissingReplicationSlotError (starts new sync rules version)
    //   6. needsInitialSync: false, valid slot -> resume streaming replication
    // The main advantage of MissingReplicationSlotError are:
    // 1. If there was a complete snapshot already (cases 4/5), users can still sync from that snapshot while
    //    we do the reprocessing under a new slot name.
    // 2. If there was a partial snapshot (case 1), we can start with the new slot faster by not waiting for
    //    the partial data to be cleared.
    if (slot != null) {
      // This checks that the slot is still valid

      // wal_status is present in postgres 13+
      // invalidation_reason is present in postgres 17+
      const lost = slot.wal_status == 'lost';
      if (lost) {
        // Case 1 / 4
        throw new MissingReplicationSlotError(
          `Replication slot ${slotName} is not valid anymore. invalidation_reason: ${slot.invalidation_reason ?? 'unknown'}`
        );
      }
      // Case 3 / 6
      return {
        needsInitialSync: !snapshotDone,
        needsNewSlot: false
      };
    } else {
      if (snapshotDone) {
        // Case 5
        // This will create a new slot, while keeping the current sync rules active
        throw new MissingReplicationSlotError(`Replication slot ${slotName} is missing`);
      }
      // Case 2
      // This will clear data (if any) and re-create the same slot
      return { needsInitialSync: true, needsNewSlot: true };
    }
  }

  async estimatedCountNumber(db: pgwire.PgConnection, table: storage.SourceTable): Promise<number> {
    const results = await db.query({
      statement: `SELECT reltuples::bigint AS estimate
  FROM   pg_class
  WHERE  oid = $1::regclass`,
      params: [{ value: table.qualifiedName, type: 'varchar' }]
    });
    const count = results.rows[0]?.decodeWithoutCustomTypes(0);
    return Number(count ?? -1n);
  }

  public async setupSlot(db: pgwire.PgConnection, status: InitResult) {
    // If anything here errors, the entire replication process is aborted,
    // and all connections are closed, including this one.
    const slotName = this.slot_name;

    if (status.needsNewSlot) {
      // This happens when there is no existing replication slot, or if the
      // existing one is unhealthy.
      // In those cases, we have to start replication from scratch.
      // If there is an existing healthy slot, we can skip this and continue
      // initial replication where we left off.
      await this.storage.clear({ signal: this.abortSignal });

      await db.query({
        statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      });

      // We use the replication connection here, not a pool.
      // The replication slot must be created before we start snapshotting tables.
      const initReplicationConnection = await this.connections.replicationConnection();
      try {
        await initReplicationConnection.query(`CREATE_REPLICATION_SLOT ${slotName} LOGICAL pgoutput`);
      } finally {
        await initReplicationConnection.end();
      }

      this.logger.info(`Created replication slot ${slotName}`);
    }
  }

  async replicateTable(table: SourceTable) {
    const db = await this.connections.snapshotConnection();
    try {
      const flushResults = await this.storage.startBatch(
        {
          logger: this.logger,
          zeroLSN: ZERO_LSN,
          defaultSchema: POSTGRES_DEFAULT_SCHEMA,
          storeCurrentData: true,
          skipExistingRows: true
        },
        async (batch) => {
          await this.snapshotTableInTx(batch, db, table);
          // This commit ensures we set keepalive_op.
          // It may be better if that is automatically set when flushing.
          await batch.commit(ZERO_LSN);
        }
      );
      this.logger.info(`Flushed snapshot at ${flushResults?.flushed_op}`);
    } finally {
      await db.end();
    }
  }

  async waitForInitialSnapshot() {
    await this.initialSnapshotDone.promise;
  }

  async replicationLoop() {
    try {
      if (this.queue.size == 0) {
        // Special case where we start with no tables to snapshot
        await this.markSnapshotDone();
      }
      while (!this.abortSignal.aborted) {
        const table = this.queue.values().next().value;
        if (table == null) {
          this.initialSnapshotDone.resolve();
          await timers.setTimeout(500, { signal: this.abortSignal });
          continue;
        }

        await this.replicateTable(table);
        this.queue.delete(table);
        if (this.queue.size == 0) {
          await this.markSnapshotDone();
        }
      }
      throw new ReplicationAbortedError(`Replication loop aborted`, this.abortSignal.reason);
    } catch (e) {
      // If initial snapshot already completed, this has no effect
      this.initialSnapshotDone.reject(e);
      throw e;
    }
  }

  private async markSnapshotDone() {
    const db = await this.connections.snapshotConnection();
    await using _ = { [Symbol.asyncDispose]: () => db.end() };

    const flushResults = await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: ZERO_LSN,
        defaultSchema: POSTGRES_DEFAULT_SCHEMA,
        storeCurrentData: true,
        skipExistingRows: true
      },
      async (batch) => {
        const rs = await db.query(`select pg_current_wal_lsn() as lsn`);
        const globalLsnNotBefore = rs.rows[0].decodeWithoutCustomTypes(0);
        await batch.markAllSnapshotDone(globalLsnNotBefore);
      }
    );
    /**
     * Send a keepalive message after initial replication.
     * In some edge cases we wait for a keepalive after the initial snapshot.
     * If we don't explicitly check the contents of keepalive messages then a keepalive is detected
     * rather quickly after initial replication - perhaps due to other WAL events.
     * If we do explicitly check the contents of messages, we need an actual keepalive payload in order
     * to advance the active sync rules LSN.
     */
    await sendKeepAlive(db);

    const lastOp = flushResults?.flushed_op;
    if (lastOp != null) {
      // Populate the cache _after_ initial replication, but _before_ we switch to this sync rules.
      // TODO: only run this after initial replication, not after each table.
      await this.storage.populatePersistentChecksumCache({
        // No checkpoint yet, but we do have the opId.
        maxOpId: lastOp,
        signal: this.abortSignal
      });
    }
  }

  /**
   * Start initial replication.
   *
   * If (partial) replication was done before on this slot, this clears the state
   * and starts again from scratch.
   */
  async queueSnapshotTables(db: pgwire.PgConnection) {
    const sourceTables = this.sync_rules.getSourceTables();

    await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: ZERO_LSN,
        defaultSchema: POSTGRES_DEFAULT_SCHEMA,
        storeCurrentData: true,
        skipExistingRows: true
      },
      async (batch) => {
        for (let tablePattern of sourceTables) {
          const tables = await this.getQualifiedTableNames(batch, db, tablePattern);
          // Pre-get counts
          for (let table of tables) {
            if (table.snapshotComplete) {
              this.logger.info(`Skipping ${table.qualifiedName} - snapshot already done`);
              continue;
            }
            const count = await this.estimatedCountNumber(db, table);
            table = await batch.updateTableProgress(table, { totalEstimatedCount: count });
            this.relationCache.update(table);

            this.logger.info(`To replicate: ${table.qualifiedName} ${table.formatSnapshotProgress()}`);

            this.queue.add(table);
          }
        }
      }
    );
  }

  static *getQueryData(results: Iterable<DatabaseInputRow>): Generator<SqliteInputRow> {
    for (let row of results) {
      yield toSyncRulesRow(row);
    }
  }

  public async queueSnapshot(batch: storage.BucketStorageBatch, table: storage.SourceTable) {
    await batch.markTableSnapshotRequired(table);
    this.queue.add(table);
  }

  public async snapshotTableInTx(
    batch: storage.BucketStorageBatch,
    db: pgwire.PgConnection,
    table: storage.SourceTable,
    limited?: PrimaryKeyValue[]
  ): Promise<storage.SourceTable> {
    // Note: We use the default "Read Committed" isolation level here, not snapshot isolation.
    // The data may change during the transaction, but that is compensated for in the streaming
    // replication afterwards.
    await db.query('BEGIN');
    try {
      let tableLsnNotBefore: string;
      await this.snapshotTable(batch, db, table, limited);

      // Get the current LSN.
      // The data will only be consistent once incremental replication has passed that point.
      // We have to get this LSN _after_ we have finished the table snapshot.
      //
      // There are basically two relevant LSNs here:
      // A: The LSN before the snapshot starts. We don't explicitly record this on the PowerSync side,
      //    but it is implicitly recorded in the replication slot.
      // B: The LSN after the table snapshot is complete, which is what we get here.
      // When we do the snapshot queries, the data that we get back for each chunk could match the state
      // anywhere between A and B. To actually have a consistent state on our side, we need to:
      // 1. Complete the snapshot.
      // 2. Wait until logical replication has caught up with all the change between A and B.
      // Calling `markSnapshotDone(LSN B)` covers that.
      const rs = await db.query(`select pg_current_wal_lsn() as lsn`);
      tableLsnNotBefore = rs.rows[0].decodeWithoutCustomTypes(0);
      // Side note: A ROLLBACK would probably also be fine here, since we only read in this transaction.
      await db.query('COMMIT');
      this.logger.info(`Snapshot complete for table ${table.qualifiedName}, resume at ${tableLsnNotBefore}`);
      const [resultTable] = await batch.markTableSnapshotDone([table], tableLsnNotBefore);
      this.relationCache.update(resultTable);
      return resultTable;
    } catch (e) {
      await db.query('ROLLBACK');
      throw e;
    }
  }

  private async snapshotTable(
    batch: storage.BucketStorageBatch,
    db: pgwire.PgConnection,
    table: storage.SourceTable,
    limited?: PrimaryKeyValue[]
  ) {
    let totalEstimatedCount = table.snapshotStatus?.totalEstimatedCount;
    let at = table.snapshotStatus?.replicatedCount ?? 0;
    let lastCountTime = 0;
    let q: SnapshotQuery;
    // We do streaming on two levels:
    // 1. Coarse level: DELCARE CURSOR, FETCH 10000 at a time.
    // 2. Fine level: Stream chunks from each fetch call.
    if (limited) {
      q = new IdSnapshotQuery(db, table, limited);
    } else if (ChunkedSnapshotQuery.supports(table)) {
      // Single primary key - we can use the primary key for chunking
      const orderByKey = table.replicaIdColumns[0];
      q = new ChunkedSnapshotQuery(db, table, this.snapshotChunkLength, table.snapshotStatus?.lastKey ?? null);
      if (table.snapshotStatus?.lastKey != null) {
        this.logger.info(
          `Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()} - resuming from ${orderByKey.name} > ${(q as ChunkedSnapshotQuery).lastKey}`
        );
      } else {
        this.logger.info(`Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()} - resumable`);
      }
    } else {
      // Fallback case - query the entire table
      this.logger.info(`Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()} - not resumable`);
      q = new SimpleSnapshotQuery(db, table, this.snapshotChunkLength);
      at = 0;
    }
    await q.initialize();

    let columns: { i: number; name: string }[] = [];
    let columnMap: Record<string, number> = {};
    let hasRemainingData = true;
    while (hasRemainingData) {
      // Fetch 10k at a time.
      // The balance here is between latency overhead per FETCH call,
      // and not spending too much time on each FETCH call.
      // We aim for a couple of seconds on each FETCH call.
      const cursor = q.nextChunk();
      hasRemainingData = false;
      // pgwire streams rows in chunks.
      // These chunks can be quite small (as little as 16KB), so we don't flush chunks automatically.
      // There are typically 100-200 rows per chunk.
      for await (let chunk of cursor) {
        if (chunk.tag == 'RowDescription') {
          continue;
        }

        if (chunk.rows.length > 0) {
          hasRemainingData = true;
        }

        for (const rawRow of chunk.rows) {
          const record = this.sync_rules.applyRowContext<never>(
            PostgresSnapshotter.decodeRow(rawRow, this.connections.types)
          );

          // This auto-flushes when the batch reaches its size limit
          await batch.save({
            tag: storage.SaveOperationTag.INSERT,
            sourceTable: table,
            before: undefined,
            beforeReplicaId: undefined,
            after: record,
            afterReplicaId: getUuidReplicaIdentityBson(record, table.replicaIdColumns)
          });
        }

        at += chunk.rows.length;
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(chunk.rows.length);

        this.touch();
      }

      // Important: flush before marking progress
      await batch.flush();
      if (limited == null) {
        let lastKey: Uint8Array | undefined;
        if (q instanceof ChunkedSnapshotQuery) {
          lastKey = q.getLastKeySerialized();
        }
        if (lastCountTime < performance.now() - 10 * 60 * 1000) {
          // Even though we're doing the snapshot inside a transaction, the transaction uses
          // the default "Read Committed" isolation level. This means we can get new data
          // within the transaction, so we re-estimate the count every 10 minutes when replicating
          // large tables.
          totalEstimatedCount = await this.estimatedCountNumber(db, table);
          lastCountTime = performance.now();
        }
        table = await batch.updateTableProgress(table, {
          lastKey: lastKey,
          replicatedCount: at,
          totalEstimatedCount: totalEstimatedCount
        });
        this.relationCache.update(table);

        this.logger.info(`Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()}`);
      } else {
        this.logger.info(`Replicating ${table.qualifiedName} ${at}/${limited.length} for resnapshot`);
      }

      if (this.abortSignal.aborted) {
        // We only abort after flushing
        throw new ReplicationAbortedError(`Table snapshot interrupted`, this.abortSignal.reason);
      }
    }
  }

  async handleRelation(options: {
    batch: storage.BucketStorageBatch;
    descriptor: SourceEntityDescriptor;
    referencedTypeIds: number[];
  }) {
    const { batch, descriptor, referencedTypeIds } = options;

    if (!descriptor.objectId && typeof descriptor.objectId != 'number') {
      throw new ReplicationAssertionError(`objectId expected, got ${typeof descriptor.objectId}`);
    }
    const result = await this.storage.resolveTable({
      connection_id: this.connection_id,
      connection_tag: this.connections.connectionTag,
      entity_descriptor: descriptor,
      sync_rules: this.sync_rules
    });
    this.relationCache.update(result.table);

    // Drop conflicting tables. This includes for example renamed tables.
    await batch.drop(result.dropTables);

    // Ensure we have a description for custom types referenced in the table.
    await this.connections.types.fetchTypes(referencedTypeIds);

    return result.table;
  }

  private touch() {
    container.probes.touch().catch((e) => {
      this.logger.error(`Error touching probe`, e);
    });
  }

  static decodeRow(row: pgwire.PgRow, types: PostgresTypeResolver): SqliteInputRow {
    let result: SqliteInputRow = {};

    row.raw.forEach((rawValue, i) => {
      const column = row.columns[i];
      let mappedValue: SqliteInputValue;

      if (typeof rawValue == 'string') {
        mappedValue = toSyncRulesValue(types.registry.decodeDatabaseValue(rawValue, column.typeOid), false, true);
      } else {
        // Binary format, expose as-is.
        mappedValue = rawValue;
      }

      result[column.name] = mappedValue;
    });
    return result;
  }
}
