import * as lib_postgres from '@powersync/lib-service-postgres';
import {
  container,
  DatabaseConnectionError,
  logger as defaultLogger,
  ErrorCode,
  Logger,
  ReplicationAbortedError,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import {
  BucketStorageBatch,
  getUuidReplicaIdentityBson,
  MetricsEngine,
  RelationCache,
  SaveUpdate,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import {
  applyRowContext,
  applyValueContext,
  CompatibilityContext,
  DatabaseInputRow,
  SqliteInputRow,
  SqliteInputValue,
  SqliteRow,
  SqliteValue,
  SqlSyncRules,
  HydratedSyncRules,
  TablePattern,
  ToastableSqliteRow,
  toSyncRulesRow,
  toSyncRulesValue
} from '@powersync/service-sync-rules';

import { ReplicationMetric } from '@powersync/service-types';
import { PgManager } from './PgManager.js';
import { getPgOutputRelation, getRelId, referencedColumnTypeIds } from './PgRelation.js';
import { checkSourceConfiguration, checkTableRls, getReplicationIdentityColumns } from './replication-utils.js';
import {
  ChunkedSnapshotQuery,
  IdSnapshotQuery,
  MissingRow,
  PrimaryKeyValue,
  SimpleSnapshotQuery,
  SnapshotQuery
} from './SnapshotQuery.js';
import { PostgresTypeResolver } from '../types/resolver.js';

export interface WalStreamOptions {
  logger?: Logger;
  connections: PgManager;
  storage: storage.SyncRulesBucketStorage;
  metrics: MetricsEngine;
  abort_signal: AbortSignal;

  /**
   * Override snapshot chunk length (number of rows), for testing.
   *
   * Defaults to 10_000.
   *
   * Note that queries are streamed, so we don't actually keep that much data in memory.
   */
  snapshotChunkLength?: number;
}

interface InitResult {
  /** True if initial snapshot is not yet done. */
  needsInitialSync: boolean;
  /** True if snapshot must be started from scratch with a new slot. */
  needsNewSlot: boolean;
}

export const ZERO_LSN = '00000000/00000000';
export const PUBLICATION_NAME = 'powersync';
export const POSTGRES_DEFAULT_SCHEMA = 'public';

export const KEEPALIVE_CONTENT = 'ping';
export const KEEPALIVE_BUFFER = Buffer.from(KEEPALIVE_CONTENT);
export const KEEPALIVE_STATEMENT: pgwire.Statement = {
  statement: /* sql */ `
    SELECT
      *
    FROM
      pg_logical_emit_message(FALSE, 'powersync', $1)
  `,
  params: [{ type: 'varchar', value: KEEPALIVE_CONTENT }]
} as const;

export const isKeepAliveMessage = (msg: pgwire.PgoutputMessage) => {
  return (
    msg.tag == 'message' &&
    msg.prefix == 'powersync' &&
    msg.content &&
    Buffer.from(msg.content).equals(KEEPALIVE_BUFFER)
  );
};

export const sendKeepAlive = async (db: pgwire.PgClient) => {
  await lib_postgres.retriedQuery(db, KEEPALIVE_STATEMENT);
};

export class MissingReplicationSlotError extends Error {
  constructor(message: string, cause?: any) {
    super(message);

    this.cause = cause;
  }
}

export class WalStream {
  sync_rules: HydratedSyncRules;
  group_id: number;

  connection_id = 1;

  private logger: Logger;

  private readonly storage: storage.SyncRulesBucketStorage;
  private readonly metrics: MetricsEngine;
  private readonly slot_name: string;

  private connections: PgManager;

  private abort_signal: AbortSignal;

  private relationCache = new RelationCache((relation: number | SourceTable) => {
    if (typeof relation == 'number') {
      return relation;
    }
    return relation.objectId!;
  });

  private startedStreaming = false;

  private snapshotChunkLength: number;

  /**
   * Time of the oldest uncommitted change, according to the source db.
   * This is used to determine the replication lag.
   */
  private oldestUncommittedChange: Date | null = null;
  /**
   * Keep track of whether we have done a commit or keepalive yet.
   * We can only compute replication lag if isStartingReplication == false, or oldestUncommittedChange is present.
   */
  private isStartingReplication = true;

  constructor(options: WalStreamOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.storage = options.storage;
    this.metrics = options.metrics;
    this.sync_rules = options.storage.getParsedSyncRules({ defaultSchema: POSTGRES_DEFAULT_SCHEMA });
    this.group_id = options.storage.group_id;
    this.slot_name = options.storage.slot_name;
    this.connections = options.connections;
    this.snapshotChunkLength = options.snapshotChunkLength ?? 10_000;

    this.abort_signal = options.abort_signal;
    this.abort_signal.addEventListener(
      'abort',
      () => {
        if (this.startedStreaming) {
          // Ping to speed up cancellation of streaming replication
          // We're not using pg_snapshot here, since it could be in the middle of
          // an initial replication transaction.
          const promise = sendKeepAlive(this.connections.pool);
          promise.catch((e) => {
            // Failures here are okay - this only speeds up stopping the process.
            this.logger.warn('Failed to ping connection', e);
          });
        } else {
          // If we haven't started streaming yet, it could be due to something like
          // and invalid password. In that case, don't attempt to ping.
        }
      },
      { once: true }
    );
  }

  get stopped() {
    return this.abort_signal.aborted;
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
          replicaIdColumns: cresult.replicationColumns,
          replicationIdentity: cresult.replicationIdentity
        } as SourceEntityDescriptor,
        snapshot: false,
        referencedTypeIds: columnTypes
      });

      result.push(table);
    }
    return result;
  }

  async initSlot(): Promise<InitResult> {
    await checkSourceConfiguration(this.connections.pool, PUBLICATION_NAME);
    await this.ensureStorageCompatibility();

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
    const row = results.rows[0];
    return Number(row?.decodeWithoutCustomTypes(0) ?? -1n);
  }

  /**
   * Start initial replication.
   *
   * If (partial) replication was done before on this slot, this clears the state
   * and starts again from scratch.
   */
  async startInitialReplication(replicationConnection: pgwire.PgConnection, status: InitResult) {
    // If anything here errors, the entire replication process is aborted,
    // and all connections are closed, including this one.
    const db = await this.connections.snapshotConnection();

    const slotName = this.slot_name;

    if (status.needsNewSlot) {
      // This happens when there is no existing replication slot, or if the
      // existing one is unhealthy.
      // In those cases, we have to start replication from scratch.
      // If there is an existing healthy slot, we can skip this and continue
      // initial replication where we left off.
      await this.storage.clear({ signal: this.abort_signal });

      await db.query({
        statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      });

      // We use the replication connection here, not a pool.
      // The replication slot must be created before we start snapshotting tables.
      await replicationConnection.query(`CREATE_REPLICATION_SLOT ${slotName} LOGICAL pgoutput`);

      this.logger.info(`Created replication slot ${slotName}`);
    }

    await this.initialReplication(db);
  }

  async initialReplication(db: pgwire.PgConnection) {
    const sourceTables = this.sync_rules.getSourceTables();
    const flushResults = await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: ZERO_LSN,
        defaultSchema: POSTGRES_DEFAULT_SCHEMA,
        storeCurrentData: true,
        skipExistingRows: true
      },
      async (batch) => {
        let tablesWithStatus: SourceTable[] = [];
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
            tablesWithStatus.push(table);

            this.logger.info(`To replicate: ${table.qualifiedName} ${table.formatSnapshotProgress()}`);
          }
        }

        for (let table of tablesWithStatus) {
          await this.snapshotTableInTx(batch, db, table);
          this.touch();
        }

        // Always commit the initial snapshot at zero.
        // This makes sure we don't skip any changes applied before starting this snapshot,
        // in the case of snapshot retries.
        // We could alternatively commit at the replication slot LSN.
        await batch.commit(ZERO_LSN);
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
      await this.storage.populatePersistentChecksumCache({
        // No checkpoint yet, but we do have the opId.
        maxOpId: lastOp,
        signal: this.abort_signal
      });
    }
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

  private async snapshotTableInTx(
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
      const [resultTable] = await batch.markSnapshotDone([table], tableLsnNotBefore);
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
          const record = this.sync_rules.applyRowContext<never>(WalStream.decodeRow(rawRow, this.connections.types));

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

      if (this.abort_signal.aborted) {
        // We only abort after flushing
        throw new ReplicationAbortedError(`Initial replication interrupted`);
      }
    }
  }

  async handleRelation(options: {
    batch: storage.BucketStorageBatch;
    descriptor: SourceEntityDescriptor;
    snapshot: boolean;
    referencedTypeIds: number[];
  }) {
    const { batch, descriptor, snapshot, referencedTypeIds } = options;

    if (!descriptor.objectId && typeof descriptor.objectId != 'number') {
      throw new ReplicationAssertionError(`objectId expected, got ${typeof descriptor.objectId}`);
    }
    const result = await this.storage.resolveTable({
      group_id: this.group_id,
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

    // Snapshot if:
    // 1. Snapshot is requested (false for initial snapshot, since that process handles it elsewhere)
    // 2. Snapshot is not already done, AND:
    // 3. The table is used in sync rules.
    const shouldSnapshot = snapshot && !result.table.snapshotComplete && result.table.syncAny;

    if (shouldSnapshot) {
      // Truncate this table, in case a previous snapshot was interrupted.
      await batch.truncate([result.table]);

      // Start the snapshot inside a transaction.
      // We use a dedicated connection for this.
      const db = await this.connections.snapshotConnection();
      try {
        const table = await this.snapshotTableInTx(batch, db, result.table);
        // After the table snapshot, we wait for replication to catch up.
        // To make sure there is actually something to replicate, we send a keepalive
        // message.
        await sendKeepAlive(db);
        return table;
      } finally {
        await db.end();
      }
    }

    return result.table;
  }

  /**
   * Process rows that have missing TOAST values.
   *
   * This can happen during edge cases in the chunked intial snapshot process.
   *
   * We handle this similar to an inline table snapshot, but limited to the specific
   * set of rows.
   */
  private async resnapshot(batch: BucketStorageBatch, rows: MissingRow[]) {
    const byTable = new Map<number, MissingRow[]>();
    for (let row of rows) {
      const relId = row.table.objectId as number; // always a number for postgres
      if (!byTable.has(relId)) {
        byTable.set(relId, []);
      }
      byTable.get(relId)!.push(row);
    }
    const db = await this.connections.snapshotConnection();
    try {
      for (let rows of byTable.values()) {
        const table = rows[0].table;
        await this.snapshotTableInTx(
          batch,
          db,
          table,
          rows.map((r) => r.key)
        );
      }
      // Even with resnapshot, we need to wait until we get a new consistent checkpoint
      // after the snapshot, so we need to send a keepalive message.
      await sendKeepAlive(db);
    } finally {
      await db.end();
    }
  }

  private getTable(relationId: number): storage.SourceTable {
    const table = this.relationCache.get(relationId);
    if (table == null) {
      // We should always receive a replication message before the relation is used.
      // If we can't find it, it's a bug.
      throw new ReplicationAssertionError(`Missing relation cache for ${relationId}`);
    }
    return table;
  }

  private syncRulesRecord(row: SqliteInputRow): SqliteRow;
  private syncRulesRecord(row: SqliteInputRow | undefined): SqliteRow | undefined;

  private syncRulesRecord(row: SqliteInputRow | undefined): SqliteRow | undefined {
    if (row == null) {
      return undefined;
    }
    return this.sync_rules.applyRowContext<never>(row);
  }

  private toastableSyncRulesRecord(row: ToastableSqliteRow<SqliteInputValue>): ToastableSqliteRow {
    return this.sync_rules.applyRowContext(row);
  }

  async writeChange(
    batch: storage.BucketStorageBatch,
    msg: pgwire.PgoutputMessage
  ): Promise<storage.FlushedResult | null> {
    if (msg.lsn == null) {
      return null;
    }
    if (msg.tag == 'insert' || msg.tag == 'update' || msg.tag == 'delete') {
      const table = this.getTable(getRelId(msg.relation));
      if (!table.syncAny) {
        this.logger.debug(`Table ${table.qualifiedName} not used in sync rules - skipping`);
        return null;
      }

      if (msg.tag == 'insert') {
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        const baseRecord = this.syncRulesRecord(this.connections.types.constructAfterRecord(msg));
        return await batch.save({
          tag: storage.SaveOperationTag.INSERT,
          sourceTable: table,
          before: undefined,
          beforeReplicaId: undefined,
          after: baseRecord,
          afterReplicaId: getUuidReplicaIdentityBson(baseRecord, table.replicaIdColumns)
        });
      } else if (msg.tag == 'update') {
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        // "before" may be null if the replica id columns are unchanged
        // It's fine to treat that the same as an insert.
        const before = this.syncRulesRecord(this.connections.types.constructBeforeRecord(msg));
        const after = this.toastableSyncRulesRecord(this.connections.types.constructAfterRecord(msg));
        return await batch.save({
          tag: storage.SaveOperationTag.UPDATE,
          sourceTable: table,
          before: before,
          beforeReplicaId: before ? getUuidReplicaIdentityBson(before, table.replicaIdColumns) : undefined,
          after: after,
          afterReplicaId: getUuidReplicaIdentityBson(after, table.replicaIdColumns)
        });
      } else if (msg.tag == 'delete') {
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        const before = this.syncRulesRecord(this.connections.types.constructBeforeRecord(msg)!);

        return await batch.save({
          tag: storage.SaveOperationTag.DELETE,
          sourceTable: table,
          before: before,
          beforeReplicaId: getUuidReplicaIdentityBson(before, table.replicaIdColumns),
          after: undefined,
          afterReplicaId: undefined
        });
      }
    } else if (msg.tag == 'truncate') {
      let tables: storage.SourceTable[] = [];
      for (let relation of msg.relations) {
        const table = this.getTable(getRelId(relation));
        tables.push(table);
      }
      return await batch.truncate(tables);
    }
    return null;
  }

  async replicate() {
    try {
      // If anything errors here, the entire replication process is halted, and
      // all connections automatically closed, including this one.
      const initReplicationConnection = await this.connections.replicationConnection();
      await this.initReplication(initReplicationConnection);
      await initReplicationConnection.end();

      // At this point, the above connection has often timed out, so we start a new one
      const streamReplicationConnection = await this.connections.replicationConnection();
      await this.streamChanges(streamReplicationConnection);
      await streamReplicationConnection.end();
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    }
  }

  async initReplication(replicationConnection: pgwire.PgConnection) {
    const result = await this.initSlot();
    if (result.needsInitialSync) {
      await this.startInitialReplication(replicationConnection, result);
    }
  }

  async streamChanges(replicationConnection: pgwire.PgConnection) {
    try {
      await this.streamChangesInternal(replicationConnection);
    } catch (e) {
      if (isReplicationSlotInvalidError(e)) {
        throw new MissingReplicationSlotError(e.message, e);
      }
      throw e;
    }
  }

  private async streamChangesInternal(replicationConnection: pgwire.PgConnection) {
    // When changing any logic here, check /docs/wal-lsns.md.
    const { createEmptyCheckpoints } = await this.ensureStorageCompatibility();

    const replicationOptions: Record<string, string> = {
      proto_version: '1',
      publication_names: PUBLICATION_NAME
    };

    /**
     * Viewing the contents of logical messages emitted with `pg_logical_emit_message`
     * is only supported on Postgres >= 14.0.
     * https://www.postgresql.org/docs/14/protocol-logical-replication.html
     */
    const exposesLogicalMessages = await this.checkLogicalMessageSupport();
    if (exposesLogicalMessages) {
      /**
       * Only add this option if the Postgres server supports it.
       * Adding the option to a server that doesn't support it will throw an exception when starting logical replication.
       * Error: `unrecognized pgoutput option: messages`
       */
      replicationOptions['messages'] = 'true';
    }

    const replicationStream = replicationConnection.logicalReplication({
      slot: this.slot_name,
      options: replicationOptions
    });

    this.startedStreaming = true;

    let resnapshot: { table: storage.SourceTable; key: PrimaryKeyValue }[] = [];

    const markRecordUnavailable = (record: SaveUpdate) => {
      if (!IdSnapshotQuery.supports(record.sourceTable)) {
        // If it's not supported, it's also safe to ignore
        return;
      }
      let key: PrimaryKeyValue = {};
      for (let column of record.sourceTable.replicaIdColumns) {
        const name = column.name;
        const value = record.after[name];
        if (value == null) {
          // We don't expect this to actually happen.
          // The key should always be present in the "after" record.
          return;
        }
        // We just need a consistent representation of the primary key, and don't care about fixed quirks.
        key[name] = applyValueContext(value, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
      }
      resnapshot.push({
        table: record.sourceTable,
        key: key
      });
    };

    await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: ZERO_LSN,
        defaultSchema: POSTGRES_DEFAULT_SCHEMA,
        storeCurrentData: true,
        skipExistingRows: false,
        markRecordUnavailable
      },
      async (batch) => {
        // We don't handle any plain keepalive messages while we have transactions.
        // While we have transactions, we use that to advance the position.
        // Replication never starts in the middle of a transaction, so this starts as false.
        let skipKeepalive = false;
        let count = 0;

        for await (const chunk of replicationStream.pgoutputDecode()) {
          this.touch();

          if (this.abort_signal.aborted) {
            break;
          }

          // chunkLastLsn may come from normal messages in the chunk,
          // or from a PrimaryKeepalive message.
          const { messages, lastLsn: chunkLastLsn } = chunk;

          /**
           * We can check if an explicit keepalive was sent if `exposesLogicalMessages == true`.
           * If we can't check the logical messages, we should assume a keepalive if we
           * receive an empty array of messages in a replication event.
           */
          const assumeKeepAlive = !exposesLogicalMessages;
          let keepAliveDetected = false;
          const lastCommit = messages.findLast((msg) => msg.tag == 'commit');

          for (const msg of messages) {
            if (msg.tag == 'relation') {
              await this.handleRelation({
                batch,
                descriptor: getPgOutputRelation(msg),
                snapshot: true,
                referencedTypeIds: referencedColumnTypeIds(msg)
              });
            } else if (msg.tag == 'begin') {
              // This may span multiple transactions in the same chunk, or even across chunks.
              skipKeepalive = true;
              if (this.oldestUncommittedChange == null) {
                this.oldestUncommittedChange = new Date(Number(msg.commitTime / 1000n));
              }
            } else if (msg.tag == 'commit') {
              this.metrics.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED).add(1);
              if (msg == lastCommit) {
                // Only commit if this is the last commit in the chunk.
                // This effectively lets us batch multiple transactions within the same chunk
                // into a single flush, increasing throughput for many small transactions.
                skipKeepalive = false;
                // flush() must be before the resnapshot check - that is
                // typically what reports the resnapshot records.
                await batch.flush({ oldestUncommittedChange: this.oldestUncommittedChange });
                // This _must_ be checked after the flush(), and before
                // commit() or ack(). We never persist the resnapshot list,
                // so we have to process it before marking our progress.
                if (resnapshot.length > 0) {
                  await this.resnapshot(batch, resnapshot);
                  resnapshot = [];
                }
                const didCommit = await batch.commit(msg.lsn!, {
                  createEmptyCheckpoints,
                  oldestUncommittedChange: this.oldestUncommittedChange
                });
                await this.ack(msg.lsn!, replicationStream);
                if (didCommit) {
                  this.oldestUncommittedChange = null;
                  this.isStartingReplication = false;
                }
              }
            } else {
              if (count % 100 == 0) {
                this.logger.info(`Replicating op ${count} ${msg.lsn}`);
              }

              /**
               * If we can see the contents of logical messages, then we can check if a keepalive
               * message is present. We only perform a keepalive (below) if we explicitly detect a keepalive message.
               * If we can't see the contents of logical messages, then we should assume a keepalive is required
               * due to the default value of `assumeKeepalive`.
               */
              if (exposesLogicalMessages && isKeepAliveMessage(msg)) {
                keepAliveDetected = true;
              }

              count += 1;
              const flushResult = await this.writeChange(batch, msg);
              if (flushResult != null && resnapshot.length > 0) {
                // If we have large transactions, we also need to flush the resnapshot list
                // periodically.
                // TODO: make sure this bit is actually triggered
                await this.resnapshot(batch, resnapshot);
                resnapshot = [];
              }
            }
          }

          if (!skipKeepalive) {
            if (assumeKeepAlive || keepAliveDetected) {
              // Reset the detection flag.
              keepAliveDetected = false;

              // In a transaction, we ack and commit according to the transaction progress.
              // Outside transactions, we use the PrimaryKeepalive messages to advance progress.
              // Big caveat: This _must not_ be used to skip individual messages, since this LSN
              // may be in the middle of the next transaction.
              // It must only be used to associate checkpoints with LSNs.
              const didCommit = await batch.keepalive(chunkLastLsn);
              if (didCommit) {
                this.oldestUncommittedChange = null;
              }

              this.isStartingReplication = false;
            }

            // We receive chunks with empty messages often (about each second).
            // Acknowledging here progresses the slot past these and frees up resources.
            await this.ack(chunkLastLsn, replicationStream);
          }

          this.metrics.getCounter(ReplicationMetric.CHUNKS_REPLICATED).add(1);
        }
      }
    );
  }

  async ack(lsn: string, replicationStream: pgwire.ReplicationStream) {
    if (lsn == ZERO_LSN) {
      return;
    }

    replicationStream.ack(lsn);
  }

  /**
   * Ensures that the storage is compatible with the replication connection.
   * @throws {DatabaseConnectionError} If the storage is not compatible with the replication connection.
   */
  protected async ensureStorageCompatibility(): Promise<storage.ResolvedBucketBatchCommitOptions> {
    const supportsLogicalMessages = await this.checkLogicalMessageSupport();

    const storageIdentifier = await this.storage.factory.getSystemIdentifier();
    if (storageIdentifier.type != lib_postgres.POSTGRES_CONNECTION_TYPE) {
      return {
        // Keep the same behaviour as before allowing Postgres storage.
        createEmptyCheckpoints: true,
        oldestUncommittedChange: null
      };
    }

    const parsedStorageIdentifier = lib_postgres.utils.decodePostgresSystemIdentifier(storageIdentifier.id);
    /**
     * Check if the same server is being used for both the sync bucket storage and the logical replication.
     */
    const replicationIdentifier = await lib_postgres.utils.queryPostgresSystemIdentifier(this.connections.pool);

    if (!supportsLogicalMessages && replicationIdentifier.server_id == parsedStorageIdentifier.server_id) {
      throw new DatabaseConnectionError(
        ErrorCode.PSYNC_S1144,
        `Separate Postgres servers are required for the replication source and sync bucket storage when using Postgres versions below 14.0.`,
        new Error('Postgres version is below 14')
      );
    }

    return {
      /**
       * Don't create empty checkpoints if the same Postgres database is used for the data source
       * and sync bucket storage. Creating empty checkpoints will cause WAL feedback loops.
       */
      createEmptyCheckpoints: replicationIdentifier.database_name != parsedStorageIdentifier.database_name,
      oldestUncommittedChange: null
    };
  }

  /**
   * Check if the replication connection Postgres server supports
   * viewing the contents of logical replication messages.
   */
  protected async checkLogicalMessageSupport() {
    const version = await this.connections.getServerVersion();
    return version ? version.compareMain('14.0.0') >= 0 : false;
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    if (this.oldestUncommittedChange == null) {
      if (this.isStartingReplication) {
        // We don't have anything to compute replication lag with yet.
        return undefined;
      } else {
        // We don't have any uncommitted changes, so replication is up-to-date.
        return 0;
      }
    }
    return Date.now() - this.oldestUncommittedChange.getTime();
  }

  private touch() {
    container.probes.touch().catch((e) => {
      this.logger.error(`Error touching probe`, e);
    });
  }
}

function isReplicationSlotInvalidError(e: any) {
  // We could access the error code from pgwire using this:
  //   e[Symbol.for('pg.ErrorCode')]
  // However, we typically get a generic code such as 42704 (undefined_object), which does not
  // help much. So we check the actual error message.
  const message = e.message ?? '';

  // Sample: record with incorrect prev-link 10000/10000 at 0/18AB778
  //   Seen during development. Some internal error, fixed by re-creating slot.
  //
  // Sample: publication "powersync" does not exist
  //   Happens when publication deleted or never created.
  //   Slot must be re-created in this case.
  return (
    /incorrect prev-link/.test(message) ||
    /replication slot.*does not exist/.test(message) ||
    /publication.*does not exist/.test(message) ||
    // Postgres 18 - exceeded max_slot_wal_keep_size
    /can no longer access replication slot/.test(message) ||
    // Postgres 17 - exceeded max_slot_wal_keep_size
    /can no longer get changes from replication slot/.test(message)
  );
}
