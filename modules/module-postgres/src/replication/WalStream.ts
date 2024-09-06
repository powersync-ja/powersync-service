import * as pgwire from '@powersync/service-jpgwire';
import * as util from '../utils/pgwire_utils.js';
import { container, errors, logger } from '@powersync/lib-services-framework';
import { DatabaseInputRow, SqliteRow, SqlSyncRules, TablePattern, toSyncRulesRow } from '@powersync/service-sync-rules';
import { getPgOutputRelation, getRelId } from './PgRelation.js';
import { Metrics, SourceEntityDescriptor, storage } from '@powersync/service-core';
import { checkSourceConfiguration, getReplicationIdentityColumns } from './replication-utils.js';
import { PgManager } from './PgManager.js';

export const ZERO_LSN = '00000000/00000000';
export const PUBLICATION_NAME = 'powersync';

export interface WalStreamOptions {
  connections: PgManager;
  storage: storage.SyncRulesBucketStorage;
  abort_signal: AbortSignal;
}

interface InitResult {
  needsInitialSync: boolean;
}

export class MissingReplicationSlotError extends Error {
  constructor(message: string) {
    super(message);
  }
}

export class WalStream {
  sync_rules: SqlSyncRules;
  group_id: number;

  connection_id = 1;

  private readonly storage: storage.SyncRulesBucketStorage;

  private readonly slot_name: string;

  private connections: PgManager;

  private abort_signal: AbortSignal;

  private relation_cache = new Map<string | number, storage.SourceTable>();

  private startedStreaming = false;

  constructor(options: WalStreamOptions) {
    this.storage = options.storage;
    this.sync_rules = options.storage.sync_rules;
    this.group_id = options.storage.group_id;
    this.slot_name = options.storage.slot_name;
    this.connections = options.connections;

    this.abort_signal = options.abort_signal;
    this.abort_signal.addEventListener(
      'abort',
      () => {
        if (this.startedStreaming) {
          // Ping to speed up cancellation of streaming replication
          // We're not using pg_snapshot here, since it could be in the middle of
          // an initial replication transaction.
          const promise = util.retriedQuery(
            this.connections.pool,
            `SELECT * FROM pg_logical_emit_message(false, 'powersync', 'ping')`
          );
          promise.catch((e) => {
            // Failures here are okay - this only speeds up stopping the process.
            logger.warn('Failed to ping connection', e);
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
    if (tablePattern.isWildcard) {
      const result = await db.query({
        statement: `SELECT c.oid AS relid, c.relname AS table_name
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = $1
      AND c.relkind = 'r'
      AND c.relname LIKE $2`,
        params: [
          { type: 'varchar', value: schema },
          { type: 'varchar', value: tablePattern.tablePattern }
        ]
      });
      tableRows = pgwire.pgwireRows(result);
    } else {
      const result = await db.query({
        statement: `SELECT c.oid AS relid, c.relname AS table_name
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = $1
      AND c.relkind = 'r'
      AND c.relname = $2`,
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
        throw new Error(`missing relid for ${name}`);
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
        logger.info(`Skipping ${tablePattern.schema}.${name} - not part of ${PUBLICATION_NAME} publication`);
        continue;
      }

      const cresult = await getReplicationIdentityColumns(db, relid);

      const table = await this.handleRelation(
        batch,
        {
          name,
          schema,
          objectId: relid,
          replicationColumns: cresult.replicationColumns
        } as SourceEntityDescriptor,
        false
      );

      result.push(table);
    }
    return result;
  }

  async initSlot(): Promise<InitResult> {
    await checkSourceConfiguration(this.connections.pool, PUBLICATION_NAME);

    const slotName = this.slot_name;

    const status = await this.storage.getStatus();
    if (status.snapshot_done && status.checkpoint_lsn) {
      logger.info(`${slotName} Initial replication already done`);

      let last_error = null;

      // Check that replication slot exists
      for (let i = 120; i >= 0; i--) {
        await touch();

        if (i == 0) {
          container.reporter.captureException(last_error, {
            level: errors.ErrorSeverity.ERROR,
            metadata: {
              replication_slot: slotName
            }
          });

          throw last_error;
        }
        try {
          // We peek a large number of changes here, to make it more likely to pick up replication slot errors.
          // For example, "publication does not exist" only occurs here if the peek actually includes changes related
          // to the slot.
          await this.connections.pool.query({
            statement: `SELECT *
                    FROM pg_catalog.pg_logical_slot_peek_binary_changes($1, NULL, 1000, 'proto_version', '1',
                                                                        'publication_names', $2)`,
            params: [
              { type: 'varchar', value: slotName },
              { type: 'varchar', value: PUBLICATION_NAME }
            ]
          });
          // Success
          logger.info(`Slot ${slotName} appears healthy`);
          return { needsInitialSync: false };
        } catch (e) {
          last_error = e;
          logger.warn(`${slotName} Replication slot error`, e);

          if (this.stopped) {
            throw e;
          }

          // Could also be `publication "powersync" does not exist`, although this error may show up much later
          // in some cases.

          if (
            /incorrect prev-link/.test(e.message) ||
            /replication slot.*does not exist/.test(e.message) ||
            /publication.*does not exist/.test(e.message)
          ) {
            container.reporter.captureException(e, {
              level: errors.ErrorSeverity.WARNING,
              metadata: {
                try_index: i,
                replication_slot: slotName
              }
            });
            // Sample: record with incorrect prev-link 10000/10000 at 0/18AB778
            //   Seen during development. Some internal error, fixed by re-creating slot.
            //
            // Sample: publication "powersync" does not exist
            //   Happens when publication deleted or never created.
            //   Slot must be re-created in this case.
            logger.info(`${slotName} does not exist anymore, will create new slot`);

            throw new MissingReplicationSlotError(`Replication slot ${slotName} does not exist anymore`);
          }
          // Try again after a pause
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    }

    return { needsInitialSync: true };
  }

  async estimatedCount(db: pgwire.PgConnection, table: storage.SourceTable): Promise<string> {
    const results = await db.query({
      statement: `SELECT reltuples::bigint AS estimate
FROM   pg_class
WHERE  oid = $1::regclass`,
      params: [{ value: table.qualifiedName, type: 'varchar' }]
    });
    const row = results.rows[0];
    if ((row?.[0] ?? -1n) == -1n) {
      return '?';
    } else {
      return `~${row[0]}`;
    }
  }

  /**
   * Start initial replication.
   *
   * If (partial) replication was done before on this slot, this clears the state
   * and starts again from scratch.
   */
  async startInitialReplication(replicationConnection: pgwire.PgConnection) {
    // If anything here errors, the entire replication process is aborted,
    // and all connections closed, including this one.
    const db = await this.connections.snapshotConnection();

    const slotName = this.slot_name;

    await this.storage.clear();

    await db.query({
      statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
      params: [{ type: 'varchar', value: slotName }]
    });

    // We use the replication connection here, not a pool.
    // This connection needs to stay open at least until the snapshot is used below.
    const result = await replicationConnection.query(
      `CREATE_REPLICATION_SLOT ${slotName} LOGICAL pgoutput EXPORT_SNAPSHOT`
    );
    const columns = result.columns;
    const row = result.rows[0]!;
    if (columns[1]?.name != 'consistent_point' || columns[2]?.name != 'snapshot_name' || row == null) {
      throw new Error(`Invalid CREATE_REPLICATION_SLOT output: ${JSON.stringify(columns)}`);
    }
    // This LSN could be used in initialReplication below.
    // But it's also safe to just use ZERO_LSN - we won't get any changes older than this lsn
    // with streaming replication.
    const lsn = pgwire.lsnMakeComparable(row[1]);
    const snapshot = row[2];
    logger.info(`Created replication slot ${slotName} at ${lsn} with snapshot ${snapshot}`);

    // https://stackoverflow.com/questions/70160769/postgres-logical-replication-starting-from-given-lsn
    await db.query('BEGIN');
    // Use the snapshot exported above.
    // Using SERIALIZABLE isolation level may give stronger guarantees, but that complicates
    // the replication slot + snapshot above. And we still won't have SERIALIZABLE
    // guarantees with streaming replication.
    // See: ./docs/serializability.md for details.
    //
    // Another alternative here is to use the same pgwire connection for initial replication as well,
    // instead of synchronizing a separate transaction to the snapshot.

    try {
      await db.query(`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`);
      await db.query(`SET TRANSACTION READ ONLY`);
      await db.query(`SET TRANSACTION SNAPSHOT '${snapshot}'`);

      // Disable statement timeout for the duration of this transaction.
      // On Supabase, the default is 2 minutes.
      await db.query(`set local statement_timeout = 0`);

      logger.info(`${slotName} Starting initial replication`);
      await this.initialReplication(db, lsn);
      logger.info(`${slotName} Initial replication done`);
      await db.query('COMMIT');
    } catch (e) {
      await db.query('ROLLBACK');
      throw e;
    }
  }

  async initialReplication(db: pgwire.PgConnection, lsn: string) {
    const sourceTables = this.sync_rules.getSourceTables();
    await this.storage.startBatch({ zeroLSN: ZERO_LSN }, async (batch) => {
      for (let tablePattern of sourceTables) {
        const tables = await this.getQualifiedTableNames(batch, db, tablePattern);
        for (let table of tables) {
          await this.snapshotTable(batch, db, table);
          await batch.markSnapshotDone([table], lsn);

          await touch();
        }
      }
      await batch.commit(lsn);
    });
  }

  static *getQueryData(results: Iterable<DatabaseInputRow>): Generator<SqliteRow> {
    for (let row of results) {
      yield toSyncRulesRow(row);
    }
  }

  private async snapshotTable(batch: storage.BucketStorageBatch, db: pgwire.PgConnection, table: storage.SourceTable) {
    logger.info(`${this.slot_name} Replicating ${table.qualifiedName}`);
    const estimatedCount = await this.estimatedCount(db, table);
    let at = 0;
    let lastLogIndex = 0;
    const cursor = db.stream({ statement: `SELECT * FROM ${table.escapedIdentifier}` });
    let columns: { i: number; name: string }[] = [];
    // pgwire streams rows in chunks.
    // These chunks can be quite small (as little as 16KB), so we don't flush chunks automatically.

    for await (let chunk of cursor) {
      if (chunk.tag == 'RowDescription') {
        let i = 0;
        columns = chunk.payload.map((c) => {
          return { i: i++, name: c.name };
        });
        continue;
      }

      const rows = chunk.rows.map((row) => {
        let q: DatabaseInputRow = {};
        for (let c of columns) {
          q[c.name] = row[c.i];
        }
        return q;
      });
      if (rows.length > 0 && at - lastLogIndex >= 5000) {
        logger.info(`${this.slot_name} Replicating ${table.qualifiedName} ${at}/${estimatedCount}`);
        lastLogIndex = at;
      }
      if (this.abort_signal.aborted) {
        throw new Error(`Aborted initial replication of ${this.slot_name}`);
      }

      for (let record of WalStream.getQueryData(rows)) {
        // This auto-flushes when the batch reaches its size limit
        await batch.save({ tag: 'insert', sourceTable: table, before: undefined, after: record });
      }
      at += rows.length;
      Metrics.getInstance().rows_replicated_total.add(rows.length);

      await touch();
    }

    await batch.flush();
  }

  async handleRelation(batch: storage.BucketStorageBatch, descriptor: SourceEntityDescriptor, snapshot: boolean) {
    if (!descriptor.objectId && typeof descriptor.objectId != 'number') {
      throw new Error('objectId expected');
    }
    const result = await this.storage.resolveTable({
      group_id: this.group_id,
      connection_id: this.connection_id,
      connection_tag: this.connections.connectionTag,
      entity_descriptor: descriptor,
      sync_rules: this.sync_rules
    });
    this.relation_cache.set(descriptor.objectId, result.table);

    // Drop conflicting tables. This includes for example renamed tables.
    await batch.drop(result.dropTables);

    // Snapshot if:
    // 1. Snapshot is requested (false for initial snapshot, since that process handles it elsewhere)
    // 2. Snapshot is not already done, AND:
    // 3. The table is used in sync rules.
    const shouldSnapshot = snapshot && !result.table.snapshotComplete && result.table.syncAny;

    if (shouldSnapshot) {
      // Truncate this table, in case a previous snapshot was interrupted.
      await batch.truncate([result.table]);

      let lsn: string = ZERO_LSN;
      // Start the snapshot inside a transaction.
      // We use a dedicated connection for this.
      const db = await this.connections.snapshotConnection();
      try {
        await db.query('BEGIN');
        try {
          // Get the current LSN.
          // The data will only be consistent once incremental replication
          // has passed that point.
          const rs = await db.query(`select pg_current_wal_lsn() as lsn`);
          lsn = rs.rows[0][0];
          await this.snapshotTable(batch, db, result.table);
          await db.query('COMMIT');
        } catch (e) {
          await db.query('ROLLBACK');
          throw e;
        }
      } finally {
        await db.end();
      }
      const [table] = await batch.markSnapshotDone([result.table], lsn);
      return table;
    }

    return result.table;
  }

  private getTable(relationId: number): storage.SourceTable {
    const table = this.relation_cache.get(relationId);
    if (table == null) {
      // We should always receive a replication message before the relation is used.
      // If we can't find it, it's a bug.
      throw new Error(`Missing relation cache for ${relationId}`);
    }
    return table;
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
        logger.debug(`Table ${table.qualifiedName} not used in sync rules - skipping`);
        return null;
      }

      if (msg.tag == 'insert') {
        Metrics.getInstance().rows_replicated_total.add(1);
        const baseRecord = util.constructAfterRecord(msg);
        return await batch.save({ tag: 'insert', sourceTable: table, before: undefined, after: baseRecord });
      } else if (msg.tag == 'update') {
        Metrics.getInstance().rows_replicated_total.add(1);
        // "before" may be null if the replica id columns are unchanged
        // It's fine to treat that the same as an insert.
        const before = util.constructBeforeRecord(msg);
        const after = util.constructAfterRecord(msg);
        return await batch.save({ tag: 'update', sourceTable: table, before: before, after: after });
      } else if (msg.tag == 'delete') {
        Metrics.getInstance().rows_replicated_total.add(1);
        const before = util.constructBeforeRecord(msg)!;

        return await batch.save({ tag: 'delete', sourceTable: table, before: before, after: undefined });
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
      const replicationConnection = await this.connections.replicationConnection();
      await this.initReplication(replicationConnection);
      await this.streamChanges(replicationConnection);
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    }
  }

  async initReplication(replicationConnection: pgwire.PgConnection) {
    const result = await this.initSlot();
    if (result.needsInitialSync) {
      await this.startInitialReplication(replicationConnection);
    }
  }

  async streamChanges(replicationConnection: pgwire.PgConnection) {
    // When changing any logic here, check /docs/wal-lsns.md.

    const replicationStream = replicationConnection.logicalReplication({
      slot: this.slot_name,
      options: {
        proto_version: '1',
        publication_names: PUBLICATION_NAME
      }
    });
    this.startedStreaming = true;

    // Auto-activate as soon as initial replication is done
    await this.storage.autoActivate();

    await this.storage.startBatch({ zeroLSN: ZERO_LSN }, async (batch) => {
      // Replication never starts in the middle of a transaction
      let inTx = false;
      let count = 0;

      for await (const chunk of replicationStream.pgoutputDecode()) {
        await touch();

        if (this.abort_signal.aborted) {
          break;
        }

        // chunkLastLsn may come from normal messages in the chunk,
        // or from a PrimaryKeepalive message.
        const { messages, lastLsn: chunkLastLsn } = chunk;

        for (const msg of messages) {
          if (msg.tag == 'relation') {
            await this.handleRelation(batch, getPgOutputRelation(msg), true);
          } else if (msg.tag == 'begin') {
            inTx = true;
          } else if (msg.tag == 'commit') {
            Metrics.getInstance().transactions_replicated_total.add(1);
            inTx = false;
            await batch.commit(msg.lsn!);
            await this.ack(msg.lsn!, replicationStream);
          } else {
            if (count % 100 == 0) {
              logger.info(`${this.slot_name} replicating op ${count} ${msg.lsn}`);
            }

            count += 1;
            const result = await this.writeChange(batch, msg);
          }
        }

        if (!inTx) {
          // In a transaction, we ack and commit according to the transaction progress.
          // Outside transactions, we use the PrimaryKeepalive messages to advance progress.
          // Big caveat: This _must not_ be used to skip individual messages, since this LSN
          // may be in the middle of the next transaction.
          // It must only be used to associate checkpoints with LSNs.
          if (await batch.keepalive(chunkLastLsn)) {
            await this.ack(chunkLastLsn, replicationStream);
          }
        }

        Metrics.getInstance().chunks_replicated_total.add(1);
      }
    });
  }

  async ack(lsn: string, replicationStream: pgwire.ReplicationStream) {
    if (lsn == ZERO_LSN) {
      return;
    }

    replicationStream.ack(lsn);
  }
}

async function touch() {
  // FIXME: The hosted Kubernetes probe does not actually check the timestamp on this.
  // FIXME: We need a timeout of around 5+ minutes in Kubernetes if we do start checking the timestamp,
  // or reduce PING_INTERVAL here.
  return container.probes.touch();
}
