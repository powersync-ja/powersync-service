import {
  Logger,
  logger as defaultLogger,
  ReplicationAbortedError,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import * as sync_rules from '@powersync/service-sync-rules';

import {
  ColumnDescriptor,
  framework,
  getUuidReplicaIdentityBson,
  InternalOpId,
  MetricsEngine,
  SourceTable,
  storage
} from '@powersync/service-core';
import mysql from 'mysql2';
import mysqlPromise from 'mysql2/promise';

import { TableMapEntry } from '@powersync/mysql-zongji';
import * as common from '../common/common-index.js';
import { createRandomServerId, qualifiedMySQLTable } from '../utils/mysql-utils.js';
import { MySQLConnectionManager } from './MySQLConnectionManager.js';
import { ReplicationMetric } from '@powersync/service-types';
import { BinLogEventHandler, BinLogListener, Row, SchemaChange, SchemaChangeType } from './zongji/BinLogListener.js';

export interface BinLogStreamOptions {
  connections: MySQLConnectionManager;
  storage: storage.SyncRulesBucketStorage;
  metrics: MetricsEngine;
  abortSignal: AbortSignal;
  logger?: Logger;
}

interface WriteChangePayload {
  type: storage.SaveOperationTag;
  row: Row;
  previous_row?: Row;
  database: string;
  table: string;
  sourceTable: storage.SourceTable;
  columns: Map<string, ColumnDescriptor>;
}

export class BinlogConfigurationError extends Error {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Unlike Postgres' relation id, MySQL's tableId is only guaranteed to be unique and stay the same
 * in the context of a single replication session.
 * Instead, we create a unique key by combining the source schema and table name
 * @param schema
 * @param tableName
 */
function createTableId(schema: string, tableName: string): string {
  return `${schema}.${tableName}`;
}

export class BinLogStream {
  private readonly syncRules: sync_rules.HydratedSyncRules;
  private readonly groupId: number;

  private readonly storage: storage.SyncRulesBucketStorage;

  private readonly connections: MySQLConnectionManager;

  private readonly abortSignal: AbortSignal;

  private readonly logger: Logger;

  private tableCache = new Map<string | number, storage.SourceTable[]>();

  /**
   * Time of the oldest uncommitted change, according to the source db.
   * This is used to determine the replication lag.
   */
  private oldestUncommittedChange: Date | null = null;
  /**
   * Keep track of whether we have done a commit or keepalive yet.
   * We can only compute replication lag if isStartingReplication == false, or oldestUncommittedChange is present.
   */
  isStartingReplication = true;

  constructor(private options: BinLogStreamOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.storage = options.storage;
    this.connections = options.connections;
    this.syncRules = options.storage.getHydratedSyncRules({ defaultSchema: this.defaultSchema });
    this.groupId = options.storage.group_id;
    this.abortSignal = options.abortSignal;
  }

  get connectionTag() {
    return this.connections.connectionTag;
  }

  private get metrics() {
    return this.options.metrics;
  }

  get connectionId() {
    const { connectionId } = this.connections;
    // Default to 1 if not set
    if (!connectionId) {
      return 1;
    }
    /**
     * This is often `"default"` (string) which will parse to `NaN`
     */
    const parsed = Number.parseInt(connectionId);
    if (isNaN(parsed)) {
      return 1;
    }
    return parsed;
  }

  get stopped() {
    return this.abortSignal.aborted;
  }

  get defaultSchema() {
    return this.connections.databaseName;
  }

  private async handleRelationSetup(
    writer: storage.BucketDataWriter,
    entity: storage.SourceEntityDescriptor,
    pattern: sync_rules.TablePattern
  ) {
    const resolvedTables = await writer.resolveTables({
      connection_id: this.connectionId,
      connection_tag: this.connectionTag,
      entity_descriptor: entity,
      pattern
    });

    // Drop conflicting tables. In the MySQL case with ObjectIds created from the table name, renames cannot be detected by the storage,
    // but changes in replication identity columns can, so this is needed.
    const dropTables = await writer.resolveTablesToDrop({
      connection_id: this.connectionId,
      connection_tag: this.connectionTag,
      entity_descriptor: entity
    });
    await writer.drop(dropTables);

    this.tableCache.set(entity.objectId!, resolvedTables);

    return resolvedTables;
  }

  async handleChangeRelation(writer: storage.BucketDataWriter, entity: storage.SourceEntityDescriptor) {
    // In common cases, there would be at most one matching pattern, since patterns
    // are de-duplicated. However, there may be multiple if:
    // 1. There is overlap with direct name matching and wildcard matching.
    // 2. There are multiple patterns with different replication config.
    const patterns = writer.rowProcessor.getMatchingTablePatterns({
      connectionTag: this.connections.connectionTag,
      schema: entity.schema,
      name: entity.name
    });

    // Drop conflicting tables. In the MySQL case with ObjectIds created from the table name, renames cannot be detected by the storage,
    // but changes in replication identity columns can, so this is needed.
    // While order of drop / snapshots shouldn't matter, tests expect drops to happen first.
    const dropTables = await writer.resolveTablesToDrop({
      connection_id: this.connectionId,
      connection_tag: this.connectionTag,
      entity_descriptor: entity
    });
    await writer.drop(dropTables);

    let allTables: SourceTable[] = [];
    for (let pattern of patterns) {
      const resolvedTables = await writer.resolveTables({
        connection_id: this.connectionId,
        connection_tag: this.connectionTag,
        entity_descriptor: entity,
        pattern
      });

      for (let table of resolvedTables) {
        // Snapshot if:
        // 1. Snapshot is not done yet, AND:
        // 2. The table is used in sync rules.
        const shouldSnapshot = !table.snapshotComplete && table.syncAny;

        if (shouldSnapshot) {
          // Truncate this table in case a previous snapshot was interrupted.
          await writer.truncate([table]);

          let gtid: common.ReplicatedGTID;
          // Start the snapshot inside a transaction.
          // We use a dedicated connection for this.
          const connection = await this.connections.getStreamingConnection();

          const promiseConnection = (connection as mysql.Connection).promise();
          try {
            await promiseConnection.query(`SET time_zone = '+00:00'`);
            await promiseConnection.query('START TRANSACTION');
            try {
              gtid = await common.readExecutedGtid(promiseConnection);
              await this.snapshotTable(connection as mysql.Connection, writer, table);
              await promiseConnection.query('COMMIT');
            } catch (e) {
              await this.tryRollback(promiseConnection);
              throw e;
            }
          } finally {
            connection.release();
          }
          const [updatedTable] = await writer.markTableSnapshotDone([table], gtid.comparable);
          allTables.push(updatedTable);
        } else {
          allTables.push(table);
        }
      }
    }

    // Since we create the objectId ourselves, this is always defined
    this.tableCache.set(entity.objectId!, allTables);
    return allTables;
  }

  async getQualifiedTableNames(
    writer: storage.BucketDataWriter,
    tablePattern: sync_rules.TablePattern
  ): Promise<storage.SourceTable[]> {
    if (tablePattern.connectionTag != this.connectionTag) {
      return [];
    }

    const connection = await this.connections.getConnection();
    const matchedTables: string[] = await common.getTablesFromPattern(connection, tablePattern);
    connection.release();

    const allTables: storage.SourceTable[] = [];
    for (const matchedTable of matchedTables) {
      const replicaIdColumns = await this.getReplicaIdColumns(matchedTable, tablePattern.schema);

      const resolvedTables = await this.handleRelationSetup(
        writer,
        {
          name: matchedTable,
          schema: tablePattern.schema,
          objectId: createTableId(tablePattern.schema, matchedTable),
          replicaIdColumns: replicaIdColumns
        },
        tablePattern
      );

      allTables.push(...resolvedTables);
    }
    return allTables;
  }

  /**
   * Checks if the initial sync has already been completed
   */
  protected async checkInitialReplicated(): Promise<boolean> {
    const status = await this.storage.getStatus();
    const lastKnowGTID = status.checkpoint_lsn ? common.ReplicatedGTID.fromSerialized(status.checkpoint_lsn) : null;
    if (status.snapshot_done && status.checkpoint_lsn) {
      this.logger.info(`Initial replication already done.`);

      if (lastKnowGTID) {
        // Check if the specific binlog file is still available. If it isn't, we need to snapshot again.
        const connection = await this.connections.getConnection();
        try {
          const isAvailable = await common.isBinlogStillAvailable(connection, lastKnowGTID.position.filename);
          if (!isAvailable) {
            this.logger.info(
              `BinLog file ${lastKnowGTID.position.filename} is no longer available, starting initial replication again.`
            );
          }
          return isAvailable;
        } finally {
          connection.release();
        }
      }

      return true;
    }

    return false;
  }

  /**
   * Does the initial replication of the database tables.
   *
   * If (partial) replication was done before on this slot, this clears the state
   * and starts again from scratch.
   */
  async startInitialReplication() {
    await this.storage.clear({ signal: this.abortSignal });
    // Replication will be performed in a single transaction on this connection
    const connection = await this.connections.getStreamingConnection();
    const promiseConnection = (connection as mysql.Connection).promise();
    const headGTID = await common.readExecutedGtid(promiseConnection);
    this.logger.info(`Using snapshot checkpoint GTID: '${headGTID}'`);
    let lastOp: InternalOpId | null = null;
    try {
      this.logger.info(`Starting initial replication`);
      await promiseConnection.query<mysqlPromise.RowDataPacket[]>(
        'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
      );
      await promiseConnection.query<mysqlPromise.RowDataPacket[]>('START TRANSACTION');
      await promiseConnection.query(`SET time_zone = '+00:00'`);

      const sourceTables = this.syncRules.getSourceTables();
      await using writer = await this.storage.createWriter({
        logger: this.logger,
        zeroLSN: common.ReplicatedGTID.ZERO.comparable,
        defaultSchema: this.defaultSchema,
        storeCurrentData: true
      });
      for (let tablePattern of sourceTables) {
        const tables = await this.getQualifiedTableNames(writer, tablePattern);
        for (let table of tables) {
          await this.snapshotTable(connection as mysql.Connection, writer, table);
          await writer.markTableSnapshotDone([table], headGTID.comparable);
          await framework.container.probes.touch();
        }
      }
      const snapshotDoneGtid = await common.readExecutedGtid(promiseConnection);
      await writer.markAllSnapshotDone(snapshotDoneGtid.comparable);
      const flushResults = await writer.flush();
      await writer.commit(headGTID.comparable);

      lastOp = flushResults?.flushed_op ?? null;
      this.logger.info(`Initial replication done`);
      await promiseConnection.query('COMMIT');
    } catch (e) {
      await this.tryRollback(promiseConnection);
      throw e;
    } finally {
      connection.release();
    }

    if (lastOp != null) {
      // Populate the cache _after_ initial replication, but _before_ we switch to this sync rules.
      await this.storage.populatePersistentChecksumCache({
        // No checkpoint yet, but we do have the opId.
        maxOpId: lastOp,
        signal: this.abortSignal
      });
    }
  }

  private async snapshotTable(
    connection: mysql.Connection,
    writer: storage.BucketDataWriter,
    table: storage.SourceTable
  ) {
    this.logger.info(`Replicating ${qualifiedMySQLTable(table)}`);
    // TODO count rows and log progress at certain batch sizes

    // MAX_EXECUTION_TIME(0) hint disables execution timeout for this query
    const query = connection.query(`SELECT /*+ MAX_EXECUTION_TIME(0) */ * FROM ${qualifiedMySQLTable(table)}`);
    const stream = query.stream();

    let columns: Map<string, ColumnDescriptor> | undefined = undefined;
    stream.on('fields', (fields: mysql.FieldPacket[]) => {
      // Map the columns and their types
      columns = common.toColumnDescriptors(fields);
    });

    for await (let row of stream) {
      if (this.stopped) {
        throw new ReplicationAbortedError(
          'Abort signal received - initial replication interrupted.',
          this.abortSignal.reason
        );
      }

      if (columns == null) {
        throw new ReplicationAssertionError(`No 'fields' event emitted`);
      }

      const record = this.toSQLiteRow(row, columns!);
      await writer.save({
        tag: storage.SaveOperationTag.INSERT,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: record,
        afterReplicaId: getUuidReplicaIdentityBson(record, table.replicaIdColumns)
      });

      this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
    }
    await writer.flush();
  }

  async replicate() {
    try {
      // If anything errors here, the entire replication process is halted, and
      // all connections automatically closed, including this one.
      await this.initReplication();
      await this.streamChanges();
      this.logger.info('BinLogStream has been shut down');
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    }
  }

  async initReplication() {
    const connection = await this.connections.getConnection();
    const errors = await common.checkSourceConfiguration(connection);
    connection.release();

    if (errors.length > 0) {
      throw new BinlogConfigurationError(`BinLog Configuration Errors: ${errors.join(', ')}`);
    }

    const initialReplicationCompleted = await this.checkInitialReplicated();
    if (!initialReplicationCompleted) {
      await this.startInitialReplication();
    } else {
      // We need to find the existing tables, to populate our table cache.
      // This is needed for includeSchema to work correctly.
      const sourceTables = this.syncRules.getSourceTables();
      await using writer = await this.storage.createWriter({
        logger: this.logger,
        zeroLSN: common.ReplicatedGTID.ZERO.comparable,
        defaultSchema: this.defaultSchema,
        storeCurrentData: true
      });
      for (let tablePattern of sourceTables) {
        await this.getQualifiedTableNames(writer, tablePattern);
      }
    }
  }

  private getTables(tableId: string): storage.SourceTable[] {
    const tables = this.tableCache.get(tableId);
    if (tables == null) {
      // We should always receive a replication message before the relation is used.
      // If we can't find it, it's a bug.
      throw new ReplicationAssertionError(`Missing relation cache for ${tableId}`);
    }
    return tables;
  }

  async streamChanges() {
    const serverId = createRandomServerId(this.storage.group_id);

    const connection = await this.connections.getConnection();
    const { checkpoint_lsn } = await this.storage.getStatus();
    if (checkpoint_lsn) {
      this.logger.info(`Existing checkpoint found: ${checkpoint_lsn}`);
    }
    const fromGTID = checkpoint_lsn
      ? common.ReplicatedGTID.fromSerialized(checkpoint_lsn)
      : await common.readExecutedGtid(connection);
    connection.release();

    if (!this.stopped) {
      await using writer = await this.storage.createWriter({
        zeroLSN: common.ReplicatedGTID.ZERO.comparable,
        defaultSchema: this.defaultSchema,
        storeCurrentData: true
      });

      const binlogEventHandler = this.createBinlogEventHandler(writer);
      const binlogListener = new BinLogListener({
        logger: this.logger,
        sourceTables: this.syncRules.getSourceTables(),
        startGTID: fromGTID,
        connectionManager: this.connections,
        serverId: serverId,
        eventHandler: binlogEventHandler
      });

      this.abortSignal.addEventListener(
        'abort',
        async () => {
          await binlogListener.stop();
        },
        { once: true }
      );

      await binlogListener.start();
      await binlogListener.replicateUntilStopped();
    }
  }

  private createBinlogEventHandler(writer: storage.BucketDataWriter): BinLogEventHandler {
    return {
      onWrite: async (rows: Row[], tableMap: TableMapEntry) => {
        await this.writeChanges(writer, {
          type: storage.SaveOperationTag.INSERT,
          rows: rows,
          tableEntry: tableMap
        });
      },

      onUpdate: async (rowsAfter: Row[], rowsBefore: Row[], tableMap: TableMapEntry) => {
        await this.writeChanges(writer, {
          type: storage.SaveOperationTag.UPDATE,
          rows: rowsAfter,
          rows_before: rowsBefore,
          tableEntry: tableMap
        });
      },
      onDelete: async (rows: Row[], tableMap: TableMapEntry) => {
        await this.writeChanges(writer, {
          type: storage.SaveOperationTag.DELETE,
          rows: rows,
          tableEntry: tableMap
        });
      },
      onKeepAlive: async (lsn: string) => {
        const didCommit = await writer.keepalive(lsn);
        if (didCommit) {
          this.oldestUncommittedChange = null;
        }
      },
      onCommit: async (lsn: string) => {
        this.metrics.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED).add(1);
        const didCommit = await writer.commit(lsn, { oldestUncommittedChange: this.oldestUncommittedChange });
        if (didCommit) {
          this.oldestUncommittedChange = null;
          this.isStartingReplication = false;
        }
      },
      onTransactionStart: async (options) => {
        if (this.oldestUncommittedChange == null) {
          this.oldestUncommittedChange = options.timestamp;
        }
      },
      onRotate: async () => {
        this.isStartingReplication = false;
      },
      onSchemaChange: async (change: SchemaChange) => {
        await this.handleSchemaChange(writer, change);
      }
    };
  }

  private async handleSchemaChange(writer: storage.BucketDataWriter, change: SchemaChange): Promise<void> {
    if (change.type === SchemaChangeType.RENAME_TABLE) {
      const fromTableId = createTableId(change.schema, change.table);

      // FIXME: we should use tables from the storage, not from the cache.
      const fromTables = this.tableCache.get(fromTableId);
      // Old table needs to be cleaned up
      if (fromTables != null) {
        await writer.drop(fromTables);
        this.tableCache.delete(fromTableId);
      }

      // The new table matched a table in the sync rules
      if (change.newTable) {
        await this.handleCreateOrUpdateTable(writer, change.newTable!, change.schema);
      }
    } else {
      const tableId = createTableId(change.schema, change.table);

      const tables = this.getTables(tableId);

      switch (change.type) {
        case SchemaChangeType.ALTER_TABLE_COLUMN:
        case SchemaChangeType.REPLICATION_IDENTITY:
          // For these changes, we need to update the table if the replication identity columns have changed.
          await this.handleCreateOrUpdateTable(writer, change.table, change.schema);
          break;
        case SchemaChangeType.TRUNCATE_TABLE:
          await writer.truncate(tables);
          break;
        case SchemaChangeType.DROP_TABLE:
          await writer.drop(tables);
          this.tableCache.delete(tableId);
          break;
        default:
          // No action needed for other schema changes
          break;
      }
    }
  }

  private async getReplicaIdColumns(tableName: string, schema: string) {
    const connection = await this.connections.getConnection();
    const replicaIdColumns = await common.getReplicationIdentityColumns({
      connection,
      schema,
      tableName
    });
    connection.release();

    return replicaIdColumns.columns;
  }

  private async handleCreateOrUpdateTable(
    writer: storage.BucketDataWriter,
    tableName: string,
    schema: string
  ): Promise<SourceTable[]> {
    const replicaIdColumns = await this.getReplicaIdColumns(tableName, schema);
    return await this.handleChangeRelation(writer, {
      name: tableName,
      schema: schema,
      objectId: createTableId(schema, tableName),
      replicaIdColumns: replicaIdColumns
    });
  }

  private async writeChanges(
    writer: storage.BucketDataWriter,
    msg: {
      type: storage.SaveOperationTag;
      rows: Row[];
      rows_before?: Row[];
      tableEntry: TableMapEntry;
    }
  ): Promise<storage.FlushedResult | null> {
    const columns = common.toColumnDescriptors(msg.tableEntry);
    const tableId = createTableId(msg.tableEntry.parentSchema, msg.tableEntry.tableName);

    let tables = this.tableCache.get(tableId);
    if (tables == null) {
      // This is an insert for a new table that matches a table in the sync rules
      // We need to create the table in the storage and cache it.
      tables = await this.handleCreateOrUpdateTable(writer, msg.tableEntry.tableName, msg.tableEntry.parentSchema);
    }

    for (const [index, row] of msg.rows.entries()) {
      for (let table of tables) {
        await this.writeChange(writer, {
          type: msg.type,
          database: msg.tableEntry.parentSchema,
          sourceTable: table!,
          table: msg.tableEntry.tableName,
          columns: columns,
          row: row,
          previous_row: msg.rows_before?.[index]
        });
      }
    }
    return null;
  }

  private toSQLiteRow(row: Record<string, any>, columns: Map<string, ColumnDescriptor>): sync_rules.SqliteRow {
    const inputRecord = common.toSQLiteRow(row, columns);
    return this.syncRules.applyRowContext<never>(inputRecord);
  }

  private async writeChange(
    writer: storage.BucketDataWriter,
    payload: WriteChangePayload
  ): Promise<storage.FlushedResult | null> {
    switch (payload.type) {
      case storage.SaveOperationTag.INSERT:
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        const record = this.toSQLiteRow(payload.row, payload.columns);
        return await writer.save({
          tag: storage.SaveOperationTag.INSERT,
          sourceTable: payload.sourceTable,
          before: undefined,
          beforeReplicaId: undefined,
          after: record,
          afterReplicaId: getUuidReplicaIdentityBson(record, payload.sourceTable.replicaIdColumns)
        });
      case storage.SaveOperationTag.UPDATE:
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        // The previous row may be null if the replica id columns are unchanged.
        // It's fine to treat that the same as an insert.
        const beforeUpdated = payload.previous_row
          ? this.toSQLiteRow(payload.previous_row, payload.columns)
          : undefined;
        const after = this.toSQLiteRow(payload.row, payload.columns);

        return await writer.save({
          tag: storage.SaveOperationTag.UPDATE,
          sourceTable: payload.sourceTable,
          before: beforeUpdated,
          beforeReplicaId: beforeUpdated
            ? getUuidReplicaIdentityBson(beforeUpdated, payload.sourceTable.replicaIdColumns)
            : undefined,
          after: after,
          afterReplicaId: getUuidReplicaIdentityBson(after, payload.sourceTable.replicaIdColumns)
        });

      case storage.SaveOperationTag.DELETE:
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        const beforeDeleted = this.toSQLiteRow(payload.row, payload.columns);

        return await writer.save({
          tag: storage.SaveOperationTag.DELETE,
          sourceTable: payload.sourceTable,
          before: beforeDeleted,
          beforeReplicaId: getUuidReplicaIdentityBson(beforeDeleted, payload.sourceTable.replicaIdColumns),
          after: undefined,
          afterReplicaId: undefined
        });
      default:
        return null;
    }
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    if (this.oldestUncommittedChange == null) {
      if (this.isStartingReplication) {
        // We don't have anything to compute replication lag with yet.
        return undefined;
      } else {
        // We don't have any uncommitted changes, so replication is up to date.
        return 0;
      }
    }
    return Date.now() - this.oldestUncommittedChange.getTime();
  }

  async tryRollback(promiseConnection: mysqlPromise.Connection) {
    try {
      await promiseConnection.query('ROLLBACK');
    } catch (e) {
      this.logger.error('Failed to rollback transaction', e);
    }
  }
}
