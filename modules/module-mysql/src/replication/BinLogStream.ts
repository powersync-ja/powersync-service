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
  MetricsEngine,
  SourceTable,
  storage
} from '@powersync/service-core';
import mysql from 'mysql2';
import mysqlPromise from 'mysql2/promise';

import { TableMapEntry } from '@powersync/mysql-zongji';
import * as common from '../common/common-index.js';
import { createRandomServerId, qualifiedMySQLTable, retriedQuery } from '../utils/mysql-utils.js';
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

interface MysqlRelId {
  schema: string;
  name: string;
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
 * MySQL does not have same relation structure. Just returning unique key as string.
 * @param source
 */
function getMysqlRelId(source: MysqlRelId): string {
  return `${source.schema}.${source.name}`;
}

export async function sendKeepAlive(connection: mysqlPromise.Connection) {
  await retriedQuery({ connection: connection, query: `XA START 'powersync_keepalive'` });
  await retriedQuery({ connection: connection, query: `XA END 'powersync_keepalive'` });
  await retriedQuery({ connection: connection, query: `XA PREPARE 'powersync_keepalive'` });
  await retriedQuery({ connection: connection, query: `XA COMMIT 'powersync_keepalive'` });
}

export class BinLogStream {
  private readonly syncRules: sync_rules.SqlSyncRules;
  private readonly groupId: number;

  private readonly storage: storage.SyncRulesBucketStorage;

  private readonly connections: MySQLConnectionManager;

  private abortSignal: AbortSignal;

  private tableCache = new Map<string | number, storage.SourceTable>();

  private logger: Logger;

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
    this.syncRules = options.storage.getParsedSyncRules({ defaultSchema: this.defaultSchema });
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

  async handleRelation(batch: storage.BucketStorageBatch, entity: storage.SourceEntityDescriptor, snapshot: boolean) {
    const result = await this.storage.resolveTable({
      group_id: this.groupId,
      connection_id: this.connectionId,
      connection_tag: this.connectionTag,
      entity_descriptor: entity,
      sync_rules: this.syncRules
    });
    // objectId is always defined for mysql
    this.tableCache.set(entity.objectId!, result.table);

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
          await this.snapshotTable(connection as mysql.Connection, batch, result.table);
          await promiseConnection.query('COMMIT');
        } catch (e) {
          await this.tryRollback(promiseConnection);
          throw e;
        }
      } finally {
        connection.release();
      }
      const [table] = await batch.markSnapshotDone([result.table], gtid.comparable);
      return table;
    }

    return result.table;
  }

  async getQualifiedTableNames(
    batch: storage.BucketStorageBatch,
    tablePattern: sync_rules.TablePattern
  ): Promise<storage.SourceTable[]> {
    if (tablePattern.connectionTag != this.connectionTag) {
      return [];
    }

    const connection = await this.connections.getConnection();
    const matchedTables: string[] = await common.getTablesFromPattern(connection, tablePattern);
    connection.release();

    let tables: storage.SourceTable[] = [];
    for (const matchedTable of matchedTables) {
      const replicaIdColumns = await this.getReplicaIdColumns(matchedTable, tablePattern.schema);

      const table = await this.handleRelation(
        batch,
        {
          name: matchedTable,
          schema: tablePattern.schema,
          objectId: getMysqlRelId({
            schema: tablePattern.schema,
            name: matchedTable
          }),
          replicaIdColumns: replicaIdColumns
        },
        false
      );

      tables.push(table);
    }
    return tables;
  }

  /**
   * Checks if the initial sync has been completed yet.
   */
  protected async checkInitialReplicated(): Promise<boolean> {
    const status = await this.storage.getStatus();
    const lastKnowGTID = status.checkpoint_lsn ? common.ReplicatedGTID.fromSerialized(status.checkpoint_lsn) : null;
    if (status.snapshot_done && status.checkpoint_lsn) {
      this.logger.info(`Initial replication already done.`);

      if (lastKnowGTID) {
        // Check if the binlog is still available. If it isn't we need to snapshot again.
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
    try {
      this.logger.info(`Starting initial replication`);
      await promiseConnection.query<mysqlPromise.RowDataPacket[]>(
        'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
      );
      await promiseConnection.query<mysqlPromise.RowDataPacket[]>('START TRANSACTION');
      await promiseConnection.query(`SET time_zone = '+00:00'`);

      const sourceTables = this.syncRules.getSourceTables();
      await this.storage.startBatch(
        {
          logger: this.logger,
          zeroLSN: common.ReplicatedGTID.ZERO.comparable,
          defaultSchema: this.defaultSchema,
          storeCurrentData: true
        },
        async (batch) => {
          for (let tablePattern of sourceTables) {
            const tables = await this.getQualifiedTableNames(batch, tablePattern);
            for (let table of tables) {
              await this.snapshotTable(connection as mysql.Connection, batch, table);
              await batch.markSnapshotDone([table], headGTID.comparable);
              await framework.container.probes.touch();
            }
          }
          await batch.commit(headGTID.comparable);
        }
      );
      this.logger.info(`Initial replication done`);
      await promiseConnection.query('COMMIT');
    } catch (e) {
      await this.tryRollback(promiseConnection);
      throw e;
    } finally {
      connection.release();
    }
  }

  private async snapshotTable(
    connection: mysql.Connection,
    batch: storage.BucketStorageBatch,
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
        throw new ReplicationAbortedError('Abort signal received - initial replication interrupted.');
      }

      if (columns == null) {
        throw new ReplicationAssertionError(`No 'fields' event emitted`);
      }

      const record = common.toSQLiteRow(row, columns!);
      await batch.save({
        tag: storage.SaveOperationTag.INSERT,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: record,
        afterReplicaId: getUuidReplicaIdentityBson(record, table.replicaIdColumns)
      });

      this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
    }
    await batch.flush();
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
      await this.storage.startBatch(
        {
          logger: this.logger,
          zeroLSN: common.ReplicatedGTID.ZERO.comparable,
          defaultSchema: this.defaultSchema,
          storeCurrentData: true
        },
        async (batch) => {
          for (let tablePattern of sourceTables) {
            await this.getQualifiedTableNames(batch, tablePattern);
          }
        }
      );
    }
  }

  private getTable(tableId: string): storage.SourceTable {
    const table = this.tableCache.get(tableId);
    if (table == null) {
      // We should always receive a replication message before the relation is used.
      // If we can't find it, it's a bug.
      throw new ReplicationAssertionError(`Missing relation cache for ${tableId}`);
    }
    return table;
  }

  async streamChanges() {
    // Auto-activate as soon as initial replication is done
    await this.storage.autoActivate();
    const serverId = createRandomServerId(this.storage.group_id);

    const connection = await this.connections.getConnection();
    const { checkpoint_lsn } = await this.storage.getStatus();
    if (checkpoint_lsn) {
      this.logger.info(`Existing checkpoint found: ${checkpoint_lsn}`);
    }
    const fromGTID = checkpoint_lsn
      ? common.ReplicatedGTID.fromSerialized(checkpoint_lsn)
      : await common.readExecutedGtid(connection);
    const binLogPositionState = fromGTID.position;
    connection.release();

    if (!this.stopped) {
      await this.storage.startBatch(
        { zeroLSN: common.ReplicatedGTID.ZERO.comparable, defaultSchema: this.defaultSchema, storeCurrentData: true },
        async (batch) => {
          const binlogEventHandler = this.createBinlogEventHandler(batch);
          const binlogListener = new BinLogListener({
            logger: this.logger,
            sourceTables: this.syncRules.getSourceTables(),
            startPosition: binLogPositionState,
            connectionManager: this.connections,
            serverId: serverId,
            eventHandler: binlogEventHandler
          });

          this.abortSignal.addEventListener(
            'abort',
            async () => {
              this.logger.info('Abort signal received, stopping replication...');
              await binlogListener.stop();
            },
            { once: true }
          );

          await binlogListener.start();
          await binlogListener.replicateUntilStopped();
        }
      );
    }
  }

  private createBinlogEventHandler(batch: storage.BucketStorageBatch): BinLogEventHandler {
    return {
      onWrite: async (rows: Row[], tableMap: TableMapEntry) => {
        await this.writeChanges(batch, {
          type: storage.SaveOperationTag.INSERT,
          rows: rows,
          tableEntry: tableMap
        });
      },

      onUpdate: async (rowsAfter: Row[], rowsBefore: Row[], tableMap: TableMapEntry) => {
        await this.writeChanges(batch, {
          type: storage.SaveOperationTag.UPDATE,
          rows: rowsAfter,
          rows_before: rowsBefore,
          tableEntry: tableMap
        });
      },
      onDelete: async (rows: Row[], tableMap: TableMapEntry) => {
        await this.writeChanges(batch, {
          type: storage.SaveOperationTag.DELETE,
          rows: rows,
          tableEntry: tableMap
        });
      },
      onCommit: async (lsn: string) => {
        this.metrics.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED).add(1);
        const didCommit = await batch.commit(lsn, { oldestUncommittedChange: this.oldestUncommittedChange });
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
        await this.handleSchemaChange(batch, change);
      }
    };
  }

  private async handleSchemaChange(batch: storage.BucketStorageBatch, change: SchemaChange): Promise<void> {
    if (change.type === SchemaChangeType.RENAME_TABLE) {
      const fromTableId = getMysqlRelId({
        schema: change.schema,
        name: change.table
      });

      const fromTable = this.tableCache.get(fromTableId);
      // Old table needs to be cleaned up
      if (fromTable) {
        await batch.drop([fromTable]);
        this.tableCache.delete(fromTableId);
      }
      // The new table matched a table in the sync rules
      if (change.newTable) {
        await this.handleCreateOrUpdateTable(batch, change.newTable!, change.schema);
      }
    } else {
      const tableId = getMysqlRelId({
        schema: change.schema,
        name: change.table
      });

      const table = this.getTable(tableId);

      switch (change.type) {
        case SchemaChangeType.ALTER_TABLE_COLUMN:
        case SchemaChangeType.REPLICATION_IDENTITY:
          // For these changes, we need to update the table if the replication identity columns have changed.
          await this.handleCreateOrUpdateTable(batch, change.table, change.schema);
          break;
        case SchemaChangeType.TRUNCATE_TABLE:
          await batch.truncate([table]);
          break;
        case SchemaChangeType.DROP_TABLE:
          await batch.drop([table]);
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
    batch: storage.BucketStorageBatch,
    tableName: string,
    schema: string
  ): Promise<SourceTable> {
    const replicaIdColumns = await this.getReplicaIdColumns(tableName, schema);
    return await this.handleRelation(
      batch,
      {
        name: tableName,
        schema: schema,
        objectId: getMysqlRelId({
          schema: schema,
          name: tableName
        }),
        replicaIdColumns: replicaIdColumns
      },
      true
    );
  }

  private async writeChanges(
    batch: storage.BucketStorageBatch,
    msg: {
      type: storage.SaveOperationTag;
      rows: Row[];
      rows_before?: Row[];
      tableEntry: TableMapEntry;
    }
  ): Promise<storage.FlushedResult | null> {
    const columns = common.toColumnDescriptors(msg.tableEntry);
    const tableId = getMysqlRelId({
      schema: msg.tableEntry.parentSchema,
      name: msg.tableEntry.tableName
    });

    let table = this.tableCache.get(tableId);
    if (table == null) {
      // This write event is for a new table that matches a table in the sync rules
      // We need to create the table in the storage and cache it.
      table = await this.handleCreateOrUpdateTable(batch, msg.tableEntry.tableName, msg.tableEntry.parentSchema);
    }

    for (const [index, row] of msg.rows.entries()) {
      await this.writeChange(batch, {
        type: msg.type,
        database: msg.tableEntry.parentSchema,
        sourceTable: table!,
        table: msg.tableEntry.tableName,
        columns: columns,
        row: row,
        previous_row: msg.rows_before?.[index]
      });
    }
    return null;
  }

  private async writeChange(
    batch: storage.BucketStorageBatch,
    payload: WriteChangePayload
  ): Promise<storage.FlushedResult | null> {
    switch (payload.type) {
      case storage.SaveOperationTag.INSERT:
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        const record = common.toSQLiteRow(payload.row, payload.columns);
        return await batch.save({
          tag: storage.SaveOperationTag.INSERT,
          sourceTable: payload.sourceTable,
          before: undefined,
          beforeReplicaId: undefined,
          after: record,
          afterReplicaId: getUuidReplicaIdentityBson(record, payload.sourceTable.replicaIdColumns)
        });
      case storage.SaveOperationTag.UPDATE:
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        // "before" may be null if the replica id columns are unchanged
        // It's fine to treat that the same as an insert.
        const beforeUpdated = payload.previous_row
          ? common.toSQLiteRow(payload.previous_row, payload.columns)
          : undefined;
        const after = common.toSQLiteRow(payload.row, payload.columns);

        return await batch.save({
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
        const beforeDeleted = common.toSQLiteRow(payload.row, payload.columns);

        return await batch.save({
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
        // We don't have any uncommitted changes, so replication is up-to-date.
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
