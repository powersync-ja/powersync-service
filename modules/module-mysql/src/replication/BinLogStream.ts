import { logger, ReplicationAbortedError, ReplicationAssertionError } from '@powersync/lib-services-framework';
import * as sync_rules from '@powersync/service-sync-rules';
import async from 'async';

import { ColumnDescriptor, framework, getUuidReplicaIdentityBson, Metrics, storage } from '@powersync/service-core';
import mysql, { FieldPacket } from 'mysql2';

import { BinLogEvent, StartOptions, TableMapEntry } from '@powersync/mysql-zongji';
import mysqlPromise from 'mysql2/promise';
import * as common from '../common/common-index.js';
import { isBinlogStillAvailable, ReplicatedGTID, toColumnDescriptors } from '../common/common-index.js';
import { createRandomServerId, escapeMysqlTableName } from '../utils/mysql-utils.js';
import { MySQLConnectionManager } from './MySQLConnectionManager.js';
import * as zongji_utils from './zongji/zongji-utils.js';

export interface BinLogStreamOptions {
  connections: MySQLConnectionManager;
  storage: storage.SyncRulesBucketStorage;
  abortSignal: AbortSignal;
}

interface MysqlRelId {
  schema: string;
  name: string;
}

interface WriteChangePayload {
  type: storage.SaveOperationTag;
  data: Data;
  previous_data?: Data;
  database: string;
  table: string;
  sourceTable: storage.SourceTable;
  columns: Map<string, ColumnDescriptor>;
}

export type Data = Record<string, any>;

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

export class BinLogStream {
  private readonly syncRules: sync_rules.SqlSyncRules;
  private readonly groupId: number;

  private readonly storage: storage.SyncRulesBucketStorage;

  private readonly connections: MySQLConnectionManager;

  private abortSignal: AbortSignal;

  private tableCache = new Map<string | number, storage.SourceTable>();

  constructor(protected options: BinLogStreamOptions) {
    this.storage = options.storage;
    this.connections = options.connections;
    this.syncRules = options.storage.getParsedSyncRules({ defaultSchema: this.defaultSchema });
    this.groupId = options.storage.group_id;
    this.abortSignal = options.abortSignal;
  }

  get connectionTag() {
    return this.connections.connectionTag;
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
    this.tableCache.set(entity.objectId, result.table);

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
        await promiseConnection.query('BEGIN');
        try {
          gtid = await common.readExecutedGtid(promiseConnection);
          await this.snapshotTable(connection.connection, batch, result.table);
          await promiseConnection.query('COMMIT');
        } catch (e) {
          await promiseConnection.query('ROLLBACK');
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

    let tableRows: any[];
    const prefix = tablePattern.isWildcard ? tablePattern.tablePrefix : undefined;
    if (tablePattern.isWildcard) {
      const result = await this.connections.query(
        `SELECT TABLE_NAME
FROM information_schema.tables
WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE ?;
`,
        [tablePattern.schema, tablePattern.tablePattern]
      );
      tableRows = result[0];
    } else {
      const result = await this.connections.query(
        `SELECT TABLE_NAME
FROM information_schema.tables
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;
`,
        [tablePattern.schema, tablePattern.tablePattern]
      );
      tableRows = result[0];
    }
    let tables: storage.SourceTable[] = [];

    for (let row of tableRows) {
      const name = row['TABLE_NAME'] as string;
      if (prefix && !name.startsWith(prefix)) {
        continue;
      }

      const result = await this.connections.query(
        `SELECT 1
FROM information_schema.tables
WHERE table_schema = ? AND table_name = ?
AND table_type = 'BASE TABLE';`,
        [tablePattern.schema, tablePattern.name]
      );
      if (result[0].length == 0) {
        logger.info(`Skipping ${tablePattern.schema}.${name} - no table exists/is not a base table`);
        continue;
      }

      const connection = await this.connections.getConnection();
      const replicationColumns = await common.getReplicationIdentityColumns({
        connection: connection,
        schema: tablePattern.schema,
        table_name: tablePattern.name
      });
      connection.release();

      const table = await this.handleRelation(
        batch,
        {
          name,
          schema: tablePattern.schema,
          objectId: getMysqlRelId(tablePattern),
          replicationColumns: replicationColumns.columns
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
      logger.info(`Initial replication already done.`);

      if (lastKnowGTID) {
        // Check if the binlog is still available. If it isn't we need to snapshot again.
        const connection = await this.connections.getConnection();
        try {
          const isAvailable = await isBinlogStillAvailable(connection, lastKnowGTID.position.filename);
          if (!isAvailable) {
            logger.info(
              `Binlog file ${lastKnowGTID.position.filename} is no longer available, starting initial replication again.`
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
    await this.storage.clear();
    // Replication will be performed in a single transaction on this connection
    const connection = await this.connections.getStreamingConnection();
    const promiseConnection = (connection as mysql.Connection).promise();
    const headGTID = await common.readExecutedGtid(promiseConnection);
    logger.info(`Using snapshot checkpoint GTID: '${headGTID}'`);
    try {
      logger.info(`Starting initial replication`);
      await promiseConnection.query<mysqlPromise.RowDataPacket[]>(
        'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
      );
      await promiseConnection.query<mysqlPromise.RowDataPacket[]>('START TRANSACTION');
      await promiseConnection.query(`SET time_zone = '+00:00'`);

      const sourceTables = this.syncRules.getSourceTables();
      await this.storage.startBatch(
        { zeroLSN: ReplicatedGTID.ZERO.comparable, defaultSchema: this.defaultSchema, storeCurrentData: true },
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
      logger.info(`Initial replication done`);
      await promiseConnection.query('COMMIT');
    } catch (e) {
      await promiseConnection.query('ROLLBACK');
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
    logger.info(`Replicating ${table.qualifiedName}`);
    // TODO count rows and log progress at certain batch sizes

    // MAX_EXECUTION_TIME(0) hint disables execution timeout for this query
    const query = connection.query(`SELECT /*+ MAX_EXECUTION_TIME(0) */ * FROM ${escapeMysqlTableName(table)}`);
    const stream = query.stream();

    let columns: Map<string, ColumnDescriptor> | undefined = undefined;
    stream.on('fields', (fields: FieldPacket[]) => {
      // Map the columns and their types
      columns = toColumnDescriptors(fields);
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
    }
    await batch.flush();
  }

  async replicate() {
    try {
      // If anything errors here, the entire replication process is halted, and
      // all connections automatically closed, including this one.
      await this.initReplication();
      await this.streamChanges();
      logger.info('BinlogStream has been shut down');
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
      throw new BinlogConfigurationError(`Binlog Configuration Errors: ${errors.join(', ')}`);
    }

    const initialReplicationCompleted = await this.checkInitialReplicated();
    if (!initialReplicationCompleted) {
      await this.startInitialReplication();
    } else {
      // We need to find the existing tables, to populate our table cache.
      // This is needed for includeSchema to work correctly.
      const sourceTables = this.syncRules.getSourceTables();
      await this.storage.startBatch(
        { zeroLSN: ReplicatedGTID.ZERO.comparable, defaultSchema: this.defaultSchema, storeCurrentData: true },
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
    logger.info(`Starting replication. Created replica client with serverId:${serverId}`);

    const connection = await this.connections.getConnection();
    const { checkpoint_lsn } = await this.storage.getStatus();
    if (checkpoint_lsn) {
      logger.info(`Existing checkpoint found: ${checkpoint_lsn}`);
    }

    const fromGTID = checkpoint_lsn
      ? common.ReplicatedGTID.fromSerialized(checkpoint_lsn)
      : await common.readExecutedGtid(connection);
    const binLogPositionState = fromGTID.position;
    connection.release();

    if (!this.stopped) {
      await this.storage.startBatch(
        { zeroLSN: ReplicatedGTID.ZERO.comparable, defaultSchema: this.defaultSchema, storeCurrentData: true },
        async (batch) => {
          const zongji = this.connections.createBinlogListener();

          let currentGTID: common.ReplicatedGTID | null = null;

          const queue = async.queue(async (evt: BinLogEvent) => {
            // State machine
            switch (true) {
              case zongji_utils.eventIsGTIDLog(evt):
                currentGTID = common.ReplicatedGTID.fromBinLogEvent({
                  raw_gtid: {
                    server_id: evt.serverId,
                    transaction_range: evt.transactionRange
                  },
                  position: {
                    filename: binLogPositionState.filename,
                    offset: evt.nextPosition
                  }
                });
                break;
              case zongji_utils.eventIsRotation(evt):
                // Update the position
                binLogPositionState.filename = evt.binlogName;
                binLogPositionState.offset = evt.position;
                break;
              case zongji_utils.eventIsWriteMutation(evt):
                const writeTableInfo = evt.tableMap[evt.tableId];
                await this.writeChanges(batch, {
                  type: storage.SaveOperationTag.INSERT,
                  data: evt.rows,
                  tableEntry: writeTableInfo
                });
                break;
              case zongji_utils.eventIsUpdateMutation(evt):
                const updateTableInfo = evt.tableMap[evt.tableId];
                await this.writeChanges(batch, {
                  type: storage.SaveOperationTag.UPDATE,
                  data: evt.rows.map((row) => row.after),
                  previous_data: evt.rows.map((row) => row.before),
                  tableEntry: updateTableInfo
                });
                break;
              case zongji_utils.eventIsDeleteMutation(evt):
                const deleteTableInfo = evt.tableMap[evt.tableId];
                await this.writeChanges(batch, {
                  type: storage.SaveOperationTag.DELETE,
                  data: evt.rows,
                  tableEntry: deleteTableInfo
                });
                break;
              case zongji_utils.eventIsXid(evt):
                Metrics.getInstance().transactions_replicated_total.add(1);
                // Need to commit with a replicated GTID with updated next position
                await batch.commit(
                  new common.ReplicatedGTID({
                    raw_gtid: currentGTID!.raw,
                    position: {
                      filename: binLogPositionState.filename,
                      offset: evt.nextPosition
                    }
                  }).comparable
                );
                currentGTID = null;
                // chunks_replicated_total.add(1);
                break;
            }
          }, 1);

          zongji.on('binlog', (evt: BinLogEvent) => {
            if (!this.stopped) {
              logger.info(`Received Binlog event:${evt.getEventName()}`);
              queue.push(evt);
            } else {
              logger.info(`Replication is busy stopping, ignoring event ${evt.getEventName()}`);
            }
          });

          if (this.stopped) {
            // Powersync is shutting down, don't start replicating
            return;
          }

          // Set a heartbeat interval for the Zongji replication connection
          // Zongji does not explicitly handle the heartbeat events - they are categorized as event:unknown
          // The heartbeat events are enough to keep the connection alive for setTimeout to work on the socket.
          await new Promise((resolve, reject) => {
            zongji.connection.query(
              // In nanoseconds, 10^9 = 1s
              'set @master_heartbeat_period=28*1000000000',
              function (error: any, results: any, fields: any) {
                if (error) {
                  reject(error);
                } else {
                  resolve(results);
                }
              }
            );
          });
          logger.info('Successfully set up replication connection heartbeat...');

          // The _socket member is only set after a query is run on the connection, so we set the timeout after setting the heartbeat.
          // The timeout here must be greater than the master_heartbeat_period.
          const socket = zongji.connection._socket!;
          socket.setTimeout(60_000, () => {
            socket.destroy(new Error('Replication connection timeout.'));
          });

          logger.info(`Reading binlog from: ${binLogPositionState.filename}:${binLogPositionState.offset}`);
          // Only listen for changes to tables in the sync rules
          const includedTables = [...this.tableCache.values()].map((table) => table.table);
          zongji.start({
            // We ignore the unknown/heartbeat event since it currently serves no purpose other than to keep the connection alive
            includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows', 'xid', 'rotate', 'gtidlog'],
            excludeEvents: [],
            includeSchema: { [this.defaultSchema]: includedTables },
            filename: binLogPositionState.filename,
            position: binLogPositionState.offset,
            serverId: serverId
          } satisfies StartOptions);

          // Forever young
          await new Promise<void>((resolve, reject) => {
            zongji.on('error', (error) => {
              logger.error('Binlog listener error:', error);
              zongji.stop();
              queue.kill();
              reject(error);
            });

            zongji.on('stopped', () => {
              logger.info('Binlog listener stopped. Replication ended.');
              resolve();
            });

            queue.error((error) => {
              logger.error('Binlog listener queue error:', error);
              zongji.stop();
              queue.kill();
              reject(error);
            });

            this.abortSignal.addEventListener(
              'abort',
              () => {
                logger.info('Abort signal received, stopping replication...');
                zongji.stop();
                queue.kill();
                resolve();
              },
              { once: true }
            );
          });
        }
      );
    }
  }

  private async writeChanges(
    batch: storage.BucketStorageBatch,
    msg: {
      type: storage.SaveOperationTag;
      data: Data[];
      previous_data?: Data[];
      tableEntry: TableMapEntry;
    }
  ): Promise<storage.FlushedResult | null> {
    const columns = toColumnDescriptors(msg.tableEntry);

    for (const [index, row] of msg.data.entries()) {
      await this.writeChange(batch, {
        type: msg.type,
        database: msg.tableEntry.parentSchema,
        sourceTable: this.getTable(
          getMysqlRelId({
            schema: msg.tableEntry.parentSchema,
            name: msg.tableEntry.tableName
          })
        ),
        table: msg.tableEntry.tableName,
        columns: columns,
        data: row,
        previous_data: msg.previous_data?.[index]
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
        Metrics.getInstance().rows_replicated_total.add(1);
        const record = common.toSQLiteRow(payload.data, payload.columns);
        return await batch.save({
          tag: storage.SaveOperationTag.INSERT,
          sourceTable: payload.sourceTable,
          before: undefined,
          beforeReplicaId: undefined,
          after: record,
          afterReplicaId: getUuidReplicaIdentityBson(record, payload.sourceTable.replicaIdColumns)
        });
      case storage.SaveOperationTag.UPDATE:
        Metrics.getInstance().rows_replicated_total.add(1);
        // "before" may be null if the replica id columns are unchanged
        // It's fine to treat that the same as an insert.
        const beforeUpdated = payload.previous_data
          ? common.toSQLiteRow(payload.previous_data, payload.columns)
          : undefined;
        const after = common.toSQLiteRow(payload.data, payload.columns);

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
        Metrics.getInstance().rows_replicated_total.add(1);
        const beforeDeleted = common.toSQLiteRow(payload.data, payload.columns);

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
}
