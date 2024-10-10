import { logger } from '@powersync/lib-services-framework';
import * as sync_rules from '@powersync/service-sync-rules';
import async from 'async';

import { framework, getUuidReplicaIdentityBson, storage } from '@powersync/service-core';
import mysql from 'mysql2';

import { BinLogEvent } from '@powersync/mysql-zongji';
import * as common from '../common/common-index.js';
import * as zongji_utils from './zongji/zongji-utils.js';
import { MySQLConnectionManager } from './MySQLConnectionManager.js';
import { ReplicatedGTID } from '../common/common-index.js';

export interface BinLogStreamOptions {
  connections: MySQLConnectionManager;
  storage: storage.SyncRulesBucketStorage;
  abortSignal: AbortSignal;
}

interface MysqlRelId {
  schema: string;
  name: string;
}

export type Data = Record<string, any>;

/**
 * MySQL does not have same relation structure. Just returning unique key as string.
 * @param source
 */
function getMysqlRelId(source: MysqlRelId): string {
  return `${source.schema}.${source.name}`;
}

export class MysqlBinLogStream {
  private readonly sync_rules: sync_rules.SqlSyncRules;
  private readonly group_id: number;

  private readonly storage: storage.SyncRulesBucketStorage;

  private readonly connections: MySQLConnectionManager;

  private abortSignal: AbortSignal;

  private relation_cache = new Map<string | number, storage.SourceTable>();

  constructor(protected options: BinLogStreamOptions) {
    this.storage = options.storage;
    this.connections = options.connections;
    this.sync_rules = options.storage.getParsedSyncRules({ defaultSchema: this.defaultSchema });
    this.group_id = options.storage.group_id;
    this.abortSignal = options.abortSignal;
  }

  get connectionTag() {
    return this.connections.connectionTag;
  }

  get connectionId() {
    // Default to 1 if not set
    return this.connections.connectionId ? Number.parseInt(this.connections.connectionId) : 1;
  }

  get stopped() {
    return this.abortSignal.aborted;
  }

  get defaultSchema() {
    return this.connections.databaseName;
  }

  async handleRelation(batch: storage.BucketStorageBatch, entity: storage.SourceEntityDescriptor, snapshot: boolean) {
    const result = await this.storage.resolveTable({
      group_id: this.group_id,
      connection_id: this.connectionId,
      connection_tag: this.connectionTag,
      entity_descriptor: entity,
      sync_rules: this.sync_rules
    });
    this.relation_cache.set(entity.objectId, result.table);

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
      const promiseConnection = connection.connection.promise();
      try {
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
        db: connection,
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
    if (status.snapshot_done && status.checkpoint_lsn) {
      logger.info(`Initial replication already done. MySQL appears healthy`);
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
    const promiseConnection = connection.connection.promise();
    const headGTID = await common.readExecutedGtid(promiseConnection);
    logger.info(`Using GTID:: '${headGTID}'`);
    try {
      logger.info(`Starting initial replication`);
      await promiseConnection.query('START TRANSACTION');
      await promiseConnection.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ ACCESS_MODE READ ONLY');
      const sourceTables = this.sync_rules.getSourceTables();
      await this.storage.startBatch(
        { zeroLSN: ReplicatedGTID.ZERO.comparable, defaultSchema: this.defaultSchema },
        async (batch) => {
          for (let tablePattern of sourceTables) {
            const tables = await this.getQualifiedTableNames(batch, tablePattern);
            for (let table of tables) {
              await this.snapshotTable(connection.connection, batch, table);
              await batch.markSnapshotDone([table], headGTID.comparable);
              await framework.container.probes.touch();
            }
          }
          await batch.commit(headGTID.comparable);
        }
      );
      logger.info(`Initial replication done`);
      await connection.query('COMMIT');
    } catch (e) {
      await connection.query('ROLLBACK');
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
    // TODO count rows

    return new Promise<void>((resolve, reject) => {
      // MAX_EXECUTION_TIME(0) hint disables execution timeout for this query
      connection
        .query(`SELECT /*+ MAX_EXECUTION_TIME(0) */ * FROM ${table.schema}.${table.table}`)
        .on('error', (err) => {
          reject(err);
        })
        .on('result', async (row) => {
          connection.pause();
          const record = common.toSQLiteRow(row);

          await batch.save({
            tag: storage.SaveOperationTag.INSERT,
            sourceTable: table,
            before: undefined,
            beforeReplicaId: undefined,
            after: record,
            afterReplicaId: getUuidReplicaIdentityBson(record, table.replicaIdColumns)
          });
          connection.resume();
        })
        .on('end', async function () {
          await batch.flush();
          resolve();
        });
    });
  }

  async replicate() {
    try {
      // If anything errors here, the entire replication process is halted, and
      // all connections automatically closed, including this one.
      await this.initReplication();
      await this.streamChanges();
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    }
  }

  async initReplication() {
    const connection = await this.connections.getConnection();
    await common.checkSourceConfiguration(connection);
    connection.release();

    const initialReplicationCompleted = await this.checkInitialReplicated();
    if (!initialReplicationCompleted) {
      await this.startInitialReplication();
    }
  }

  private getTable(tableId: string): storage.SourceTable {
    const table = this.relation_cache.get(tableId);
    if (table == null) {
      // We should always receive a replication message before the relation is used.
      // If we can't find it, it's a bug.
      throw new Error(`Missing relation cache for ${tableId}`);
    }
    return table;
  }

  async streamChanges() {
    // Auto-activate as soon as initial replication is done
    await this.storage.autoActivate();

    const connection = await this.connections.getConnection();
    const { checkpoint_lsn } = await this.storage.getStatus();
    const fromGTID = checkpoint_lsn
      ? common.ReplicatedGTID.fromSerialized(checkpoint_lsn)
      : await common.readExecutedGtid(connection);
    const binLogPositionState = fromGTID.position;
    connection.release();

    await this.storage.startBatch(
      { zeroLSN: ReplicatedGTID.ZERO.comparable, defaultSchema: this.defaultSchema },
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
              // TODO, can multiple tables be present?
              const writeTableInfo = evt.tableMap[evt.tableId];
              await this.writeChanges(batch, {
                type: storage.SaveOperationTag.INSERT,
                data: evt.rows,
                database: writeTableInfo.parentSchema,
                table: writeTableInfo.tableName,
                sourceTable: this.getTable(
                  getMysqlRelId({
                    schema: writeTableInfo.parentSchema,
                    name: writeTableInfo.tableName
                  })
                )
              });
              break;
            case zongji_utils.eventIsUpdateMutation(evt):
              const updateTableInfo = evt.tableMap[evt.tableId];
              await this.writeChanges(batch, {
                type: storage.SaveOperationTag.UPDATE,
                data: evt.rows.map((row) => row.after),
                previous_data: evt.rows.map((row) => row.before),
                database: updateTableInfo.parentSchema,
                table: updateTableInfo.tableName,
                sourceTable: this.getTable(
                  getMysqlRelId({
                    schema: updateTableInfo.parentSchema,
                    name: updateTableInfo.tableName
                  })
                )
              });
              break;
            case zongji_utils.eventIsDeleteMutation(evt):
              // TODO, can multiple tables be present?
              const deleteTableInfo = evt.tableMap[evt.tableId];
              await this.writeChanges(batch, {
                type: storage.SaveOperationTag.DELETE,
                data: evt.rows,
                database: deleteTableInfo.parentSchema,
                table: deleteTableInfo.tableName,
                // TODO cleanup
                sourceTable: this.getTable(
                  getMysqlRelId({
                    schema: deleteTableInfo.parentSchema,
                    name: deleteTableInfo.tableName
                  })
                )
              });
              break;
            case zongji_utils.eventIsXid(evt):
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
          queue.push(evt);
        });

        zongji.start({
          includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows', 'xid', 'rotate', 'gtidlog'],
          excludeEvents: [],
          filename: binLogPositionState.filename,
          position: binLogPositionState.offset
        });

        // Forever young
        await new Promise<void>((resolve, reject) => {
          queue.error((error) => {
            zongji.stop();
            queue.kill();
            reject(error);
          });
          this.abortSignal.addEventListener(
            'abort',
            async () => {
              zongji.stop();
              queue.kill();
              if (!queue.length) {
                await queue.drain();
              }
              resolve();
            },
            { once: true }
          );
        });
      }
    );
  }

  private async writeChanges(
    batch: storage.BucketStorageBatch,
    msg: {
      type: storage.SaveOperationTag;
      data: Data[];
      previous_data?: Data[];
      database: string;
      table: string;
      sourceTable: storage.SourceTable;
    }
  ): Promise<storage.FlushedResult | null> {
    for (const [index, row] of msg.data.entries()) {
      await this.writeChange(batch, {
        ...msg,
        data: row,
        previous_data: msg.previous_data?.[index]
      });
    }
    return null;
  }

  private async writeChange(
    batch: storage.BucketStorageBatch,
    msg: {
      type: storage.SaveOperationTag;
      data: Data;
      previous_data?: Data;
      database: string;
      table: string;
      sourceTable: storage.SourceTable;
    }
  ): Promise<storage.FlushedResult | null> {
    switch (msg.type) {
      case storage.SaveOperationTag.INSERT:
        const record = common.toSQLiteRow(msg.data);
        return await batch.save({
          tag: storage.SaveOperationTag.INSERT,
          sourceTable: msg.sourceTable,
          before: undefined,
          beforeReplicaId: undefined,
          after: record,
          afterReplicaId: getUuidReplicaIdentityBson(record, msg.sourceTable.replicaIdColumns)
        });
      case storage.SaveOperationTag.UPDATE:
        // "before" may be null if the replica id columns are unchanged
        // It's fine to treat that the same as an insert.
        const beforeUpdated = msg.previous_data ? common.toSQLiteRow(msg.previous_data) : undefined;
        const after = common.toSQLiteRow(msg.data);

        return await batch.save({
          tag: storage.SaveOperationTag.UPDATE,
          sourceTable: msg.sourceTable,
          before: beforeUpdated,
          beforeReplicaId: beforeUpdated
            ? getUuidReplicaIdentityBson(beforeUpdated, msg.sourceTable.replicaIdColumns)
            : undefined,
          after: common.toSQLiteRow(msg.data),
          afterReplicaId: getUuidReplicaIdentityBson(after, msg.sourceTable.replicaIdColumns)
        });

      case storage.SaveOperationTag.DELETE:
        const beforeDeleted = common.toSQLiteRow(msg.data);

        return await batch.save({
          tag: storage.SaveOperationTag.DELETE,
          sourceTable: msg.sourceTable,
          before: beforeDeleted,
          beforeReplicaId: getUuidReplicaIdentityBson(beforeDeleted, msg.sourceTable.replicaIdColumns),
          after: undefined,
          afterReplicaId: undefined
        });
      default:
        return null;
    }
  }
}
