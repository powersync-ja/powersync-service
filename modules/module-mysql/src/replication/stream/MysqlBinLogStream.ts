import { logger } from '@powersync/lib-services-framework';
import * as sync_rules from '@powersync/service-sync-rules';
import async from 'async';

import { framework, storage } from '@powersync/service-core';
import mysql, { RowDataPacket } from 'mysql2/promise';

import ZongJi, { BinLogEvent } from '@vlasky/zongji';
import * as common from '../common/common-index.js';
import { NormalizedMySQLConnectionConfig } from '../types/types.js';
import * as zongji_utils from './zongji/zongji-utils.js';

export interface BinLogStreamOptions {
  pool: mysql.Pool;
  connection_config: NormalizedMySQLConnectionConfig;
  storage: storage.SyncRulesBucketStorage;
  abort_signal: AbortSignal;
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
  sync_rules: sync_rules.SqlSyncRules;
  group_id: number;

  private readonly storage: storage.SyncRulesBucketStorage;

  private abort_signal: AbortSignal;

  private pool: mysql.Pool;
  private relation_cache = new Map<string | number, storage.SourceTable>();

  constructor(protected options: BinLogStreamOptions) {
    this.storage = options.storage;
    this.sync_rules = options.storage.sync_rules;
    this.group_id = options.storage.group_id;
    this.pool = options.pool;

    this.abort_signal = options.abort_signal;
  }

  get connectionTag() {
    return this.options.connection_config.tag;
  }

  get connectionId() {
    // TODO this is currently hardcoded to 1 in most places
    // return this.options.connection_config.id;
    return 1;
  }

  get stopped() {
    return this.abort_signal.aborted;
  }

  async handleRelation(batch: storage.BucketStorageBatch, entity: storage.SourceEntityDescriptor, snapshot: boolean) {
    const result = await this.storage.resolveTable({
      group_id: this.group_id,
      connection_id: this.connectionId,
      connection_tag: this.connectionTag,
      entity_descriptor: entity,
      sync_rules: this.sync_rules
    });

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
      const connection = await this.pool.getConnection();
      try {
        await connection.query('BEGIN');
        try {
          gtid = await common.readMasterGtid(this.pool);
          await this.snapshotTable(batch, connection, result.table);
          await connection.query('COMMIT');
        } catch (e) {
          await connection.query('ROLLBACK');
          throw e;
        }
      } finally {
        await connection.end();
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
      const result = await this.pool.query<RowDataPacket[]>(
        `SELECT TABLE_NAME
FROM information_schema.tables
WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE ?;
`,
        [tablePattern.schema, tablePattern.tablePattern]
      );
      tableRows = result[0];
    } else {
      const result = await this.pool.query<RowDataPacket[]>(
        `SELECT TABLE_NAME
FROM information_schema.tables
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;
`,
        [tablePattern.schema, tablePattern.tablePattern]
      );
      tableRows = result[0];
    }
    let result: storage.SourceTable[] = [];

    for (let row of tableRows) {
      const name = row['TABLE_NAME'] as string;
      if (prefix && !name.startsWith(prefix)) {
        continue;
      }

      const rs = await this.pool.query<RowDataPacket[]>(
        `SELECT 1
FROM information_schema.tables
WHERE table_schema = ? AND table_name = ?
AND table_type = 'BASE TABLE';`,
        [tablePattern.schema, tablePattern.name]
      );
      if (rs[0].length == 0) {
        logger.info(`Skipping ${tablePattern.schema}.${name} - no table exists/is not a base table`);
        continue;
      }

      const cresult = await common.getReplicationIdentityColumns({
        db: this.pool,
        schema: tablePattern.schema,
        table_name: tablePattern.name
      });

      const table = await this.handleRelation(
        batch,
        {
          name,
          schema: tablePattern.schema,
          objectId: getMysqlRelId(tablePattern),
          replicationColumns: cresult.columns
        },
        false
      );

      result.push(table);
    }
    return result;
  }

  /**
   * Checks if the initial sync has been completed yet.
   */
  protected async checkInitialReplicated(): Promise<boolean> {
    await common.checkSourceConfiguration(this.pool);

    const status = await this.storage.getStatus();
    if (status.snapshot_done && status.checkpoint_lsn) {
      logger.info(`Initial replication already done. MySQL appears healthy`);
      return true;
    }
    return false;
  }

  /**
   * Snapshots initial tables
   *
   * If (partial) replication was done before on this slot, this clears the state
   * and starts again from scratch.
   */
  async replicateInitial() {
    await this.storage.clear();
    const headGTID = await common.readMasterGtid(this.pool);
    logger.info(`Using GTID:: '${headGTID}'`);
    await this.pool.query('BEGIN');
    try {
      logger.info(`Starting initial replication`);
      const sourceTables = this.sync_rules.getSourceTables();
      await this.storage.startBatch({}, async (batch) => {
        for (let tablePattern of sourceTables) {
          const tables = await this.getQualifiedTableNames(batch, tablePattern);
          for (let table of tables) {
            await this.snapshotTable(batch, table);
            await batch.markSnapshotDone([table], headGTID.comparable);
            await framework.container.probes.touch();
          }
        }
        await batch.commit(headGTID.comparable);
      });
      logger.info(`Initial replication done`);
      await this.pool.query('COMMIT');
    } catch (e) {
      await this.pool.query('ROLLBACK');
      throw e;
    }
  }

  private async snapshotTable(
    batch: storage.BucketStorageBatch,
    connection: mysql.Connection,
    table: storage.SourceTable
  ) {
    logger.info(`Replicating ${table.qualifiedName}`);
    // TODO this could be large. Should we chunk this
    const [rows] = await connection.query<mysql.RowDataPacket[]>(`SELECT * FROM ${table.schema}.${table.table}`);
    for (const record of rows) {
      await batch.save({
        tag: storage.SaveOperationTag.INSERT,
        sourceTable: table,
        before: undefined,
        after: common.toSQLiteRow(record)
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
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    }
  }

  async initReplication() {
    const initialReplicationCompleted = await this.checkInitialReplicated();

    const sourceTables = this.sync_rules.getSourceTables();
    await this.storage.startBatch({}, async (batch) => {
      for (let tablePattern of sourceTables) {
        await this.getQualifiedTableNames(batch, tablePattern);
      }
    });

    if (!initialReplicationCompleted) {
      await this.replicateInitial();
    }
  }

  async streamChanges() {
    // Auto-activate as soon as initial replication is done
    await this.storage.autoActivate();

    const { checkpoint_lsn } = await this.storage.getStatus();
    const fromGTID = checkpoint_lsn
      ? common.ReplicatedGTID.fromSerialized(checkpoint_lsn)
      : await common.readMasterGtid(this.pool);
    const binLogPositionState = fromGTID.position;

    await this.storage.startBatch({}, async (batch) => {
      const zongji = new ZongJi({
        host: this.options.connection_config.hostname,
        user: this.options.connection_config.username,
        password: this.options.connection_config.password
      });

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
              // TODO cleanup
              sourceTable: (
                await this.storage.resolveTable({
                  connection_id: this.connectionId,
                  connection_tag: this.connectionTag,
                  entity_descriptor: {
                    name: writeTableInfo.tableName,
                    objectId: evt.tableId,
                    schema: writeTableInfo.parentSchema,
                    replicationColumns: writeTableInfo.columns.map((c: any, index: number) => ({
                      name: c.name,
                      type: writeTableInfo.columnSchemas[index].COLUMN_TYPE
                    }))
                  },
                  group_id: this.group_id,
                  sync_rules: this.sync_rules
                })
              ).table
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
              // TODO cleanup
              sourceTable: (
                await this.storage.resolveTable({
                  connection_id: this.connectionId,
                  connection_tag: this.connectionTag,
                  entity_descriptor: {
                    name: updateTableInfo.tableName,
                    objectId: evt.tableId,
                    schema: updateTableInfo.parentSchema,
                    replicationColumns: updateTableInfo.columns.map((c: any, index: number) => ({
                      name: c.name,
                      type: updateTableInfo.columnSchemas[index].COLUMN_TYPE
                    }))
                  },
                  group_id: this.group_id,
                  sync_rules: this.sync_rules
                })
              ).table
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
              sourceTable: (
                await this.storage.resolveTable({
                  connection_id: this.connectionId,
                  connection_tag: this.connectionTag,
                  entity_descriptor: {
                    name: deleteTableInfo.tableName,
                    objectId: evt.tableId,
                    schema: deleteTableInfo.parentSchema,
                    replicationColumns: deleteTableInfo.columns.map((c: any, index: number) => ({
                      name: c.name,
                      type: deleteTableInfo.columnSchemas[index].COLUMN_TYPE
                    }))
                  },
                  group_id: this.group_id,
                  sync_rules: this.sync_rules
                })
              ).table
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
            // TODO update other metrics
            break;
        }
      }, 1);

      zongji.on('binlog', (evt: any) => {
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
        this.abort_signal.addEventListener(
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
    });
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
        return await batch.save({
          tag: storage.SaveOperationTag.INSERT,
          sourceTable: msg.sourceTable,
          before: undefined,
          after: common.toSQLiteRow(msg.data)
        });
      case storage.SaveOperationTag.UPDATE:
        return await batch.save({
          tag: storage.SaveOperationTag.UPDATE,
          sourceTable: msg.sourceTable,
          before: msg.previous_data ? common.toSQLiteRow(msg.previous_data) : undefined,
          after: common.toSQLiteRow(msg.data)
        });

      case storage.SaveOperationTag.DELETE:
        return await batch.save({
          tag: storage.SaveOperationTag.DELETE,
          sourceTable: msg.sourceTable,
          before: common.toSQLiteRow(msg.data),
          after: undefined
        });
      default:
        return null;
    }
  }
}
