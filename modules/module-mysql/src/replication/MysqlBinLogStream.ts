import { logger } from '@powersync/lib-services-framework';
import * as sync_rules from '@powersync/service-sync-rules';

import { storage } from '@powersync/service-core';

import mysql, { RowDataPacket } from 'mysql2/promise';
// @ts-expect-error
import ZongJi from '@vlasky/zongji';
import { NormalizedMySQLConnectionConfig } from '../types/types.js';
import * as replication_utils from '../utils/replication/replication-utils.js';

export interface BinLogStreamOptions {
  pool: mysql.Pool;
  connection_config: NormalizedMySQLConnectionConfig;

  factory: storage.BucketStorageFactory;
  storage: storage.SyncRulesBucketStorage;
  abort_signal: AbortSignal;
}

interface InitResult {
  needsInitialSync: boolean;
}

type Data = Record<string, any>;
type EventType = 'insert' | 'update' | 'delete';

interface DatabaseEvent {
  database: string;
  table: string;
  type: EventType;
  ts: number;
  xid: number;
  xoffset?: number;
  gtid: string;
  commit?: boolean;
  data: Data;
  old?: Data;
}

interface MysqlRelId {
  schema: string;
  name: string;
}

/**
 * MySQL does not have same relation structure. Just returning unique key as string.
 * @param source
 */
function getMysqlRelId(source: MysqlRelId): string {
  return `${source.schema}.${source.name}`;
}

async function getReplicationKeyColumns(db: mysql.Pool, relId: MysqlRelId) {
  const primaryKeyQuery = `
        SELECT s.COLUMN_NAME AS name, c.DATA_TYPE AS type
        FROM INFORMATION_SCHEMA.STATISTICS s
        JOIN INFORMATION_SCHEMA.COLUMNS c 
            ON s.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND s.TABLE_NAME = c.TABLE_NAME
            AND s.COLUMN_NAME = c.COLUMN_NAME
        WHERE s.TABLE_SCHEMA = ?
        AND s.TABLE_NAME = ?
        AND s.INDEX_NAME = 'PRIMARY'
        ORDER BY s.SEQ_IN_INDEX;
    `;
  const primaryKeyRows = await db.execute<RowDataPacket[]>(primaryKeyQuery, [relId.schema, relId.name]);

  if (primaryKeyRows[0].length > 0) {
    logger.info(`Found primary key, returning it: ${primaryKeyRows[0].reduce((a, b) => `${a}, "${b.name}"`, '')}`);
    return {
      columns: (primaryKeyRows[0] as RowDataPacket[]).map((row) => ({
        name: row.name as string,
        // Ignoring MySQL types: we should check if they are used.
        typeOid: -1
      })),
      replicationIdentity: 'default'
    };
  }

  // TODO: test code with tables with unique keys, compound key etc.
  // No primary key, find the first valid unique key
  const uniqueKeyQuery = `
        SELECT s.INDEX_NAME, s.COLUMN_NAME, c.DATA_TYPE, s.NON_UNIQUE, s.NULLABLE
        FROM INFORMATION_SCHEMA.STATISTICS s
        JOIN INFORMATION_SCHEMA.COLUMNS c
            ON s.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND s.TABLE_NAME = c.TABLE_NAME
            AND s.COLUMN_NAME = c.COLUMN_NAME
        WHERE s.TABLE_SCHEMA = ?
        AND s.TABLE_NAME = ?
        AND s.INDEX_NAME != 'PRIMARY'
        AND s.NON_UNIQUE = 0
        ORDER BY s.SEQ_IN_INDEX;
    `;
  const uniqueKeyRows = await db.execute<RowDataPacket[]>(uniqueKeyQuery, [relId.schema, relId.name]);

  const currentUniqueKey = uniqueKeyRows[0]?.[0]?.INDEX_NAME ?? '';
  const uniqueKeyColumns: RowDataPacket[] = [];
  for (const row of uniqueKeyRows[0]) {
    if (row.INDEX_NAME === currentUniqueKey) {
      uniqueKeyColumns.push(row);
    }
  }
  if (uniqueKeyColumns.length > 0) {
    logger.info('Found unique key, returning it');
    return {
      columns: uniqueKeyColumns.map((col) => ({
        name: col.COLUMN_NAME as string,
        // Ignoring MySQL types: we should check if they are used.
        typeOid: -1
      })),
      replicationIdentity: 'index'
    };
  }

  logger.info('No unique key found, returning all columns');
  const allColumnsQuery = `
        SELECT COLUMN_NAME AS name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION;
    `;
  const allColumnsRows = await db.execute<RowDataPacket[]>(allColumnsQuery, [relId.schema, relId.name]);

  return {
    columns: (allColumnsRows[0] as RowDataPacket[]).map((row) => ({
      name: row.name as string,
      typeOid: -1
    })),
    replicationIdentity: 'full'
  };
}

export class MysqlBinLogStream {
  sync_rules: sync_rules.SqlSyncRules;
  group_id: number;

  connection_id = 1;

  private readonly storage: storage.SyncRulesBucketStorage;

  private slot_name: string;

  private abort_signal: AbortSignal;

  private pool: mysql.Pool;
  private relation_cache = new Map<string | number, storage.SourceTable>();

  constructor(protected options: BinLogStreamOptions) {
    this.storage = options.storage;
    this.sync_rules = options.storage.sync_rules;
    this.group_id = options.storage.group_id;
    this.slot_name = options.storage.slot_name;
    this.pool = options.pool;

    this.abort_signal = options.abort_signal;
    this.abort_signal.addEventListener(
      'abort',
      () => {
        // TODO close things
      },
      { once: true }
    );
  }

  get connectionTag() {
    // TODO
    return 'default';
  }

  get stopped() {
    return this.abort_signal.aborted;
  }

  async handleRelation(batch: storage.BucketStorageBatch, entity: storage.SourceEntityDescriptor, snapshot: boolean) {
    const result = await this.storage.resolveTable({
      group_id: this.group_id,
      connection_id: this.connection_id,
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

      // TODO: put zero GTID somewhere.
      let gtid: replication_utils.ReplicatedGTID | null = null;
      // Start the snapshot inside a transaction.
      // We use a dedicated connection for this.
      try {
        await this.pool.query('BEGIN');
        try {
          gtid = await replication_utils.readMasterGtid(this.pool);
          await this.snapshotTable(batch, this.pool, result.table);
          await this.pool.query('COMMIT');
        } catch (e) {
          await this.pool.query('ROLLBACK');
          throw e;
        }
      } finally {
        await this.pool.end();
      }
      const [table] = await batch.markSnapshotDone([result.table], gtid.comparable);
      return table;
    }

    return result.table;
  }

  async getQualifiedTableNames(
    batch: storage.BucketStorageBatch,
    db: mysql.Pool,
    tablePattern: sync_rules.TablePattern
  ): Promise<storage.SourceTable[]> {
    if (tablePattern.connectionTag != this.connectionTag) {
      return [];
    }

    let tableRows: any[];
    const prefix = tablePattern.isWildcard ? tablePattern.tablePrefix : undefined;
    if (tablePattern.isWildcard) {
      const result = await db.query<RowDataPacket[]>(
        `SELECT TABLE_NAME
FROM information_schema.tables
WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE ?;
`,
        [tablePattern.schema, tablePattern.tablePattern]
      );
      tableRows = result[0];
    } else {
      const result = await db.query<RowDataPacket[]>(
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

      const rs = await db.query<RowDataPacket[]>(
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

      const cresult = await replication_utils.getReplicationIdentityColumns({
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

  async initSlot(): Promise<InitResult> {
    await replication_utils.checkSourceConfiguration(this.pool);

    const slotName = this.slot_name;

    const status = await this.storage.getStatus();
    if (status.snapshot_done && status.checkpoint_lsn) {
      logger.info(`${slotName} Initial replication already done`);
      // Success
      logger.info(`MySQL not using Slots ${slotName} appears healthy`);
      return { needsInitialSync: false };
    }

    return { needsInitialSync: true };
  }

  /**
   * Start initial replication.
   *
   * If (partial) replication was done before on this slot, this clears the state
   * and starts again from scratch.
   */
  async startInitialReplication() {
    const db = this.pool;

    const slotName = this.slot_name;

    await this.storage.clear();
    const headGTID = await replication_utils.readMasterGtid(db);
    logger.info(`Using GTID:: '${headGTID}'`);
    await db.query('BEGIN');
    try {
      logger.info(`${slotName} Starting initial replication`);
      await this.initialReplication(db, headGTID);
      logger.info(`${slotName} Initial replication done`);
      await db.query('COMMIT');
    } catch (e) {
      await db.query('ROLLBACK');
      throw e;
    }
  }

  async initialReplication(db: mysql.Pool, gtid: replication_utils.ReplicatedGTID) {
    // TODO fix database
    const sourceTables = this.sync_rules
      .getSourceTables()
      .map((table) => new sync_rules.TablePattern('mydatabase', table.tablePattern));
    await this.storage.startBatch({}, async (batch) => {
      for (let tablePattern of sourceTables) {
        const tables = await this.getQualifiedTableNames(batch, db, tablePattern);
        for (let table of tables) {
          await this.snapshotTable(batch, db, table);
          await batch.markSnapshotDone([table], gtid.comparable);

          // await touch();
        }
      }
      await batch.commit(gtid.comparable);
    });
  }

  private transformMysqlToSynRulesRow(row: Record<string, any>): sync_rules.SqliteRow {
    for (let key in row) {
      if (row[key] instanceof Date) {
        row[key] = row[key].toISOString();
      }
    }
    return sync_rules.toSyncRulesRow(row);
  }

  private async snapshotTable(batch: storage.BucketStorageBatch, db: mysql.Pool, table: storage.SourceTable) {
    logger.info(`${this.slot_name} Replicating ${table.qualifiedName}`);
    const results = await db.query<mysql.RowDataPacket[]>(`SELECT * FROM ${table.schema}.${table.table}`);
    for (let record of results[0]) {
      await batch.save({
        tag: 'insert',
        sourceTable: table,
        before: undefined,
        after: this.transformMysqlToSynRulesRow(record)
      });
    }
    await batch.flush();
  }

  private getTable(relationId: MysqlRelId): storage.SourceTable {
    const resolvedRelationId = getMysqlRelId(relationId);
    const table = this.relation_cache.get(resolvedRelationId);
    if (table == null) {
      // We should always receive a replication message before the relation is used.
      // If we can't find it, it's a bug.
      throw new Error(`Missing relation cache for ${resolvedRelationId}`);
    }
    return table;
  }

  async writeChange(batch: storage.BucketStorageBatch, msg: DatabaseEvent): Promise<storage.FlushedResult | null> {
    if (msg.type == 'insert' || msg.type == 'update' || msg.type == 'delete') {
      const table = this.getTable({ schema: msg.database, name: msg.table });
      if (msg.type == 'insert') {
        // rows_replicated_total.add(1);
        return await batch.save({
          tag: 'insert',
          sourceTable: table,
          before: undefined,
          after: this.transformMysqlToSynRulesRow(msg.data)
        });
      } else if (msg.type == 'update') {
        // rows_replicated_total.add(1);
        return await batch.save({
          tag: 'update',
          sourceTable: table,
          before: msg.old ? this.transformMysqlToSynRulesRow(msg.old) : undefined,
          after: this.transformMysqlToSynRulesRow(msg.data)
        });
      } else if (msg.type == 'delete') {
        // rows_replicated_total.add(1);
        return await batch.save({
          tag: 'delete',
          sourceTable: table,
          before: this.transformMysqlToSynRulesRow(msg.data),
          after: undefined
        });
      }
    }
    return null;
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
    const result = await this.initSlot();
    await this.loadTables();
    if (result.needsInitialSync) {
      await this.startInitialReplication();
    }
  }

  private async loadTables() {
    // TODO fix database
    const sourceTables = this.sync_rules
      .getSourceTables()
      .map((table) => new sync_rules.TablePattern('mydatabase', table.tablePattern));
    await this.storage.startBatch({}, async (batch) => {
      for (let tablePattern of sourceTables) {
        await this.getQualifiedTableNames(batch, this.pool, tablePattern);
      }
    });
  }

  async streamChanges() {
    // Auto-activate as soon as initial replication is done
    await this.storage.autoActivate();

    const { checkpoint_lsn } = await this.storage.getStatus();
    const fromGTID = checkpoint_lsn
      ? replication_utils.ReplicatedGTID.fromSerialized(checkpoint_lsn)
      : await replication_utils.readMasterGtid(this.pool);
    const binLogPositionState = fromGTID.nextPosition!;

    const zongji = new ZongJi({
      host: this.options.connection_config.hostname,
      user: this.options.connection_config.username,
      password: this.options.connection_config.password
    });

    zongji.on('binlog', async (evt: any) => {
      console.log(evt, evt.getEventName());

      // State machine
      switch (evt.getEventName()) {
        case 'unknown':
          // This is probably the GTID event
          break;
        case 'rotate':
          // Update the position
          binLogPositionState.filename = evt.binlogName;
          binLogPositionState.offset = evt.position;
          break;
        case 'writerows':
          break;
        case 'updaterows':
          break;
        case 'deleterows':
          break;
        case 'xid':
          // Need to commit
          break;
      }
      // logger.info(`mysqlbinlogstream: received message: '${evt}'`);
      // let parsedMsg: DatabaseEvent = JSON.parse(evt);
      // logger.info(`mysqlbinlogstream: commit: '${parsedMsg.commit}' xoffset:'${parsedMsg.xoffset}'`);
      // // await this.writeChange(batch, parsedMsg);

      // const gtid = parsedMsg.gtid; //gtidMakeComparable(parsedMsg.gtid);
      // if (parsedMsg.commit) {
      //   // await batch.commit(gtid);
      // }
      // logger.info(`${this.slot_name} replicating op GTID: '${gtid}'`);
      // chunks_replicated_total.add(1);
    });

    zongji.start({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows', 'xid', 'rotate', 'unknown'],
      filename: binLogPositionState.filename,
      position: binLogPositionState.offset
    });

    // Forever young
    await new Promise<void>((r) => {
      this.abort_signal.addEventListener(
        'abort',
        () => {
          zongji.stop();
          r();
        },
        { once: true }
      );
    });

    // // Replication never starts in the middle of a transaction
    // const finished = new Promise<void>((resolve, reject) => {
    //   readStream.on('data', async (chunk: string) => {
    //     await touch();

    //     if (this.abort_signal.aborted) {
    //       logger.info('Aborted, closing stream');
    //       readStream.close();
    //       deleteFifoPipe(fifoPath);
    //       resolve();
    //       return;
    //     }
    //     for (const msg of chunk.split('\n')) {
    //       if (msg.trim()) {
    //         logger.info(`mysqlbinlogstream: received message: '${msg}'`);
    //         let parsedMsg: DatabaseEvent = JSON.parse(msg);
    //         logger.info(`mysqlbinlogstream: commit: '${parsedMsg.commit}' xoffset:'${parsedMsg.xoffset}'`);
    //         await this.writeChange(batch, parsedMsg);
    //         const gtid = gtidMakeComparable(parsedMsg.gtid);
    //         if (parsedMsg.commit) {
    //           await batch.commit(gtid);
    //         }
    //         micro.logger.info(`${this.slot_name} replicating op GTID: '${gtid}'`);
    //         chunks_replicated_total.add(1);
    //       }
    //     }
    //   });
    // });

    // await finished;
    // });
  }
}
