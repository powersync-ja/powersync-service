import { container, logger } from '@powersync/lib-services-framework';
import { Metrics, SourceEntityDescriptor, storage } from '@powersync/service-core';
import { DatabaseInputRow, SqliteRow, SqlSyncRules, TablePattern, toSyncRulesRow } from '@powersync/service-sync-rules';
import * as mongo from 'mongodb';
import { MongoManager } from './MongoManager.js';
import { constructAfterRecord, getMongoLsn, getMongoRelation, mongoLsnToTimestamp } from './MongoRelation.js';

export const ZERO_LSN = '00000000';

export interface WalStreamOptions {
  connections: MongoManager;
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

export class ChangeStream {
  sync_rules: SqlSyncRules;
  group_id: number;

  connection_id = 1;

  private readonly storage: storage.SyncRulesBucketStorage;

  private connections: MongoManager;
  private readonly client: mongo.MongoClient;
  private readonly defaultDb: mongo.Db;

  private abort_signal: AbortSignal;

  private relation_cache = new Map<string | number, storage.SourceTable>();

  constructor(options: WalStreamOptions) {
    this.storage = options.storage;
    this.sync_rules = options.storage.sync_rules;
    this.group_id = options.storage.group_id;
    this.connections = options.connections;
    this.client = this.connections.client;
    this.defaultDb = this.connections.db;

    this.abort_signal = options.abort_signal;
    this.abort_signal.addEventListener(
      'abort',
      () => {
        // TODO: Fast abort?
      },
      { once: true }
    );
  }

  get stopped() {
    return this.abort_signal.aborted;
  }

  async getQualifiedTableNames(
    batch: storage.BucketStorageBatch,
    tablePattern: TablePattern
  ): Promise<storage.SourceTable[]> {
    const schema = tablePattern.schema;
    if (tablePattern.connectionTag != this.connections.connectionTag) {
      return [];
    }

    const prefix = tablePattern.isWildcard ? tablePattern.tablePrefix : undefined;
    if (tablePattern.isWildcard) {
      // TODO: Implement
      throw new Error('not supported yet');
    }
    let result: storage.SourceTable[] = [];

    const name = tablePattern.name;

    const table = await this.handleRelation(
      batch,
      {
        name,
        schema,
        objectId: name,
        replicationColumns: [{ name: '_id' }]
      } as SourceEntityDescriptor,
      false
    );

    result.push(table);
    return result;
  }

  async initSlot(): Promise<InitResult> {
    const status = await this.storage.getStatus();
    if (status.snapshot_done && status.checkpoint_lsn) {
      logger.info(`Initial replication already done`);
      return { needsInitialSync: false };
    }

    return { needsInitialSync: true };
  }

  async estimatedCount(table: storage.SourceTable): Promise<string> {
    const db = this.client.db(table.schema);
    const count = db.collection(table.table).estimatedDocumentCount();
    return `~${count}`;
  }

  /**
   * Start initial replication.
   *
   * If (partial) replication was done before on this slot, this clears the state
   * and starts again from scratch.
   */
  async startInitialReplication() {
    await this.storage.clear();
    await this.initialReplication();
  }

  async initialReplication() {
    const sourceTables = this.sync_rules.getSourceTables();
    await this.storage.startBatch({ zeroLSN: ZERO_LSN }, async (batch) => {
      for (let tablePattern of sourceTables) {
        const tables = await this.getQualifiedTableNames(batch, tablePattern);
        for (let table of tables) {
          await this.snapshotTable(batch, table);
          await batch.markSnapshotDone([table], ZERO_LSN);

          await touch();
        }
      }
      await batch.commit(ZERO_LSN);
    });
  }

  static *getQueryData(results: Iterable<DatabaseInputRow>): Generator<SqliteRow> {
    for (let row of results) {
      yield toSyncRulesRow(row);
    }
  }

  private async snapshotTable(batch: storage.BucketStorageBatch, table: storage.SourceTable) {
    logger.info(`Replicating ${table.qualifiedName}`);
    const estimatedCount = await this.estimatedCount(table);
    let at = 0;

    const db = this.client.db(table.schema);
    const collection = db.collection(table.table);
    const query = collection.find({}, {});

    const cursor = query.stream();

    for await (let document of cursor) {
      if (this.abort_signal.aborted) {
        throw new Error(`Aborted initial replication`);
      }

      const record = constructAfterRecord(document);

      // This auto-flushes when the batch reaches its size limit
      await batch.save({ tag: 'insert', sourceTable: table, before: undefined, after: record });

      at += 1;
      Metrics.getInstance().rows_replicated_total.add(1);

      await touch();
    }

    await batch.flush();
  }

  async handleRelation(batch: storage.BucketStorageBatch, descriptor: SourceEntityDescriptor, snapshot: boolean) {
    if (!descriptor.objectId && typeof descriptor.objectId != 'string') {
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
      // TODO: Transaction / consistency
      await this.snapshotTable(batch, result.table);
      const [table] = await batch.markSnapshotDone([result.table], lsn);
      return table;
    }

    return result.table;
  }

  async writeChange(
    batch: storage.BucketStorageBatch,
    table: storage.SourceTable,
    change: mongo.ChangeStreamDocument
  ): Promise<storage.FlushedResult | null> {
    if (!table.syncAny) {
      logger.debug(`Collection ${table.qualifiedName} not used in sync rules - skipping`);
      return null;
    }

    Metrics.getInstance().rows_replicated_total.add(1);
    if (change.operationType == 'insert') {
      const baseRecord = constructAfterRecord(change.fullDocument);
      return await batch.save({ tag: 'insert', sourceTable: table, before: undefined, after: baseRecord });
    } else if (change.operationType == 'update') {
      const before = undefined; // TODO: handle changing _id?
      const after = constructAfterRecord(change.fullDocument!);
      return await batch.save({ tag: 'update', sourceTable: table, before: before, after: after });
    } else if (change.operationType == 'delete') {
      const key = constructAfterRecord(change.documentKey);
      console.log('delete', key);
      return await batch.save({ tag: 'delete', sourceTable: table, before: key });
    } else {
      throw new Error(`Unsupported operation: ${change.operationType}`);
    }
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
    if (result.needsInitialSync) {
      await this.startInitialReplication();
    }
  }

  async streamChanges() {
    // Auto-activate as soon as initial replication is done
    await this.storage.autoActivate();

    await this.storage.startBatch({ zeroLSN: ZERO_LSN }, async (batch) => {
      // TODO: Resume replication
      const lastLsn = batch.lastCheckpointLsn;
      const startAfter = mongoLsnToTimestamp(lastLsn) ?? undefined;
      logger.info(`Resume streaming at ${startAfter?.inspect()} / ${lastLsn}`);

      // TODO: Use changeStreamSplitLargeEvent

      const stream = this.client.watch(undefined, {
        startAtOperationTime: startAfter,
        showExpandedEvents: true,
        useBigInt64: true,
        fullDocument: 'updateLookup' // FIXME: figure this one out
      });
      this.abort_signal.addEventListener('abort', () => {
        stream.close();
      });

      let lastEventTimestamp: mongo.Timestamp | null = null;

      for await (const changeDocument of stream) {
        await touch();

        if (this.abort_signal.aborted) {
          break;
        }

        if (startAfter != null && changeDocument.clusterTime?.lte(startAfter)) {
          continue;
        }

        if (
          lastEventTimestamp != null &&
          changeDocument.clusterTime != null &&
          changeDocument.clusterTime.neq(lastEventTimestamp)
        ) {
          const lsn = getMongoLsn(lastEventTimestamp);
          await batch.commit(lsn);
          lastEventTimestamp = null;
        }

        if ((changeDocument as any).ns?.db != this.defaultDb.databaseName) {
          // HACK: Ignore events to other databases, but only _after_
          // the above commit.

          // TODO: filter out storage db on in the pipeline
          // TODO: support non-default dbs
          continue;
        }
        console.log('event', changeDocument);

        if (
          changeDocument.operationType == 'insert' ||
          changeDocument.operationType == 'update' ||
          changeDocument.operationType == 'delete'
        ) {
          const rel = getMongoRelation(changeDocument.ns);
          const table = await this.handleRelation(batch, rel, true);
          if (table.syncAny) {
            await this.writeChange(batch, table, changeDocument);
            if (changeDocument.clusterTime) {
              lastEventTimestamp = changeDocument.clusterTime;
            }
          }
        }
      }
    });
  }
}

async function touch() {
  // FIXME: The hosted Kubernetes probe does not actually check the timestamp on this.
  // FIXME: We need a timeout of around 5+ minutes in Kubernetes if we do start checking the timestamp,
  // or reduce PING_INTERVAL here.
  return container.probes.touch();
}
