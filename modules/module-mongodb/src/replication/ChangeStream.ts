import { container, logger } from '@powersync/lib-services-framework';
import { Metrics, SourceEntityDescriptor, storage } from '@powersync/service-core';
import { DatabaseInputRow, SqliteRow, SqlSyncRules, TablePattern, toSyncRulesRow } from '@powersync/service-sync-rules';
import * as mongo from 'mongodb';
import { MongoManager } from './MongoManager.js';
import { constructAfterRecord, getMongoLsn, getMongoRelation, mongoLsnToTimestamp } from './MongoRelation.js';

export const ZERO_LSN = '0000000000000000';

export interface ChangeStreamOptions {
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

  constructor(options: ChangeStreamOptions) {
    this.storage = options.storage;
    this.group_id = options.storage.group_id;
    this.connections = options.connections;
    this.client = this.connections.client;
    this.defaultDb = this.connections.db;
    this.sync_rules = options.storage.getParsedSyncRules({
      defaultSchema: this.defaultDb.databaseName
    });

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

    if (tablePattern.isWildcard) {
      // TODO: Implement
      throw new Error('Wildcard collections not supported yet');
    }
    let result: storage.SourceTable[] = [];

    const name = tablePattern.name;

    // Check if the collection exists
    const collections = await this.client
      .db(schema)
      .listCollections(
        {
          name: name
        },
        { nameOnly: true }
      )
      .toArray();

    if (collections[0]?.name == name) {
      const table = await this.handleRelation(
        batch,
        {
          name,
          schema,
          objectId: name,
          replicationColumns: [{ name: '_id' }]
        } as SourceEntityDescriptor,
        // This is done as part of the initial setup - snapshot is handled elsewhere
        { snapshot: false }
      );

      result.push(table);
    }

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
    await this.client.connect();

    const ping = await this.defaultDb.command({ ping: 1 });
    const startTime = ping.$clusterTime.clusterTime as mongo.Timestamp;
    const session = await this.client.startSession({
      snapshot: true
    });
    try {
      await this.storage.startBatch(
        { zeroLSN: ZERO_LSN, defaultSchema: this.defaultDb.databaseName },
        async (batch) => {
          for (let tablePattern of sourceTables) {
            const tables = await this.getQualifiedTableNames(batch, tablePattern);
            for (let table of tables) {
              await this.snapshotTable(batch, table, session);
              await batch.markSnapshotDone([table], ZERO_LSN);

              await touch();
            }
          }

          const snapshotTime = session.clusterTime?.clusterTime ?? startTime;

          if (snapshotTime != null) {
            const lsn = getMongoLsn(snapshotTime);
            logger.info(`Snapshot commit at ${snapshotTime.inspect()} / ${lsn}`);
            // keepalive() does an auto-commit if there is data
            await batch.keepalive(lsn);
          } else {
            logger.info(`No snapshot clusterTime (no snapshot data?) - skipping commit.`);
          }
        }
      );
    } finally {
      session.endSession();
    }
  }

  private getSourceNamespaceFilters() {
    const sourceTables = this.sync_rules.getSourceTables();

    let filters: any[] = [{ db: this.defaultDb.databaseName, coll: '_powersync_checkpoints' }];
    for (let tablePattern of sourceTables) {
      if (tablePattern.connectionTag != this.connections.connectionTag) {
        continue;
      }

      if (tablePattern.isWildcard) {
        // TODO: Implement
        throw new Error('wildcard collections not supported yet');
      }

      filters.push({
        db: tablePattern.schema,
        coll: tablePattern.name
      });
    }
    return { $in: filters };
  }

  static *getQueryData(results: Iterable<DatabaseInputRow>): Generator<SqliteRow> {
    for (let row of results) {
      yield constructAfterRecord(row);
    }
  }

  private async snapshotTable(
    batch: storage.BucketStorageBatch,
    table: storage.SourceTable,
    session?: mongo.ClientSession
  ) {
    logger.info(`Replicating ${table.qualifiedName}`);
    const estimatedCount = await this.estimatedCount(table);
    let at = 0;

    const db = this.client.db(table.schema);
    const collection = db.collection(table.table);
    const query = collection.find({}, { session });

    const cursor = query.stream();

    for await (let document of cursor) {
      if (this.abort_signal.aborted) {
        throw new Error(`Aborted initial replication`);
      }

      const record = constructAfterRecord(document);

      // This auto-flushes when the batch reaches its size limit
      await batch.save({
        tag: 'insert',
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: record,
        afterReplicaId: document._id
      });

      at += 1;
      Metrics.getInstance().rows_replicated_total.add(1);

      await touch();
    }

    await batch.flush();
  }

  async handleRelation(
    batch: storage.BucketStorageBatch,
    descriptor: SourceEntityDescriptor,
    options: { snapshot: boolean }
  ) {
    const snapshot = options.snapshot;
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
      return await batch.save({
        tag: 'insert',
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: baseRecord,
        afterReplicaId: change.documentKey._id
      });
    } else if (change.operationType == 'update') {
      const after = constructAfterRecord(change.fullDocument ?? {});
      return await batch.save({
        tag: 'update',
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: after,
        afterReplicaId: change.documentKey._id
      });
    } else if (change.operationType == 'delete') {
      return await batch.save({
        tag: 'delete',
        sourceTable: table,
        before: undefined,
        beforeReplicaId: change.documentKey._id
      });
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

    await this.storage.startBatch({ zeroLSN: ZERO_LSN, defaultSchema: this.defaultDb.databaseName }, async (batch) => {
      const lastLsn = batch.lastCheckpointLsn;
      const startAfter = mongoLsnToTimestamp(lastLsn) ?? undefined;
      logger.info(`Resume streaming at ${startAfter?.inspect()} / ${lastLsn}`);

      // TODO: Use changeStreamSplitLargeEvent

      const pipeline: mongo.Document[] = [
        {
          $match: {
            ns: this.getSourceNamespaceFilters()
          }
        }
      ];

      const stream = this.client.watch(pipeline, {
        startAtOperationTime: startAfter,
        showExpandedEvents: true,
        useBigInt64: true,
        maxAwaitTimeMS: 200,
        fullDocument: 'updateLookup' // FIXME: figure this one out
      });
      this.abort_signal.addEventListener('abort', () => {
        stream.close();
      });

      let lastEventTimestamp: mongo.Timestamp | null = null;

      while (true) {
        const changeDocument = await stream.tryNext();
        if (changeDocument == null) {
          // We don't get events for transaction commit.
          // So if no events are available in the stream within maxAwaitTimeMS,
          // we assume the transaction is complete.
          // This is not foolproof - we may end up with a commit in the middle
          // of a transaction.
          if (lastEventTimestamp != null) {
            const lsn = getMongoLsn(lastEventTimestamp);
            await batch.commit(lsn);
            lastEventTimestamp = null;
          }

          continue;
        }
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

        // console.log('event', changeDocument);

        if (changeDocument.operationType == 'insert' && changeDocument.ns.coll == '_powersync_checkpoints') {
          const lsn = getMongoLsn(changeDocument.clusterTime!);
          await batch.keepalive(lsn);
        } else if (
          changeDocument.operationType == 'insert' ||
          changeDocument.operationType == 'update' ||
          changeDocument.operationType == 'delete'
        ) {
          const rel = getMongoRelation(changeDocument.ns);
          // Don't snapshot tables here - we get all the data as insert events
          const table = await this.handleRelation(batch, rel, { snapshot: false });
          if (table.syncAny) {
            await this.writeChange(batch, table, changeDocument);
            if (changeDocument.clusterTime) {
              lastEventTimestamp = changeDocument.clusterTime;
            }
          }
        } else if (changeDocument.operationType == 'drop') {
          const rel = getMongoRelation(changeDocument.ns);
          const table = await this.handleRelation(batch, rel, { snapshot: false });
          if (table.syncAny) {
            await batch.drop([table]);
          }
        } else if (changeDocument.operationType == 'rename') {
          const relFrom = getMongoRelation(changeDocument.ns);
          const relTo = getMongoRelation(changeDocument.to);
          const tableFrom = await this.handleRelation(batch, relFrom, { snapshot: false });
          if (tableFrom.syncAny) {
            await batch.drop([tableFrom]);
          }
          // Here we do need to snapshot the new table
          await this.handleRelation(batch, relTo, { snapshot: true });
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
