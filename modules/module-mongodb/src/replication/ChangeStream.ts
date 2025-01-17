import { mongo } from '@powersync/lib-service-mongodb';
import {
  container,
  ErrorCode,
  logger,
  ReplicationAbortedError,
  ReplicationAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import { Metrics, SaveOperationTag, SourceEntityDescriptor, SourceTable, storage } from '@powersync/service-core';
import { DatabaseInputRow, SqliteRow, SqlSyncRules, TablePattern } from '@powersync/service-sync-rules';
import { PostImagesOption } from '../types/types.js';
import { escapeRegExp } from '../utils.js';
import { MongoManager } from './MongoManager.js';
import {
  constructAfterRecord,
  createCheckpoint,
  getMongoLsn,
  getMongoRelation,
  mongoLsnToTimestamp
} from './MongoRelation.js';
import { CHECKPOINTS_COLLECTION } from './replication-utils.js';

export const ZERO_LSN = '0000000000000000';

export interface ChangeStreamOptions {
  connections: MongoManager;
  storage: storage.SyncRulesBucketStorage;
  abort_signal: AbortSignal;
}

interface InitResult {
  needsInitialSync: boolean;
}

/**
 * Thrown when the change stream is not valid anymore, and replication
 * must be restarted.
 *
 * Possible reasons:
 *  * Some change stream documents do not have postImages.
 *  * startAfter/resumeToken is not valid anymore.
 */
export class ChangeStreamInvalidatedError extends Error {
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

  private get usePostImages() {
    return this.connections.options.postImages != PostImagesOption.OFF;
  }

  private get configurePostImages() {
    return this.connections.options.postImages == PostImagesOption.AUTO_CONFIGURE;
  }

  /**
   * This resolves a pattern, persists the related metadata, and returns
   * the resulting SourceTables.
   *
   * This implicitly checks the collection postImage configuration.
   */
  async resolveQualifiedTableNames(
    batch: storage.BucketStorageBatch,
    tablePattern: TablePattern
  ): Promise<storage.SourceTable[]> {
    const schema = tablePattern.schema;
    if (tablePattern.connectionTag != this.connections.connectionTag) {
      return [];
    }

    let nameFilter: RegExp | string;
    if (tablePattern.isWildcard) {
      nameFilter = new RegExp('^' + escapeRegExp(tablePattern.tablePrefix));
    } else {
      nameFilter = tablePattern.name;
    }
    let result: storage.SourceTable[] = [];

    // Check if the collection exists
    const collections = await this.client
      .db(schema)
      .listCollections(
        {
          name: nameFilter
        },
        { nameOnly: false }
      )
      .toArray();

    if (!tablePattern.isWildcard && collections.length == 0) {
      logger.warn(`Collection ${schema}.${tablePattern.name} not found`);
    }

    for (let collection of collections) {
      const table = await this.handleRelation(
        batch,
        {
          name: collection.name,
          schema,
          objectId: collection.name,
          replicationColumns: [{ name: '_id' }]
        } as SourceEntityDescriptor,
        // This is done as part of the initial setup - snapshot is handled elsewhere
        { snapshot: false, collectionInfo: collection }
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

    // We need to get the snapshot time before taking the initial snapshot.
    const hello = await this.defaultDb.command({ hello: 1 });
    const snapshotTime = hello.lastWrite?.majorityOpTime?.ts as mongo.Timestamp;
    if (hello.msg == 'isdbgrid') {
      throw new ServiceError(
        ErrorCode.PSYNC_S1341,
        'Sharded MongoDB Clusters are not supported yet (including MongoDB Serverless instances).'
      );
    } else if (hello.setName == null) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1342,
        'Standalone MongoDB instances are not supported - use a replicaset.'
      );
    } else if (snapshotTime == null) {
      // Not known where this would happen apart from the above cases
      throw new ReplicationAssertionError('MongoDB lastWrite timestamp not found.');
    }
    // We previously used {snapshot: true} for the snapshot session.
    // While it gives nice consistency guarantees, it fails when the
    // snapshot takes longer than 5 minutes, due to minSnapshotHistoryWindowInSeconds
    // expiring the snapshot.
    const session = await this.client.startSession();
    try {
      await this.storage.startBatch(
        { zeroLSN: ZERO_LSN, defaultSchema: this.defaultDb.databaseName, storeCurrentData: false },
        async (batch) => {
          // Start by resolving all tables.
          // This checks postImage configuration, and that should fail as
          // earlier as possible.
          let allSourceTables: SourceTable[] = [];
          for (let tablePattern of sourceTables) {
            const tables = await this.resolveQualifiedTableNames(batch, tablePattern);
            allSourceTables.push(...tables);
          }

          for (let table of allSourceTables) {
            await this.snapshotTable(batch, table, session);
            await batch.markSnapshotDone([table], ZERO_LSN);

            await touch();
          }

          const lsn = getMongoLsn(snapshotTime);
          logger.info(`Snapshot commit at ${snapshotTime.inspect()} / ${lsn}`);
          await batch.commit(lsn);
        }
      );
    } finally {
      session.endSession();
    }
  }

  private async setupCheckpointsCollection() {
    const collection = await this.getCollectionInfo(this.defaultDb.databaseName, CHECKPOINTS_COLLECTION);
    if (collection == null) {
      await this.defaultDb.createCollection(CHECKPOINTS_COLLECTION, {
        changeStreamPreAndPostImages: { enabled: true }
      });
    } else if (this.usePostImages && collection.options?.changeStreamPreAndPostImages?.enabled != true) {
      // Drop + create requires less permissions than collMod,
      // and we don't care about the data in this collection.
      await this.defaultDb.dropCollection(CHECKPOINTS_COLLECTION);
      await this.defaultDb.createCollection(CHECKPOINTS_COLLECTION, {
        changeStreamPreAndPostImages: { enabled: true }
      });
    }
  }

  private getSourceNamespaceFilters(): { $match: any; multipleDatabases: boolean } {
    const sourceTables = this.sync_rules.getSourceTables();

    let $inFilters: any[] = [{ db: this.defaultDb.databaseName, coll: CHECKPOINTS_COLLECTION }];
    let $refilters: any[] = [];
    let multipleDatabases = false;
    for (let tablePattern of sourceTables) {
      if (tablePattern.connectionTag != this.connections.connectionTag) {
        continue;
      }

      if (tablePattern.schema != this.defaultDb.databaseName) {
        multipleDatabases = true;
      }

      if (tablePattern.isWildcard) {
        $refilters.push({
          'ns.db': tablePattern.schema,
          'ns.coll': new RegExp('^' + escapeRegExp(tablePattern.tablePrefix))
        });
      } else {
        $inFilters.push({
          db: tablePattern.schema,
          coll: tablePattern.name
        });
      }
    }
    if ($refilters.length > 0) {
      return { $match: { $or: [{ ns: { $in: $inFilters } }, ...$refilters] }, multipleDatabases };
    }
    return { $match: { ns: { $in: $inFilters } }, multipleDatabases };
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
    const query = collection.find({}, { session, readConcern: { level: 'majority' } });

    const cursor = query.stream();

    for await (let document of cursor) {
      if (this.abort_signal.aborted) {
        throw new ReplicationAbortedError(`Aborted initial replication`);
      }

      at += 1;

      const record = constructAfterRecord(document);

      // This auto-flushes when the batch reaches its size limit
      await batch.save({
        tag: SaveOperationTag.INSERT,
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
    logger.info(`Replicated ${at} documents for ${table.qualifiedName}`);
  }

  private async getRelation(
    batch: storage.BucketStorageBatch,
    descriptor: SourceEntityDescriptor
  ): Promise<SourceTable> {
    const existing = this.relation_cache.get(descriptor.objectId);
    if (existing != null) {
      return existing;
    }

    // Note: collection may have been dropped at this point, so we handle
    // missing values.
    const collection = await this.getCollectionInfo(descriptor.schema, descriptor.name);

    return this.handleRelation(batch, descriptor, { snapshot: false, collectionInfo: collection });
  }

  private async getCollectionInfo(db: string, name: string): Promise<mongo.CollectionInfo | undefined> {
    const collection = (
      await this.client
        .db(db)
        .listCollections(
          {
            name: name
          },
          { nameOnly: false }
        )
        .toArray()
    )[0];
    return collection;
  }

  private async checkPostImages(db: string, collectionInfo: mongo.CollectionInfo) {
    if (!this.usePostImages) {
      // Nothing to check
      return;
    }

    const enabled = collectionInfo.options?.changeStreamPreAndPostImages?.enabled == true;

    if (!enabled && this.configurePostImages) {
      await this.client.db(db).command({
        collMod: collectionInfo.name,
        changeStreamPreAndPostImages: { enabled: true }
      });
      logger.info(`Enabled postImages on ${db}.${collectionInfo.name}`);
    } else if (!enabled) {
      throw new ServiceError(ErrorCode.PSYNC_S1343, `postImages not enabled on ${db}.${collectionInfo.name}`);
    }
  }

  async handleRelation(
    batch: storage.BucketStorageBatch,
    descriptor: SourceEntityDescriptor,
    options: { snapshot: boolean; collectionInfo: mongo.CollectionInfo | undefined }
  ) {
    if (options.collectionInfo != null) {
      await this.checkPostImages(descriptor.schema, options.collectionInfo);
    } else {
      // If collectionInfo is null, the collection may have been dropped.
      // Ignore the postImages check in this case.
    }

    const snapshot = options.snapshot;
    if (!descriptor.objectId && typeof descriptor.objectId != 'string') {
      throw new ReplicationAssertionError('MongoDB replication - objectId expected');
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

      await this.snapshotTable(batch, result.table);
      const no_checkpoint_before_lsn = await createCheckpoint(this.client, this.defaultDb);

      const [table] = await batch.markSnapshotDone([result.table], no_checkpoint_before_lsn);
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
        tag: SaveOperationTag.INSERT,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: baseRecord,
        afterReplicaId: change.documentKey._id
      });
    } else if (change.operationType == 'update' || change.operationType == 'replace') {
      if (change.fullDocument == null) {
        // Treat as delete
        return await batch.save({
          tag: SaveOperationTag.DELETE,
          sourceTable: table,
          before: undefined,
          beforeReplicaId: change.documentKey._id
        });
      }
      const after = constructAfterRecord(change.fullDocument!);
      return await batch.save({
        tag: SaveOperationTag.UPDATE,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: after,
        afterReplicaId: change.documentKey._id
      });
    } else if (change.operationType == 'delete') {
      return await batch.save({
        tag: SaveOperationTag.DELETE,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: change.documentKey._id
      });
    } else {
      throw new ReplicationAssertionError(`Unsupported operation: ${change.operationType}`);
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
    await this.setupCheckpointsCollection();
    if (result.needsInitialSync) {
      await this.startInitialReplication();
    }
  }

  async streamChanges() {
    try {
      await this.streamChangesInternal();
    } catch (e) {
      if (
        e instanceof mongo.MongoServerError &&
        e.codeName == 'NoMatchingDocument' &&
        e.errmsg?.includes('post-image was not found')
      ) {
        throw new ChangeStreamInvalidatedError(e.errmsg);
      }
      throw e;
    }
  }

  async streamChangesInternal() {
    // Auto-activate as soon as initial replication is done
    await this.storage.autoActivate();

    await this.storage.startBatch(
      { zeroLSN: ZERO_LSN, defaultSchema: this.defaultDb.databaseName, storeCurrentData: false },
      async (batch) => {
        const lastLsn = batch.lastCheckpointLsn;
        const startAfter = mongoLsnToTimestamp(lastLsn) ?? undefined;
        logger.info(`Resume streaming at ${startAfter?.inspect()} / ${lastLsn}`);

        const filters = this.getSourceNamespaceFilters();

        const pipeline: mongo.Document[] = [
          {
            $match: filters.$match
          },
          { $changeStreamSplitLargeEvent: {} }
        ];

        let fullDocument: 'required' | 'updateLookup';

        if (this.usePostImages) {
          // 'read_only' or 'auto_configure'
          // Configuration happens during snapshot, or when we see new
          // collections.
          fullDocument = 'required';
        } else {
          fullDocument = 'updateLookup';
        }

        const streamOptions: mongo.ChangeStreamOptions = {
          startAtOperationTime: startAfter,
          showExpandedEvents: true,
          useBigInt64: true,
          maxAwaitTimeMS: 200,
          fullDocument: fullDocument
        };
        let stream: mongo.ChangeStream<mongo.Document>;
        if (filters.multipleDatabases) {
          // Requires readAnyDatabase@admin on Atlas
          stream = this.client.watch(pipeline, streamOptions);
        } else {
          // Same general result, but requires less permissions than the above
          stream = this.defaultDb.watch(pipeline, streamOptions);
        }

        if (this.abort_signal.aborted) {
          stream.close();
          return;
        }

        this.abort_signal.addEventListener('abort', () => {
          stream.close();
        });

        // Always start with a checkpoint.
        // This helps us to clear erorrs when restarting, even if there is
        // no data to replicate.
        let waitForCheckpointLsn: string | null = await createCheckpoint(this.client, this.defaultDb);

        let splitDocument: mongo.ChangeStreamDocument | null = null;

        while (true) {
          if (this.abort_signal.aborted) {
            break;
          }

          const originalChangeDocument = await stream.tryNext();

          if (originalChangeDocument == null || this.abort_signal.aborted) {
            continue;
          }
          await touch();

          if (startAfter != null && originalChangeDocument.clusterTime?.lte(startAfter)) {
            continue;
          }

          let changeDocument = originalChangeDocument;
          if (originalChangeDocument?.splitEvent != null) {
            // Handle split events from $changeStreamSplitLargeEvent.
            // This is only relevant for very large update operations.
            const splitEvent = originalChangeDocument?.splitEvent;

            if (splitDocument == null) {
              splitDocument = originalChangeDocument;
            } else {
              splitDocument = Object.assign(splitDocument, originalChangeDocument);
            }

            if (splitEvent.fragment == splitEvent.of) {
              // Got all fragments
              changeDocument = splitDocument;
              splitDocument = null;
            } else {
              // Wait for more fragments
              continue;
            }
          } else if (splitDocument != null) {
            // We were waiting for fragments, but got a different event
            throw new ReplicationAssertionError(`Incomplete splitEvent: ${JSON.stringify(splitDocument.splitEvent)}`);
          }

          // console.log('event', changeDocument);

          if (
            (changeDocument.operationType == 'insert' ||
              changeDocument.operationType == 'update' ||
              changeDocument.operationType == 'replace') &&
            changeDocument.ns.coll == CHECKPOINTS_COLLECTION
          ) {
            const lsn = getMongoLsn(changeDocument.clusterTime!);
            if (waitForCheckpointLsn != null && lsn >= waitForCheckpointLsn) {
              waitForCheckpointLsn = null;
            }
            await batch.commit(lsn);
          } else if (
            changeDocument.operationType == 'insert' ||
            changeDocument.operationType == 'update' ||
            changeDocument.operationType == 'replace' ||
            changeDocument.operationType == 'delete'
          ) {
            if (waitForCheckpointLsn == null) {
              waitForCheckpointLsn = await createCheckpoint(this.client, this.defaultDb);
            }
            const rel = getMongoRelation(changeDocument.ns);
            const table = await this.getRelation(batch, rel);
            if (table.syncAny) {
              await this.writeChange(batch, table, changeDocument);
            }
          } else if (changeDocument.operationType == 'drop') {
            const rel = getMongoRelation(changeDocument.ns);
            const table = await this.getRelation(batch, rel);
            if (table.syncAny) {
              await batch.drop([table]);
              this.relation_cache.delete(table.objectId);
            }
          } else if (changeDocument.operationType == 'rename') {
            const relFrom = getMongoRelation(changeDocument.ns);
            const relTo = getMongoRelation(changeDocument.to);
            const tableFrom = await this.getRelation(batch, relFrom);
            if (tableFrom.syncAny) {
              await batch.drop([tableFrom]);
              this.relation_cache.delete(tableFrom.objectId);
            }
            // Here we do need to snapshot the new table
            const collection = await this.getCollectionInfo(relTo.schema, relTo.name);
            await this.handleRelation(batch, relTo, { snapshot: true, collectionInfo: collection });
          }
        }
      }
    );
  }
}

async function touch() {
  // FIXME: The hosted Kubernetes probe does not actually check the timestamp on this.
  // FIXME: We need a timeout of around 5+ minutes in Kubernetes if we do start checking the timestamp,
  // or reduce PING_INTERVAL here.
  return container.probes.touch();
}
