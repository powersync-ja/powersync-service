import { isMongoNetworkTimeoutError, isMongoServerError, mongo } from '@powersync/lib-service-mongodb';
import {
  container,
  DatabaseConnectionError,
  ErrorCode,
  logger,
  ReplicationAbortedError,
  ReplicationAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import {
  MetricsEngine,
  RelationCache,
  SaveOperationTag,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import { DatabaseInputRow, SqliteRow, SqlSyncRules, TablePattern } from '@powersync/service-sync-rules';
import { ReplicationMetric } from '@powersync/service-types';
import { MongoLSN } from '../common/MongoLSN.js';
import { PostImagesOption } from '../types/types.js';
import { escapeRegExp } from '../utils.js';
import { MongoManager } from './MongoManager.js';
import {
  constructAfterRecord,
  createCheckpoint,
  getCacheIdentifier,
  getMongoRelation,
  STANDALONE_CHECKPOINT_ID
} from './MongoRelation.js';
import { ChunkedSnapshotQuery } from './MongoSnapshotQuery.js';
import { CHECKPOINTS_COLLECTION } from './replication-utils.js';

export interface ChangeStreamOptions {
  connections: MongoManager;
  storage: storage.SyncRulesBucketStorage;
  metrics: MetricsEngine;
  abort_signal: AbortSignal;
  /**
   * Override maxAwaitTimeMS for testing.
   *
   * In most cases, the default of 10_000 is fine. However, for MongoDB 6.0, this can cause a delay
   * in closing the stream. To cover that case, reduce the timeout for tests.
   */
  maxAwaitTimeMS?: number;
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
export class ChangeStreamInvalidatedError extends DatabaseConnectionError {
  constructor(message: string, cause: any) {
    super(ErrorCode.PSYNC_S1344, message, cause);
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
  private readonly metrics: MetricsEngine;

  private readonly maxAwaitTimeMS: number;

  private abort_signal: AbortSignal;

  private relationCache = new RelationCache(getCacheIdentifier);

  private checkpointStreamId = new mongo.ObjectId();

  constructor(options: ChangeStreamOptions) {
    this.storage = options.storage;
    this.metrics = options.metrics;
    this.group_id = options.storage.group_id;
    this.connections = options.connections;
    this.maxAwaitTimeMS = options.maxAwaitTimeMS ?? 10_000;
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

  private get logPrefix() {
    return `[powersync_${this.group_id}]`;
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
      logger.warn(`${this.logPrefix} Collection ${schema}.${tablePattern.name} not found`);
    }

    for (let collection of collections) {
      const table = await this.handleRelation(
        batch,
        getMongoRelation({ db: schema, coll: collection.name }),
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
      logger.info(`${this.logPrefix} Initial replication already done`);
      return { needsInitialSync: false };
    }

    return { needsInitialSync: true };
  }

  async estimatedCount(table: storage.SourceTable): Promise<string> {
    const count = await this.estimatedCountNumber(table);
    return `~${count}`;
  }

  async estimatedCountNumber(table: storage.SourceTable): Promise<number> {
    const db = this.client.db(table.schema);
    return await db.collection(table.table).estimatedDocumentCount();
  }
  /**
   * Start initial replication.
   */
  async startInitialReplication() {
    await this.initialReplication();
  }

  private async getSnapshotLsn(): Promise<string> {
    const hello = await this.defaultDb.command({ hello: 1 });
    // Basic sanity check
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
    }

    // Open a change stream just to get a resume token for later use.
    // We could use clusterTime from the hello command, but that won't tell us if the
    // snapshot isn't valid anymore.
    const { stream } = this.openChangeStream({ lsn: null, maxAwaitTimeMs: 0 });
    try {
      await stream.hasNext();
      const resumeToken = stream.resumeToken;
      const { comparable: lsn } = MongoLSN.fromResumeToken(resumeToken);
      return lsn;
    } catch (e) {
      throw mapChangeStreamError(e);
    } finally {
      await stream.close();
    }
  }

  private async validateSnapshotLsn(lsn: string) {
    const { stream } = this.openChangeStream({ lsn: lsn, maxAwaitTimeMs: 0 });
    try {
      await stream.hasNext();
    } catch (e) {
      // Note: A timeout here is not handled as a ChangeStreamInvalidatedError, even though
      // we possibly cannot recover from it.
      throw mapChangeStreamError(e);
    } finally {
      await stream.close();
    }
  }

  async initialReplication() {
    const sourceTables = this.sync_rules.getSourceTables();
    await this.client.connect();

    await this.storage.startBatch(
      {
        zeroLSN: MongoLSN.ZERO.comparable,
        defaultSchema: this.defaultDb.databaseName,
        storeCurrentData: false,
        skipExistingRows: true
      },
      async (batch) => {
        let lsn = batch.lastCheckpointLsn;
        if (lsn == null) {
          // First replication attempt - get a snapshot and store the timestamp
          lsn = await this.getSnapshotLsn();
          await batch.setSnapshotLsn(lsn);
          logger.info(`${this.logPrefix} Marking snapshot at ${lsn}`);
        } else {
          logger.info(`${this.logPrefix} Resuming snapshot at ${lsn}`);
          // Check that the snapshot is still valid.
          await this.validateSnapshotLsn(lsn);
        }

        // Start by resolving all tables.
        // This checks postImage configuration, and that should fail as
        // early as possible.
        let allSourceTables: SourceTable[] = [];
        for (let tablePattern of sourceTables) {
          const tables = await this.resolveQualifiedTableNames(batch, tablePattern);
          allSourceTables.push(...tables);
        }

        let tablesWithStatus: SourceTable[] = [];
        for (let table of allSourceTables) {
          if (table.snapshotComplete) {
            logger.info(`${this.logPrefix} Skipping ${table.qualifiedName} - snapshot already done`);
            continue;
          }
          let count = await this.estimatedCountNumber(table);
          const updated = await batch.updateTableProgress(table, {
            totalEstimatedCount: count
          });
          tablesWithStatus.push(updated);
          this.relationCache.update(updated);
          logger.info(
            `${this.logPrefix} To replicate: ${table.qualifiedName}: ${updated.snapshotStatus?.replicatedCount}/~${updated.snapshotStatus?.totalEstimatedCount}`
          );
        }

        for (let table of tablesWithStatus) {
          await this.snapshotTable(batch, table);
          await batch.markSnapshotDone([table], MongoLSN.ZERO.comparable);

          await touch();
        }

        logger.info(`${this.logPrefix} Snapshot commit at ${lsn}`);
        await batch.commit(lsn);
      }
    );
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
    } else {
      // Clear the collection on startup, to keep it clean
      // We never query this collection directly, and don't want to keep the data around.
      // We only use this to get data into the oplog/changestream.
      await this.defaultDb.collection(CHECKPOINTS_COLLECTION).deleteMany({});
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

  private async snapshotTable(batch: storage.BucketStorageBatch, table: storage.SourceTable) {
    const totalEstimatedCount = await this.estimatedCountNumber(table);
    let at = table.snapshotStatus?.replicatedCount ?? 0;
    const db = this.client.db(table.schema);
    const collection = db.collection(table.table);
    await using query = new ChunkedSnapshotQuery({ collection, key: table.snapshotStatus?.lastKey });
    if (query.lastKey != null) {
      logger.info(
        `${this.logPrefix} Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()} - resuming at _id > ${query.lastKey}`
      );
    } else {
      logger.info(`${this.logPrefix} Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()}`);
    }

    let lastBatch = performance.now();
    let nextChunkPromise = query.nextChunk();
    while (true) {
      const { docs: docBatch, lastKey } = await nextChunkPromise;
      if (docBatch.length == 0) {
        break;
      }
      // Pre-fetch next batch, so that we can read and write concurrently
      nextChunkPromise = query.nextChunk();
      for (let document of docBatch) {
        if (this.abort_signal.aborted) {
          throw new ReplicationAbortedError(`Aborted initial replication`);
        }

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
      }

      // Important: flush before marking progress
      await batch.flush();
      at += docBatch.length;
      this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(docBatch.length);

      table = await batch.updateTableProgress(table, {
        lastKey,
        replicatedCount: at,
        totalEstimatedCount: totalEstimatedCount
      });
      this.relationCache.update(table);

      const duration = performance.now() - lastBatch;
      lastBatch = performance.now();
      logger.info(
        `${this.logPrefix} Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()} in ${duration.toFixed(0)}ms`
      );
      await touch();
    }
    // In case the loop was interrupted, make sure we await the last promise.
    await nextChunkPromise;

    await batch.flush();
    logger.info(`${this.logPrefix} Replicated ${at} documents for ${table.qualifiedName}`);
  }

  private async getRelation(
    batch: storage.BucketStorageBatch,
    descriptor: SourceEntityDescriptor,
    options: { snapshot: boolean }
  ): Promise<SourceTable> {
    const existing = this.relationCache.get(descriptor);
    if (existing != null) {
      return existing;
    }

    // Note: collection may have been dropped at this point, so we handle
    // missing values.
    const collection = await this.getCollectionInfo(descriptor.schema, descriptor.name);

    return this.handleRelation(batch, descriptor, { snapshot: options.snapshot, collectionInfo: collection });
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
      logger.info(`${this.logPrefix} Enabled postImages on ${db}.${collectionInfo.name}`);
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
    const result = await this.storage.resolveTable({
      group_id: this.group_id,
      connection_id: this.connection_id,
      connection_tag: this.connections.connectionTag,
      entity_descriptor: descriptor,
      sync_rules: this.sync_rules
    });
    this.relationCache.update(result.table);

    // Drop conflicting collections.
    // This is generally not expected for MongoDB source dbs, so we log an error.
    if (result.dropTables.length > 0) {
      logger.error(
        `Conflicting collections found for ${JSON.stringify(descriptor)}. Dropping: ${result.dropTables.map((t) => t.id).join(', ')}`
      );
      await batch.drop(result.dropTables);
    }

    // Snapshot if:
    // 1. Snapshot is requested (false for initial snapshot, since that process handles it elsewhere)
    // 2. Snapshot is not already done, AND:
    // 3. The table is used in sync rules.
    const shouldSnapshot = snapshot && !result.table.snapshotComplete && result.table.syncAny;
    if (shouldSnapshot) {
      logger.info(`${this.logPrefix} New collection: ${descriptor.schema}.${descriptor.name}`);
      // Truncate this table, in case a previous snapshot was interrupted.
      await batch.truncate([result.table]);

      await this.snapshotTable(batch, result.table);
      const no_checkpoint_before_lsn = await createCheckpoint(this.client, this.defaultDb, STANDALONE_CHECKPOINT_ID);

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
      logger.debug(`${this.logPrefix} Collection ${table.qualifiedName} not used in sync rules - skipping`);
      return null;
    }

    this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
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
        throw new ChangeStreamInvalidatedError(e.errmsg, e);
      }
      throw e;
    }
  }

  private openChangeStream(options: { lsn: string | null; maxAwaitTimeMs?: number }) {
    const lastLsn = options.lsn ? MongoLSN.fromSerialized(options.lsn) : null;
    const startAfter = lastLsn?.timestamp;
    const resumeAfter = lastLsn?.resumeToken;

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
      showExpandedEvents: true,
      maxAwaitTimeMS: options.maxAwaitTimeMs ?? this.maxAwaitTimeMS,
      fullDocument: fullDocument
    };

    /**
     * Only one of these options can be supplied at a time.
     */
    if (resumeAfter) {
      streamOptions.resumeAfter = resumeAfter;
    } else {
      // Legacy: We don't persist lsns without resumeTokens anymore, but we do still handle the
      // case if we have an old one.
      streamOptions.startAtOperationTime = startAfter;
    }

    let stream: mongo.ChangeStream<mongo.Document>;
    if (filters.multipleDatabases) {
      // Requires readAnyDatabase@admin on Atlas
      stream = this.client.watch(pipeline, streamOptions);
    } else {
      // Same general result, but requires less permissions than the above
      stream = this.defaultDb.watch(pipeline, streamOptions);
    }

    this.abort_signal.addEventListener('abort', () => {
      stream.close();
    });

    return { stream, filters };
  }

  async streamChangesInternal() {
    // Auto-activate as soon as initial replication is done
    await this.storage.autoActivate();

    await this.storage.startBatch(
      { zeroLSN: MongoLSN.ZERO.comparable, defaultSchema: this.defaultDb.databaseName, storeCurrentData: false },
      async (batch) => {
        const { lastCheckpointLsn } = batch;
        const lastLsn = lastCheckpointLsn ? MongoLSN.fromSerialized(lastCheckpointLsn) : null;
        const startAfter = lastLsn?.timestamp;

        logger.info(`${this.logPrefix} Resume streaming at ${startAfter?.inspect()} / ${lastLsn}`);

        const { stream, filters } = this.openChangeStream({ lsn: lastCheckpointLsn });
        if (this.abort_signal.aborted) {
          await stream.close();
          return;
        }

        // Always start with a checkpoint.
        // This helps us to clear errors when restarting, even if there is
        // no data to replicate.
        let waitForCheckpointLsn: string | null = await createCheckpoint(
          this.client,
          this.defaultDb,
          this.checkpointStreamId
        );

        let splitDocument: mongo.ChangeStreamDocument | null = null;

        let flexDbNameWorkaroundLogged = false;

        let lastEmptyResume = performance.now();

        while (true) {
          if (this.abort_signal.aborted) {
            break;
          }

          const originalChangeDocument = await stream.tryNext().catch((e) => {
            throw mapChangeStreamError(e);
          });
          // The stream was closed, we will only ever receive `null` from it
          if (!originalChangeDocument && stream.closed) {
            break;
          }

          if (this.abort_signal.aborted) {
            break;
          }

          if (originalChangeDocument == null) {
            // We get a new null document after `maxAwaitTimeMS` if there were no other events.
            // In this case, stream.resumeToken is the resume token associated with the last response.
            // stream.resumeToken is not updated if stream.tryNext() returns data, while stream.next()
            // does update it.
            // From observed behavior, the actual resumeToken changes around once every 10 seconds.
            // If we don't update it on empty events, we do keep consistency, but resuming the stream
            // with old tokens may cause connection timeouts.
            // We throttle this further by only persisting a keepalive once a minute.
            // We add an additional check for waitForCheckpointLsn == null, to make sure we're not
            // doing a keepalive in the middle of a transaction.
            if (waitForCheckpointLsn == null && performance.now() - lastEmptyResume > 60_000) {
              const { comparable: lsn } = MongoLSN.fromResumeToken(stream.resumeToken);
              await batch.keepalive(lsn);
              await touch();
              lastEmptyResume = performance.now();
            }
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

          if (
            !filters.multipleDatabases &&
            'ns' in changeDocument &&
            changeDocument.ns.db != this.defaultDb.databaseName &&
            changeDocument.ns.db.endsWith(`_${this.defaultDb.databaseName}`)
          ) {
            // When all of the following conditions are met:
            // 1. We're replicating from an Atlas Flex instance.
            // 2. There were changestream events recorded while the PowerSync service is paused.
            // 3. We're only replicating from a single database.
            // Then we've observed an ns with for example {db: '67b83e86cd20730f1e766dde_ps'},
            // instead of the expected {db: 'ps'}.
            // We correct this.
            changeDocument.ns.db = this.defaultDb.databaseName;

            if (!flexDbNameWorkaroundLogged) {
              flexDbNameWorkaroundLogged = true;
              logger.warn(
                `${this.logPrefix} Incorrect DB name in change stream: ${changeDocument.ns.db}. Changed to ${this.defaultDb.databaseName}.`
              );
            }
          }

          const ns = 'ns' in changeDocument && 'coll' in changeDocument.ns ? changeDocument.ns : undefined;

          if (ns?.coll == CHECKPOINTS_COLLECTION) {
            /**
             * Dropping the database does not provide an `invalidate` event.
             * We typically would receive `drop` events for the collection which we
             * would process below.
             *
             * However we don't commit the LSN after collections are dropped.
             * The prevents the `startAfter` or `resumeToken` from advancing past the drop events.
             * The stream also closes after the drop events.
             * This causes an infinite loop of processing the collection drop events.
             *
             * This check here invalidates the change stream if our `_checkpoints` collection
             * is dropped. This allows for detecting when the DB is dropped.
             */
            if (changeDocument.operationType == 'drop') {
              throw new ChangeStreamInvalidatedError(
                'Internal collections have been dropped',
                new Error('_checkpoints collection was dropped')
              );
            }

            if (
              !(
                changeDocument.operationType == 'insert' ||
                changeDocument.operationType == 'update' ||
                changeDocument.operationType == 'replace'
              )
            ) {
              continue;
            }

            // We handle two types of checkpoint events:
            // 1. "Standalone" checkpoints, typically write checkpoints. We want to process these
            //    immediately, regardless of where they were created.
            // 2. "Batch" checkpoints for the current stream. This is used as a form of dynamic rate
            //    limiting of commits, so we specifically want to exclude checkpoints from other streams.
            //
            // It may be useful to also throttle commits due to standalone checkpoints in the future.
            // However, these typically have a much lower rate than batch checkpoints, so we don't do that for now.

            const checkpointId = changeDocument.documentKey._id as string | mongo.ObjectId;
            if (!(checkpointId == STANDALONE_CHECKPOINT_ID || this.checkpointStreamId.equals(checkpointId))) {
              continue;
            }
            const { comparable: lsn } = new MongoLSN({
              timestamp: changeDocument.clusterTime!,
              resume_token: changeDocument._id
            });

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
              waitForCheckpointLsn = await createCheckpoint(this.client, this.defaultDb, this.checkpointStreamId);
            }
            const rel = getMongoRelation(changeDocument.ns);
            const table = await this.getRelation(batch, rel, {
              // In most cases, we should not need to snapshot this. But if this is the first time we see the collection
              // for whatever reason, then we do need to snapshot it.
              // This may result in some duplicate operations when a collection is created for the first time after
              // sync rules was deployed.
              snapshot: true
            });
            if (table.syncAny) {
              await this.writeChange(batch, table, changeDocument);
            }
          } else if (changeDocument.operationType == 'drop') {
            const rel = getMongoRelation(changeDocument.ns);
            const table = await this.getRelation(batch, rel, {
              // We're "dropping" this collection, so never snapshot it.
              snapshot: false
            });
            if (table.syncAny) {
              await batch.drop([table]);
              this.relationCache.delete(table);
            }
          } else if (changeDocument.operationType == 'rename') {
            const relFrom = getMongoRelation(changeDocument.ns);
            const relTo = getMongoRelation(changeDocument.to);
            const tableFrom = await this.getRelation(batch, relFrom, {
              // We're "dropping" this collection, so never snapshot it.
              snapshot: false
            });
            if (tableFrom.syncAny) {
              await batch.drop([tableFrom]);
              this.relationCache.delete(relFrom);
            }
            // Here we do need to snapshot the new table
            const collection = await this.getCollectionInfo(relTo.schema, relTo.name);
            await this.handleRelation(batch, relTo, {
              // This is a new (renamed) collection, so always snapshot it.
              snapshot: true,
              collectionInfo: collection
            });
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

function mapChangeStreamError(e: any) {
  if (isMongoNetworkTimeoutError(e)) {
    // This typically has an unhelpful message like "connection 2 to 159.41.94.47:27017 timed out".
    // We wrap the error to make it more useful.
    throw new DatabaseConnectionError(ErrorCode.PSYNC_S1345, `Timeout while reading MongoDB ChangeStream`, e);
  } else if (
    isMongoServerError(e) &&
    e.codeName == 'NoMatchingDocument' &&
    e.errmsg?.includes('post-image was not found')
  ) {
    throw new ChangeStreamInvalidatedError(e.errmsg, e);
  } else if (isMongoServerError(e) && e.hasErrorLabel('NonResumableChangeStreamError')) {
    throw new ChangeStreamInvalidatedError(e.message, e);
  } else {
    throw new DatabaseConnectionError(ErrorCode.PSYNC_S1346, `Error reading MongoDB ChangeStream`, e);
  }
}
