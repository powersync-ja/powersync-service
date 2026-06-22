import { mongo } from '@powersync/lib-service-mongodb';
import { container, ErrorCode, Logger, ReplicationAbortedError, ServiceError } from '@powersync/lib-services-framework';
import {
  MetricsEngine,
  PerformanceTracer,
  SaveOperationTag,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import { HydratedSyncConfig, TablePattern } from '@powersync/service-sync-rules';
import { ReplicationMetric } from '@powersync/service-types';
import { performance } from 'node:perf_hooks';
import { MongoLSN } from '../common/MongoLSN.js';
import { PostImagesOption } from '../types/types.js';
import { escapeRegExp } from '../utils.js';
import { CheckpointImplementation } from './checkpoints/CheckpointImplementation.js';
import { createCheckpointImplementation } from './checkpoints/create-checkpoint-implementation.js';
import { MongoManager } from './MongoManager.js';
import { getMongoRelation } from './MongoRelation.js';
import { ChunkedSnapshotQuery } from './MongoSnapshotQuery.js';
import { ChangeStreamBatch, parseChangeDocument, rawChangeStream } from './RawChangeStream.js';
import { CHECKPOINTS_COLLECTION, detectDocumentDb } from './replication-utils.js';
import { DirectSourceRowConverter, SourceRowConverter } from './SourceRowConverter.js';

export interface MongoSnapshotterOptions {
  connections: MongoManager;
  storage: storage.SyncRulesBucketStorage;
  metrics: MetricsEngine;
  abortSignal: AbortSignal;
  maxAwaitTimeMS?: number;
  snapshotChunkLength?: number;
  logger?: Logger;
  checkpointStreamId: mongo.ObjectId;
  storageHooks?: storage.StorageHooks;
  snapshotHooks?: MongoSnapshotterHooks;
}

export interface MongoSnapshotterHooks {
  beforeSnapshotStarted?: (table: SourceTable) => Promise<void>;
}

interface InitResult {
  needsInitialSync: boolean;
  snapshotLsn: string | null;
}

interface SnapshotQueueItem {
  table: SourceTable;
  ready: Promise<void>;
  cancelled: boolean;
}

export class MongoSnapshotter {
  private readonly storage: storage.SyncRulesBucketStorage;
  private readonly metrics: MetricsEngine;
  private readonly connections: MongoManager;
  private readonly client: mongo.MongoClient;
  private readonly defaultDb: mongo.Db;
  private readonly syncRules: HydratedSyncConfig;
  private readonly sourceRowConverter: SourceRowConverter;
  private readonly maxAwaitTimeMS: number;
  private readonly snapshotChunkLength: number;
  private readonly abortSignal: AbortSignal;
  private readonly logger: Logger;
  private readonly checkpointStreamId: mongo.ObjectId;
  private readonly storageHooks: storage.StorageHooks | undefined;
  private readonly snapshotHooks: MongoSnapshotterHooks | undefined;
  private readonly changeStreamTimeout: number;

  private readonly connectionId = 1;
  private readonly queue = new Set<SnapshotQueueItem>();
  private initialSnapshotDone = Promise.withResolvers<void>();
  private nextItemQueued: PromiseWithResolvers<void> | null = null;
  private lastTouchedAt = performance.now();

  private isDocumentDb = false;
  private _checkpointImplementation: CheckpointImplementation | null = null;

  constructor(options: MongoSnapshotterOptions) {
    this.storage = options.storage;
    this.metrics = options.metrics;
    this.connections = options.connections;
    this.client = options.connections.client;
    this.defaultDb = options.connections.db;
    this.maxAwaitTimeMS = options.maxAwaitTimeMS ?? 10_000;
    this.snapshotChunkLength = options.snapshotChunkLength ?? 6_000;
    this.abortSignal = options.abortSignal;
    this.logger = options.logger ?? options.storage.logger;
    this.checkpointStreamId = options.checkpointStreamId;
    this.storageHooks = options.storageHooks;
    this.snapshotHooks = options.snapshotHooks;
    this.changeStreamTimeout = Math.ceil(this.client.options.socketTimeoutMS * 0.9);
    this.syncRules = options.storage.getParsedSyncRules({
      defaultSchema: this.defaultDb.databaseName
    });
    this.sourceRowConverter = new DirectSourceRowConverter(this.syncRules.compatibility);

    this.abortSignal.addEventListener('abort', () => {
      this.nextItemQueued?.resolve();
    });
  }

  private get usePostImages() {
    return this.connections.options.postImages != PostImagesOption.OFF;
  }

  private get configurePostImages() {
    return this.connections.options.postImages == PostImagesOption.AUTO_CONFIGURE;
  }

  public get supportsConcurrentSnapshots() {
    return this.storage.storageConfig.softDeleteCurrentData;
  }

  /** The active checkpoint strategy. Only valid after ensureDetected(). */
  private get checkpointImplementation(): CheckpointImplementation {
    if (this._checkpointImplementation == null) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1301,
        'Checkpoint implementation not initialized - call ensureDetected() first'
      );
    }
    return this._checkpointImplementation;
  }

  /**
   * Detect DocumentDB and select the checkpoint implementation. Idempotent.
   * Also validates server topology (sharded/standalone) for standard MongoDB.
   */
  private async ensureDetected(): Promise<void> {
    if (this._checkpointImplementation != null) {
      return;
    }
    this.isDocumentDb = await detectDocumentDb(this.defaultDb);
    if (!this.isDocumentDb) {
      const hello = await this.defaultDb.command({ hello: 1 });
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
    }
    this._checkpointImplementation = createCheckpointImplementation(this.isDocumentDb, {
      client: this.client,
      db: this.defaultDb,
      checkpointStreamId: this.checkpointStreamId,
      logger: this.logger
    });
  }

  async checkSlot(): Promise<InitResult> {
    const status = await this.storage.getStatus();
    if (status.snapshotDone) {
      this.logger.info(`Initial replication already done`);
      return { needsInitialSync: false, snapshotLsn: null };
    }

    return { needsInitialSync: true, snapshotLsn: status.resumeLsn };
  }

  async setupCheckpointsCollection() {
    await this.ensureDetected();
    const collection = await this.getCollectionInfo(this.defaultDb.databaseName, CHECKPOINTS_COLLECTION);
    if (collection == null) {
      await this.defaultDb.createCollection(CHECKPOINTS_COLLECTION, {
        // DocumentDB does not support changeStreamPreAndPostImages.
        ...(this.isDocumentDb ? {} : { changeStreamPreAndPostImages: { enabled: true } })
      });
    } else if (
      !this.isDocumentDb &&
      this.usePostImages &&
      collection.options?.changeStreamPreAndPostImages?.enabled != true
    ) {
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
      //
      // The implementation supplies the filter: the sentinel implementation must
      // preserve the sentinel checkpoint document, since its counter is the
      // globally-ordered component of every committed DocumentDB LSN.
      await this.defaultDb
        .collection(CHECKPOINTS_COLLECTION)
        .deleteMany(this.checkpointImplementation.checkpointClearFilter);
    }
  }

  async queueSnapshotTables(snapshotLsn: string | null) {
    await this.client.connect();
    // Ensure isDocumentDb is set before any getSourceNamespaceFilters() call below
    // (notably the validateSnapshotLsn resume path, which does not go through
    // getSnapshotLsn). Idempotent.
    await this.ensureDetected();
    await using writer = await this.storage.createWriter({
      zeroLSN: MongoLSN.ZERO.comparable,
      defaultSchema: this.defaultDb.databaseName,
      storeCurrentData: false,
      skipExistingRows: true,
      tracer: new PerformanceTracer('MongoDB initial snapshot setup')
    });
    if (snapshotLsn == null) {
      // First replication attempt - get a snapshot and store the timestamp
      snapshotLsn = await this.getSnapshotLsn();
      await writer.setResumeLsn(snapshotLsn);
      this.logger.info(`Marking snapshot at ${snapshotLsn}`);
    } else {
      this.logger.info(`Resuming snapshot at ${snapshotLsn}`);
      // Check that the snapshot is still valid.
      await this.validateSnapshotLsn(snapshotLsn);
    }

    // Start by resolving all tables.
    // This checks postImage configuration, and that should fail as
    // early as possible.
    const allSourceTables: SourceTable[] = [];
    for (const tablePattern of this.syncRules.getSourceTables()) {
      allSourceTables.push(...(await this.resolveQualifiedTableNames(writer, tablePattern)));
    }

    for (const table of allSourceTables) {
      if (table.snapshotComplete) {
        this.logger.info(`Skipping ${table.qualifiedName} - snapshot already done`);
        continue;
      }
      const count = await this.estimatedCountNumber(table);
      const updated = await writer.updateTableProgress(table, {
        totalEstimatedCount: count
      });
      this.queueTable(updated);
      this.logger.info(
        `To replicate: ${updated.qualifiedName}: ${updated.snapshotStatus?.replicatedCount}/~${updated.snapshotStatus?.totalEstimatedCount}`
      );
    }
  }

  async waitForInitialSnapshot() {
    await this.initialSnapshotDone.promise;
  }

  async replicationLoop() {
    try {
      if (this.queue.size == 0) {
        // Special case where we start with no tables to snapshot
        await this.markSnapshotDone();
      }
      while (!this.abortSignal.aborted) {
        const item = this.queue.values().next().value;
        if (item == null) {
          this.initialSnapshotDone.resolve();
          this.nextItemQueued = Promise.withResolvers<void>();
          await this.nextItemQueued.promise;
          this.nextItemQueued = null;
          continue;
        }

        await item.ready;
        if (!item.cancelled) {
          await this.replicateTable(item.table);
        }
        this.queue.delete(item);
        if (this.queue.size == 0) {
          await this.markSnapshotDone();
        }
      }
      throw new ReplicationAbortedError(`Replication snapshotter aborted`, this.abortSignal.reason);
    } catch (e) {
      // If initial snapshot already completed, this has no effect
      this.initialSnapshotDone.reject(e);
      throw e;
    }
  }

  private async queueSnapshot(batch: storage.BucketStorageBatch, table: storage.SourceTable) {
    const ready = Promise.withResolvers<void>();
    const item = this.queueTable(table, ready.promise);
    try {
      await batch.markTableSnapshotRequired(table);
      ready.resolve();
    } catch (e) {
      item.cancelled = true;
      ready.resolve();
      throw e;
    } finally {
      this.nextItemQueued?.resolve();
    }
  }

  /**
   * Snapshot tables.
   *
   * If concurrency is supported, the snapshots are queued and processed in the background.
   * Otherwise, snapshots are processed inline.
   */
  async snapshotTables(batch: storage.BucketStorageBatch, tables: storage.SourceTable[]): Promise<void> {
    if (this.supportsConcurrentSnapshots) {
      // Queue concurrent snapshots
      for (const tableToSnapshot of tables) {
        await this.queueSnapshot(batch, tableToSnapshot);
      }
    } else {
      // No concurrency supported - snapshot inline
      // Truncate in case a previous inline snapshot was interrupted after flushing rows, but before
      // recording snapshot progress. Without this, resuming can replay already-flushed rows on v1/v2 storage.
      await batch.truncate(tables);

      for (const table of tables) {
        await this.snapshotTable(batch, table);
      }
      const noCheckpointBefore = await this.checkpointImplementation.createStandaloneCheckpoint();

      await batch.markTableSnapshotDone(tables, noCheckpointBefore);
    }
  }

  private queueTable(table: SourceTable, ready = Promise.resolve()) {
    const item: SnapshotQueueItem = { table, ready, cancelled: false };
    this.queue.add(item);
    this.nextItemQueued?.resolve();
    return item;
  }

  private async markSnapshotDone() {
    if (this.queue.size != 0) {
      return;
    }

    const status = await this.storage.getStatus();
    if (status.snapshotDone) {
      return;
    }

    // Populate the cache _after_ initial replication, but _before_ we switch to this replication stream.
    // Keeping snapshot_done false until this completes makes this resumable after interruption.
    // No checkpoint exists yet - storage defaults to its highest persisted op id.
    await this.storage.populatePersistentChecksumCache({
      signal: this.abortSignal
    });

    if (this.queue.size != 0) {
      return;
    }

    await using writer = await this.storage.createWriter({
      logger: this.logger,
      zeroLSN: MongoLSN.ZERO.comparable,
      defaultSchema: this.defaultDb.databaseName,
      storeCurrentData: false,
      skipExistingRows: true
    });

    // The checkpoint here is a marker - we need to replicate up to at least this
    // point before the data can be considered consistent.
    const checkpoint = await this.checkpointImplementation.createStandaloneCheckpoint();
    if (this.queue.size != 0) {
      return;
    }

    await writer.markSnapshotDone(checkpoint, {
      // If there is a conflict, we'll try again after the next snapshot
      throwOnConflict: false
    });
    // KLUDGE: We need to create an extra checkpoint _after_ marking the snapshot done, to fix
    // issues with order of processing commits(). This is picked up by tests on postgres storage,
    // the issue may be specific to that storage engine.
    await this.checkpointImplementation.createStandaloneCheckpoint();
  }

  private async replicateTable(tableRequest: SourceTable) {
    await this.snapshotHooks?.beforeSnapshotStarted?.(tableRequest);

    await using writer = await this.storage.createWriter({
      logger: this.logger,
      zeroLSN: MongoLSN.ZERO.comparable,
      defaultSchema: this.defaultDb.databaseName,
      storeCurrentData: false,
      skipExistingRows: true,
      hooks: this.storageHooks,
      tracer: new PerformanceTracer('MongoDB snapshot table')
    });
    // Get fresh table info, in case it was updated while queuing.
    // This deliberately does not resolve by namespace, since that could recreate a replacement source table
    // for a dropped/recreated collection and leave the original queued snapshot with no owner.
    const table = await writer.getSourceTableStatus(tableRequest);
    if (table == null || table.snapshotComplete) {
      return;
    }

    await this.snapshotTable(writer, table);
    const noCheckpointBefore = await this.checkpointImplementation.createStandaloneCheckpoint();
    await writer.markTableSnapshotDone([table], noCheckpointBefore);

    // This commit durably records the persisted ops, so a later checkpoint covers them.
    const resumeLsn = writer.resumeFromLsn ?? MongoLSN.ZERO.comparable;
    await writer.commit(resumeLsn);

    this.logger.info(`Flushed snapshot at ${writer.last_flushed_op}`);
  }

  private async resolveQualifiedTableNames(
    batch: storage.BucketStorageBatch,
    tablePattern: TablePattern
  ): Promise<storage.SourceTable[]> {
    const schema = tablePattern.schema;
    if (tablePattern.connectionTag != this.connections.connectionTag) {
      return [];
    }

    const nameFilter = tablePattern.isWildcard
      ? new RegExp('^' + escapeRegExp(tablePattern.tablePrefix))
      : tablePattern.name;
    // Check if the collection exists
    const collections = await this.client
      .db(schema)
      .listCollections({ name: nameFilter }, { nameOnly: false })
      .toArray();

    if (!tablePattern.isWildcard && collections.length == 0) {
      this.logger.warn(`Collection ${schema}.${tablePattern.name} not found`);
    }

    const result: storage.SourceTable[] = [];
    for (const collection of collections) {
      result.push(
        ...(await this.handleRelation(
          batch,
          getMongoRelation({ db: schema, coll: collection.name }, this.connections.connectionTag),
          {
            collectionInfo: collection
          }
        ))
      );
    }

    return result;
  }

  private async snapshotTable(batch: storage.BucketStorageBatch, table: storage.SourceTable) {
    const rowsReplicatedMetric = this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED);
    const bytesReplicatedMetric = this.metrics.getCounter(ReplicationMetric.DATA_REPLICATED_BYTES);
    const chunksReplicatedMetric = this.metrics.getCounter(ReplicationMetric.CHUNKS_REPLICATED);

    const totalEstimatedCount = await this.estimatedCountNumber(table);
    let at = table.snapshotStatus?.replicatedCount ?? 0;
    const collection = this.client.db(table.schema).collection(table.name);
    await using query = new ChunkedSnapshotQuery({
      collection,
      key: table.snapshotStatus?.lastKey,
      batchSize: this.snapshotChunkLength
    });
    if (query.lastKey != null) {
      this.logger.info(
        `Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()} - resuming at _id > ${query.lastKey}`
      );
    } else {
      this.logger.info(`Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()}`);
    }

    let lastBatch = performance.now();
    let nextChunkPromise = query.nextChunk();
    while (true) {
      const { docs: docBatch, lastKey, bytes: chunkBytes } = await nextChunkPromise;
      if (docBatch.length == 0) {
        // No more data - stop iterating
        break;
      }
      bytesReplicatedMetric.add(chunkBytes);
      chunksReplicatedMetric.add(1);

      if (this.abortSignal.aborted) {
        throw new ReplicationAbortedError(`Aborted initial replication`, this.abortSignal.reason);
      }

      // Pre-fetch next batch, so that we can read and write concurrently
      nextChunkPromise = query.nextChunk();
      for (const buffer of docBatch) {
        const { row, replicaId } = this.sourceRowConverter.rawToSqliteRow(buffer);
        // This auto-flushes when the batch reaches its size limit
        await batch.save({
          tag: SaveOperationTag.INSERT,
          sourceTable: table,
          before: undefined,
          beforeReplicaId: undefined,
          after: row,
          afterReplicaId: replicaId
        });
      }

      // Important: flush before marking progress
      await batch.flush();
      at += docBatch.length;
      rowsReplicatedMetric.add(docBatch.length);

      table = await batch.updateTableProgress(table, {
        lastKey,
        replicatedCount: at,
        totalEstimatedCount
      });

      const duration = performance.now() - lastBatch;
      lastBatch = performance.now();
      this.logger.info(
        `Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()} in ${duration.toFixed(0)}ms`
      );
      this.touch();
    }
    // In case the loop was interrupted, make sure we await the last promise.
    await nextChunkPromise;
  }

  private async handleRelation(
    batch: storage.BucketStorageBatch,
    descriptor: SourceEntityDescriptor,
    options: { collectionInfo: mongo.CollectionInfo | undefined }
  ): Promise<SourceTable[]> {
    if (options.collectionInfo != null) {
      await this.checkPostImages(descriptor.schema, options.collectionInfo);
    } else {
      // If collectionInfo is null, the collection may have been dropped.
      // Ignore the postImages check in this case.
    }

    // Note: resolveTables uses the batch's own parsed sync config set. Passing
    // this.syncRules here would pair sources from one parse with the batch's mapping
    // from another parse.
    const result = await batch.resolveTables({
      connection_id: this.connectionId,
      source: descriptor
    });

    // Drop conflicting collections.
    // This is generally not expected for MongoDB source dbs, so we log an error.
    if (result.dropTables.length > 0) {
      this.logger.error(
        `Conflicting collections found for ${JSON.stringify(descriptor)}. Dropping: ${result.dropTables.map((t) => t.id).join(', ')}`
      );
      await batch.drop(result.dropTables);
    }

    return result.tables;
  }

  private async estimatedCountNumber(table: storage.SourceTable): Promise<number> {
    return await this.client.db(table.schema).collection(table.name).estimatedDocumentCount();
  }

  private async getCollectionInfo(db: string, name: string): Promise<mongo.CollectionInfo | undefined> {
    return (await this.client.db(db).listCollections({ name }, { nameOnly: false }).toArray())[0];
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
      this.logger.info(`Enabled postImages on ${db}.${collectionInfo.name}`);
    } else if (!enabled) {
      throw new ServiceError(ErrorCode.PSYNC_S1343, `postImages not enabled on ${db}.${collectionInfo.name}`);
    }
  }

  private async getSnapshotLsn(): Promise<string> {
    await this.ensureDetected();

    // Open a change stream just to get a resume token for later use.
    // We could use clusterTime from the hello command, but that won't tell us if the
    // snapshot isn't valid anymore.
    // If we just use the first resumeToken from the stream, we get two potential issues:
    // 1. The resumeToken may just be a wrapped clusterTime, which does not detect changes
    //    in source db or other stream issues.
    // 2. The first actual change we get may have the same clusterTime, causing us to incorrect
    //    skip that event.
    // Instead, we create a new checkpoint document, and wait until we get that document back in the stream.
    // To avoid potential race conditions with the checkpoint creation, we create a new checkpoint document
    // periodically until the timeout is reached.
    //
    // For the sentinel implementation (DocumentDB) there is no
    // startAtOperationTime: the stream opens from "now" (lsn null) and the
    // retry loop below re-creates checkpoints until one is observed.

    const LSN_TIMEOUT_SECONDS = 60;
    const LSN_CREATE_INTERVAL_SECONDS = 1;

    this.checkpointImplementation.seedPosition(null);
    const startStreamFromLsn = await this.checkpointImplementation.createFirstBarrier();
    const filters = this.getSourceNamespaceFilters();
    const iter = this.rawChangeStreamBatches({
      lsn: startStreamFromLsn,
      maxAwaitTimeMS: 0,
      signal: this.abortSignal,
      filters
    });
    const startTime = performance.now();
    let lastCheckpointCreated = performance.now();
    let eventsSeen = 0;
    let batchesSeen = 0;

    for await (const { events } of iter) {
      if (performance.now() - startTime >= LSN_TIMEOUT_SECONDS * 1000) {
        break;
      }
      if (performance.now() - lastCheckpointCreated >= LSN_CREATE_INTERVAL_SECONDS * 1000) {
        await this.checkpointImplementation.createBatchCheckpoint();
        lastCheckpointCreated = performance.now();
      }
      batchesSeen += 1;

      for (const rawChangeDocument of events) {
        const changeDocument = parseChangeDocument(rawChangeDocument);
        const ns = 'ns' in changeDocument && 'coll' in changeDocument.ns ? changeDocument.ns : undefined;

        if (ns?.coll == CHECKPOINTS_COLLECTION && 'documentKey' in changeDocument) {
          const kind = this.checkpointImplementation.event.observe(changeDocument);
          if (kind != 'own-barrier') {
            // Standalone events still feed the implementation's coordinate via
            // event.observe above; we only resolve on our own barrier.
            continue;
          }
          // Observing our own barrier has set the coordinate (the barrier event
          // carries the sentinel counter directly), so the LSN is ready.
          return this.checkpointImplementation.event.lsn(changeDocument);
        }

        eventsSeen += 1;
      }
    }

    // Could happen if there is a very large replication lag?
    throw new ServiceError(
      ErrorCode.PSYNC_S1301,
      `Timeout after while waiting for checkpoint document for ${LSN_TIMEOUT_SECONDS}s. Streamed events = ${eventsSeen}, batches = ${batchesSeen}`
    );
  }

  /**
   * Given a snapshot LSN, validate that we can read from it, by opening a change stream.
   */
  private async validateSnapshotLsn(lsn: string) {
    const stream = this.rawChangeStreamBatches({
      lsn,
      maxAwaitTimeMS: 0,
      filters: this.getSourceNamespaceFilters()
    });
    for await (const _batch of stream) {
      break;
    }
  }

  private getSourceNamespaceFilters(): { $match: any; multipleDatabases: boolean } {
    const sourceTables = this.syncRules.getSourceTables();

    const inFilters: { db: string; coll: string }[] = [
      { db: this.defaultDb.databaseName, coll: CHECKPOINTS_COLLECTION }
    ];
    const regexFilters: { 'ns.db': string; 'ns.coll': RegExp }[] = [];
    let multipleDatabases = false;
    for (const tablePattern of sourceTables) {
      if (tablePattern.connectionTag != this.connections.connectionTag) {
        continue;
      }

      if (tablePattern.schema != this.defaultDb.databaseName) {
        multipleDatabases = true;
      }

      if (tablePattern.isWildcard) {
        regexFilters.push({
          'ns.db': tablePattern.schema,
          'ns.coll': new RegExp('^' + escapeRegExp(tablePattern.tablePrefix))
        });
      } else {
        inFilters.push({
          db: tablePattern.schema,
          coll: tablePattern.name
        });
      }
    }

    // DocumentDB always opens a cluster-level change stream, even in single-database
    // mode, so a coll-only filter would match same-named collections in other
    // databases of the cluster. Filter on the full namespace whenever the stream
    // is cluster-scoped. See ChangeStream.getSourceNamespaceFilters for details.
    const useFullNamespaceFilter = this.isDocumentDb || multipleDatabases;
    const nsFilter = useFullNamespaceFilter
      ? { ns: { $in: inFilters } }
      : { 'ns.coll': { $in: inFilters.map((ns) => ns.coll) } };
    if (regexFilters.length > 0) {
      return { $match: { $or: [nsFilter, ...regexFilters] }, multipleDatabases };
    }
    return { $match: nsFilter, multipleDatabases };
  }

  private rawChangeStreamBatches(options: {
    lsn: string | null;
    maxAwaitTimeMS?: number;
    batchSize?: number;
    filters: { $match: any; multipleDatabases: boolean };
    signal?: AbortSignal;
    tracer?: PerformanceTracer<'changestream'>;
  }): AsyncIterableIterator<ChangeStreamBatch> {
    const position = options.lsn ? this.checkpointImplementation.parseResumePosition(options.lsn) : null;
    const startAfter = position?.startAfter ?? undefined;
    const resumeAfter = position?.resumeAfter ?? undefined;

    let fullDocument: 'required' | 'updateLookup';
    if (this.isDocumentDb) {
      // DocumentDB does not support changeStreamPreAndPostImages, so 'required' won't work.
      fullDocument = 'updateLookup';
    } else if (this.usePostImages) {
      // 'read_only' or 'auto_configure'
      // Configuration happens during snapshot, or when we see new
      // collections.
      fullDocument = 'required';
    } else {
      fullDocument = 'updateLookup';
    }
    const streamOptions: mongo.ChangeStreamOptions & mongo.Document = {
      fullDocument
    };
    if (!this.isDocumentDb) {
      // DocumentDB does not support showExpandedEvents.
      streamOptions.showExpandedEvents = true;
    }
    const pipeline: mongo.Document[] = [{ $changeStream: streamOptions }, { $match: options.filters.$match }];
    if (!this.isDocumentDb) {
      // DocumentDB does not support $changeStreamSplitLargeEvent.
      pipeline.push({ $changeStreamSplitLargeEvent: {} });
    }

    // Only one of these options can be supplied at a time.
    if (resumeAfter) {
      streamOptions.resumeAfter = resumeAfter;
    } else if (startAfter != null) {
      // Legacy: We don't persist lsns without resumeTokens anymore, but we do still handle the
      // case if we have an old one. The sentinel implementation never produces a startAfter,
      // and a fresh DocumentDB stream opens from "now" with neither option set.
      streamOptions.startAtOperationTime = startAfter;
    }

    let watchDb: mongo.Db;
    if (this.isDocumentDb || options.filters.multipleDatabases) {
      // DocumentDB only supports cluster-level change streams.
      // Requires readAnyDatabase@admin on Atlas.
      watchDb = this.client.db('admin');
      streamOptions.allChangesForCluster = true;
    } else {
      // Same general result, but requires less permissions than the above
      watchDb = this.defaultDb;
    }

    return rawChangeStream(watchDb, pipeline, {
      batchSize: options.batchSize ?? this.snapshotChunkLength,
      maxAwaitTimeMS: options.maxAwaitTimeMS ?? this.maxAwaitTimeMS,
      maxTimeMS: this.changeStreamTimeout,
      signal: options.signal,
      logger: this.logger,
      tracer: options.tracer
    });
  }

  private touch() {
    if (performance.now() - this.lastTouchedAt > 1_000) {
      this.lastTouchedAt = performance.now();
      // Update the probes, but don't wait for it
      container.probes.touch().catch((e) => {
        this.logger.error(`Failed to touch the container probe: ${e.message}`, e);
      });
    }
  }
}
