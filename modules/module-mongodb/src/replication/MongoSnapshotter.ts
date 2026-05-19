import { mongo } from '@powersync/lib-service-mongodb';
import { container, ErrorCode, Logger, ReplicationAbortedError, ServiceError } from '@powersync/lib-services-framework';
import {
  InternalOpId,
  MetricsEngine,
  PerformanceTracer,
  RelationCache,
  SaveOperationTag,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import { HydratedSyncRules, TablePattern } from '@powersync/service-sync-rules';
import { ReplicationMetric } from '@powersync/service-types';
import { performance } from 'node:perf_hooks';
import { MongoLSN } from '../common/MongoLSN.js';
import { PostImagesOption } from '../types/types.js';
import { escapeRegExp } from '../utils.js';
import { MongoManager } from './MongoManager.js';
import { createCheckpoint, getCacheIdentifier, getMongoRelation, STANDALONE_CHECKPOINT_ID } from './MongoRelation.js';
import { ChunkedSnapshotQuery } from './MongoSnapshotQuery.js';
import { ChangeStreamBatch, parseChangeDocument, rawChangeStream } from './RawChangeStream.js';
import { CHECKPOINTS_COLLECTION } from './replication-utils.js';
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
  private readonly syncRules: HydratedSyncRules;
  private readonly sourceRowConverter: SourceRowConverter;
  private readonly maxAwaitTimeMS: number;
  private readonly snapshotChunkLength: number;
  private readonly abortSignal: AbortSignal;
  private readonly logger: Logger;
  private readonly checkpointStreamId: mongo.ObjectId;
  private readonly changeStreamTimeout: number;
  private readonly relationCache = new RelationCache(getCacheIdentifier);

  private readonly connectionId = 1;
  private readonly queue = new Set<SnapshotQueueItem>();
  private initialSnapshotDone = Promise.withResolvers<void>();
  private nextItemQueued: PromiseWithResolvers<void> | null = null;
  private lastSnapshotOpId: InternalOpId | null = null;
  private lastTouchedAt = performance.now();

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

  async checkSlot(): Promise<InitResult> {
    const status = await this.storage.getStatus();
    if (status.snapshot_done && status.checkpoint_lsn) {
      this.logger.info(`Initial replication already done`);
      return { needsInitialSync: false, snapshotLsn: null };
    }

    return { needsInitialSync: true, snapshotLsn: status.snapshot_lsn };
  }

  async setupCheckpointsCollection() {
    const collection = await this.getCollectionInfo(this.defaultDb.databaseName, CHECKPOINTS_COLLECTION);
    if (collection == null) {
      await this.defaultDb.createCollection(CHECKPOINTS_COLLECTION, {
        changeStreamPreAndPostImages: { enabled: true }
      });
    } else if (this.usePostImages && collection.options?.changeStreamPreAndPostImages?.enabled != true) {
      await this.defaultDb.dropCollection(CHECKPOINTS_COLLECTION);
      await this.defaultDb.createCollection(CHECKPOINTS_COLLECTION, {
        changeStreamPreAndPostImages: { enabled: true }
      });
    } else {
      await this.defaultDb.collection(CHECKPOINTS_COLLECTION).deleteMany({});
    }
  }

  async queueSnapshotTables(snapshotLsn: string | null) {
    await this.client.connect();
    await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: MongoLSN.ZERO.comparable,
        defaultSchema: this.defaultDb.databaseName,
        storeCurrentData: false,
        skipExistingRows: true,
        tracer: new PerformanceTracer('MongoDB initial snapshot setup')
      },
      async (batch) => {
        if (snapshotLsn == null) {
          snapshotLsn = await this.getSnapshotLsn();
          await batch.setResumeLsn(snapshotLsn);
          this.logger.info(`Marking snapshot at ${snapshotLsn}`);
        } else {
          this.logger.info(`Resuming snapshot at ${snapshotLsn}`);
          await this.validateSnapshotLsn(snapshotLsn);
        }

        const allSourceTables: SourceTable[] = [];
        for (const tablePattern of this.syncRules.getSourceTables()) {
          allSourceTables.push(...(await this.resolveQualifiedTableNames(batch, tablePattern)));
        }

        for (const table of allSourceTables) {
          if (table.snapshotComplete) {
            this.logger.info(`Skipping ${table.qualifiedName} - snapshot already done`);
            continue;
          }
          const count = await this.estimatedCountNumber(table);
          const updated = await batch.updateTableProgress(table, {
            totalEstimatedCount: count
          });
          this.relationCache.update(updated);
          this.queueTable(updated);
          this.logger.info(
            `To replicate: ${updated.qualifiedName}: ${updated.snapshotStatus?.replicatedCount}/~${updated.snapshotStatus?.totalEstimatedCount}`
          );
        }
      }
    );
  }

  async waitForInitialSnapshot() {
    await this.initialSnapshotDone.promise;
  }

  async replicationLoop() {
    try {
      if (this.queue.size == 0) {
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
      this.initialSnapshotDone.reject(e);
      throw e;
    }
  }

  async queueSnapshot(batch: storage.BucketStorageBatch, table: storage.SourceTable) {
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

  private queueTable(table: SourceTable, ready = Promise.resolve()) {
    const item: SnapshotQueueItem = { table, ready, cancelled: false };
    this.queue.add(item);
    this.nextItemQueued?.resolve();
    return item;
  }

  private async markSnapshotDone() {
    let markedDone = false;
    const flushResult = await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: MongoLSN.ZERO.comparable,
        defaultSchema: this.defaultDb.databaseName,
        storeCurrentData: false,
        skipExistingRows: true
      },
      async (batch) => {
        if (this.queue.size != 0) {
          return;
        }
        const checkpoint = await createCheckpoint(this.client, this.defaultDb, STANDALONE_CHECKPOINT_ID);
        if (this.queue.size != 0) {
          return;
        }
        await batch.markAllSnapshotDone(checkpoint);
        await createCheckpoint(this.client, this.defaultDb, STANDALONE_CHECKPOINT_ID);
        markedDone = true;
      }
    );
    if (!markedDone) {
      return;
    }

    const lastOp = flushResult?.flushed_op ?? this.lastSnapshotOpId;
    if (lastOp != null) {
      await this.storage.populatePersistentChecksumCache({
        maxOpId: lastOp,
        signal: this.abortSignal
      });
    }
  }

  private async replicateTable(tableRequest: SourceTable) {
    const flushResult = await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: MongoLSN.ZERO.comparable,
        defaultSchema: this.defaultDb.databaseName,
        storeCurrentData: false,
        skipExistingRows: true,
        tracer: new PerformanceTracer('MongoDB snapshot table')
      },
      async (batch) => {
        const tables = await this.handleRelation(
          batch,
          getMongoRelation({ db: tableRequest.schema, coll: tableRequest.name }, this.connections.connectionTag),
          { collectionInfo: undefined }
        );
        const table = tables.find((candidate) => sourceTableIdsEqual(candidate.id, tableRequest.id));
        if (table == null || table.snapshotComplete) {
          return;
        }

        await this.snapshotTable(batch, table);
        const noCheckpointBefore = await createCheckpoint(this.client, this.defaultDb, STANDALONE_CHECKPOINT_ID);
        await batch.markTableSnapshotDone([table], noCheckpointBefore);

        const resumeLsn = batch.resumeFromLsn ?? MongoLSN.ZERO.comparable;
        await batch.commit(resumeLsn);
      }
    );
    if (flushResult?.flushed_op != null) {
      this.lastSnapshotOpId = flushResult.flushed_op;
    }
    this.logger.info(`Flushed snapshot at ${flushResult?.flushed_op}`);
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
        break;
      }
      bytesReplicatedMetric.add(chunkBytes);
      chunksReplicatedMetric.add(1);

      if (this.abortSignal.aborted) {
        throw new ReplicationAbortedError(`Aborted initial replication`, this.abortSignal.reason);
      }

      nextChunkPromise = query.nextChunk();
      for (const buffer of docBatch) {
        const { row, replicaId } = this.sourceRowConverter.rawToSqliteRow(buffer);
        await batch.save({
          tag: SaveOperationTag.INSERT,
          sourceTable: table,
          before: undefined,
          beforeReplicaId: undefined,
          after: row,
          afterReplicaId: replicaId
        });
      }

      const result = await batch.flush();
      if (result?.flushed_op != null) {
        this.lastSnapshotOpId = result.flushed_op;
      }
      at += docBatch.length;
      rowsReplicatedMetric.add(docBatch.length);

      table = await batch.updateTableProgress(table, {
        lastKey,
        replicatedCount: at,
        totalEstimatedCount
      });
      this.relationCache.update(table);

      const duration = performance.now() - lastBatch;
      lastBatch = performance.now();
      this.logger.info(
        `Replicating ${table.qualifiedName} ${table.formatSnapshotProgress()} in ${duration.toFixed(0)}ms`
      );
      this.touch();
    }
    await nextChunkPromise;
  }

  private async handleRelation(
    batch: storage.BucketStorageBatch,
    descriptor: SourceEntityDescriptor,
    options: { collectionInfo: mongo.CollectionInfo | undefined }
  ): Promise<SourceTable[]> {
    if (options.collectionInfo != null) {
      await this.checkPostImages(descriptor.schema, options.collectionInfo);
    }

    const result = await batch.resolveTables({
      connection_id: this.connectionId,
      source: descriptor,
      syncRules: this.syncRules
    });
    this.relationCache.updateAll(descriptor, result.tables);

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

    const LSN_TIMEOUT_SECONDS = 60;
    const LSN_CREATE_INTERVAL_SECONDS = 1;

    const firstCheckpointLsn = await createCheckpoint(this.client, this.defaultDb, this.checkpointStreamId);
    const filters = this.getSourceNamespaceFilters();
    const iter = this.rawChangeStreamBatches({
      lsn: firstCheckpointLsn,
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
        await createCheckpoint(this.client, this.defaultDb, this.checkpointStreamId);
        lastCheckpointCreated = performance.now();
      }
      batchesSeen += 1;

      for (const rawChangeDocument of events) {
        const changeDocument = parseChangeDocument(rawChangeDocument);
        const ns = 'ns' in changeDocument && 'coll' in changeDocument.ns ? changeDocument.ns : undefined;

        if (ns?.coll == CHECKPOINTS_COLLECTION && 'documentKey' in changeDocument) {
          const checkpointId = changeDocument.documentKey._id as string | mongo.ObjectId;
          if (!this.checkpointStreamId.equals(checkpointId)) {
            continue;
          }
          return new MongoLSN({
            timestamp: changeDocument.clusterTime!,
            resume_token: changeDocument._id
          }).comparable;
        }

        eventsSeen += 1;
      }
    }

    throw new ServiceError(
      ErrorCode.PSYNC_S1301,
      `Timeout after while waiting for checkpoint document for ${LSN_TIMEOUT_SECONDS}s. Streamed events = ${eventsSeen}, batches = ${batchesSeen}`
    );
  }

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

    const nsFilter = multipleDatabases
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
    const lastLsn = options.lsn ? MongoLSN.fromSerialized(options.lsn) : null;
    const startAfter = lastLsn?.timestamp;
    const resumeAfter = lastLsn?.resumeToken;

    let fullDocument: 'required' | 'updateLookup';
    if (this.usePostImages) {
      fullDocument = 'required';
    } else {
      fullDocument = 'updateLookup';
    }
    const streamOptions: mongo.ChangeStreamOptions & mongo.Document = {
      showExpandedEvents: true,
      fullDocument
    };
    const pipeline: mongo.Document[] = [
      { $changeStream: streamOptions },
      { $match: options.filters.$match },
      { $changeStreamSplitLargeEvent: {} }
    ];

    if (resumeAfter) {
      streamOptions.resumeAfter = resumeAfter;
    } else {
      streamOptions.startAtOperationTime = startAfter;
    }

    let watchDb: mongo.Db;
    if (options.filters.multipleDatabases) {
      watchDb = this.client.db('admin');
      streamOptions.allChangesForCluster = true;
    } else {
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
      container.probes.touch().catch((e) => {
        this.logger.error(`Failed to touch the container probe: ${e.message}`, e);
      });
    }
  }
}

function sourceTableIdsEqual(left: SourceTable['id'], right: SourceTable['id']) {
  if (left instanceof mongo.ObjectId && right instanceof mongo.ObjectId) {
    return left.equals(right);
  }
  return String(left) == String(right);
}
