import { mongo } from '@powersync/lib-service-mongodb';
import {
  container,
  logger as defaultLogger,
  ErrorCode,
  Logger,
  ReplicationAbortedError,
  ServiceError
} from '@powersync/lib-services-framework';
import { InternalOpId, MetricsEngine, SaveOperationTag, SourceTable, storage } from '@powersync/service-core';
import { DatabaseInputRow, RowProcessor, SqliteInputRow, SqliteRow, TablePattern } from '@powersync/service-sync-rules';
import { ReplicationMetric } from '@powersync/service-types';
import * as timers from 'node:timers/promises';
import pDefer, { DeferredPromise } from 'p-defer';
import { MongoLSN } from '../common/MongoLSN.js';
import { PostImagesOption } from '../types/types.js';
import { escapeRegExp } from '../utils.js';
import { mapChangeStreamError } from './ChangeStreamErrors.js';
import { MongoManager } from './MongoManager.js';
import { constructAfterRecord, createCheckpoint, getMongoRelation, STANDALONE_CHECKPOINT_ID } from './MongoRelation.js';
import { ChunkedSnapshotQuery } from './MongoSnapshotQuery.js';
import { CHECKPOINTS_COLLECTION } from './replication-utils.js';

export interface MongoSnapshotterOptions {
  connections: MongoManager;
  writer: () => Promise<storage.BucketDataWriter>;
  metrics: MetricsEngine;
  abort_signal: AbortSignal;
  /**
   * Override maxAwaitTimeMS for testing.
   */
  maxAwaitTimeMS?: number;
  /**
   * Override snapshotChunkLength for testing.
   */
  snapshotChunkLength?: number;
  logger?: Logger;
  checkpointStreamId: mongo.ObjectId;
}

export class MongoSnapshotter {
  connection_id = 1;

  private readonly writerFactory: () => Promise<storage.BucketDataWriter>;

  private readonly metrics: MetricsEngine;

  private connections: MongoManager;
  private readonly client: mongo.MongoClient;
  private readonly defaultDb: mongo.Db;

  private readonly maxAwaitTimeMS: number;
  private readonly snapshotChunkLength: number;

  private abortSignal: AbortSignal;

  private logger: Logger;

  private checkpointStreamId: mongo.ObjectId;
  private changeStreamTimeout: number;

  private queue = new Set<SourceTable>();
  private nextItemQueued: DeferredPromise<void> | null = null;
  private initialSnapshotDone = pDefer<void>();
  private lastSnapshotOpId: InternalOpId | null = null;

  constructor(options: MongoSnapshotterOptions) {
    this.writerFactory = options.writer;
    this.metrics = options.metrics;
    this.connections = options.connections;
    this.maxAwaitTimeMS = options.maxAwaitTimeMS ?? 10_000;
    this.snapshotChunkLength = options.snapshotChunkLength ?? 6_000;
    this.client = this.connections.client;
    this.defaultDb = this.connections.db;
    this.abortSignal = options.abort_signal;
    this.logger = options.logger ?? defaultLogger;
    this.checkpointStreamId = options.checkpointStreamId;
    this.changeStreamTimeout = Math.ceil(this.client.options.socketTimeoutMS * 0.9);

    this.abortSignal.addEventListener('abort', () => {
      // Wake up the queue if is waiting for items
      this.nextItemQueued?.resolve();
    });
  }

  private get usePostImages() {
    return this.connections.options.postImages != PostImagesOption.OFF;
  }

  private get configurePostImages() {
    return this.connections.options.postImages == PostImagesOption.AUTO_CONFIGURE;
  }

  async queueSnapshotTables(snapshotLsn: string | null) {
    await using writer = await this.writerFactory();
    const sourceTables = writer.rowProcessor.getSourceTables();

    if (snapshotLsn == null) {
      // First replication attempt - get a snapshot and store the timestamp
      snapshotLsn = await this.getSnapshotLsn(writer);
      // FIXME: check the logic for resumeLSN.
      await writer.setAllResumeLsn(snapshotLsn);
      this.logger.info(`Marking snapshot at ${snapshotLsn}`);
    } else {
      this.logger.info(`Resuming snapshot at ${snapshotLsn}`);
      // Check that the snapshot is still valid.
      await this.validateSnapshotLsn(writer, snapshotLsn);
    }

    // Start by resolving all tables.
    // This checks postImage configuration, and that should fail as
    // early as possible.
    // This resolves _all_ tables, including those already snapshotted.
    let allSourceTables: SourceTable[] = [];
    for (let tablePattern of sourceTables) {
      const tables = await this.resolveQualifiedTableNames(writer, tablePattern);
      allSourceTables.push(...tables);
    }

    let tablesWithStatus: SourceTable[] = [];
    for (let table of allSourceTables) {
      if (table.snapshotComplete) {
        this.logger.info(`Skipping ${table.qualifiedName} - snapshot already done`);
        continue;
      }
      const count = await this.estimatedCountNumber(table);
      const updated = await writer.updateTableProgress(table, {
        totalEstimatedCount: count
      });
      tablesWithStatus.push(updated);
      this.logger.info(
        `To replicate: ${updated.qualifiedName}: ${updated.snapshotStatus?.replicatedCount}/~${updated.snapshotStatus?.totalEstimatedCount}`
      );
    }

    for (let table of tablesWithStatus) {
      this.queue.add(table);
    }
    this.nextItemQueued?.resolve();
  }

  async waitForInitialSnapshot() {
    await this.initialSnapshotDone.promise;
  }

  async replicationLoop() {
    try {
      await using writer = await this.writerFactory();
      if (this.queue.size == 0) {
        // Special case where we start with no tables to snapshot
        await this.markSnapshotDone(writer);
      }
      while (!this.abortSignal.aborted) {
        const table = this.queue.values().next().value;
        if (table == null) {
          this.initialSnapshotDone.resolve();
          // There must be no await in between checking the queue above and creating this deferred promise,
          // otherwise we may miss new items being queued.
          this.nextItemQueued = pDefer<void>();
          await this.nextItemQueued.promise;
          this.nextItemQueued = null;
          // At this point, either we have have a new item in the queue, or we are aborted.
          continue;
        }

        await this.replicateTable(writer, table);
        this.queue.delete(table);
        if (this.queue.size == 0) {
          await this.markSnapshotDone(writer);
        }
      }
      throw new ReplicationAbortedError(`Replication loop aborted`, this.abortSignal.reason);
    } catch (e) {
      // If initial snapshot already completed, this has no effect
      this.initialSnapshotDone.reject(e);
      throw e;
    }
  }

  private async markSnapshotDone(writer: storage.BucketDataWriter) {
    // The checkpoint here is a marker - we need to replicate up to at least this
    // point before the data can be considered consistent.
    const checkpoint = await createCheckpoint(this.client, this.defaultDb, STANDALONE_CHECKPOINT_ID);
    await writer.markAllSnapshotDone(checkpoint);
    // KLUDGE: We need to create an extra checkpoint _after_ marking the snapshot done, to fix
    // issues with order of processing commits(). This is picked up by tests on postgres storage,
    // the issue may be specific to that storage engine.
    await createCheckpoint(this.client, this.defaultDb, STANDALONE_CHECKPOINT_ID);

    if (this.lastSnapshotOpId != null) {
      // Populate the cache _after_ initial replication, but _before_ we switch to this sync rules.
      // TODO: only run this after initial replication, not after each table.
      // FIXME: implement this again
      // await this.storage.populatePersistentChecksumCache({
      //   // No checkpoint yet, but we do have the opId.
      //   maxOpId: this.lastSnapshotOpId,
      //   signal: this.abortSignal
      // });
    }
  }

  private async replicateTable(writer: storage.BucketDataWriter, tableRequest: SourceTable) {
    // Get fresh table info, in case it was updated while queuing
    const table = await writer.getTable(tableRequest);
    if (table == null) {
      return;
    }
    if (table.snapshotComplete) {
      return;
    }
    await this.snapshotTable(writer, table);

    const noCheckpointBefore = await createCheckpoint(this.client, this.defaultDb, STANDALONE_CHECKPOINT_ID);
    await writer.markTableSnapshotDone([table], noCheckpointBefore);

    // This commit ensures we set keepalive_op.
    const resumeLsn = writer.resumeFromLsn ?? MongoLSN.ZERO.comparable;
    // FIXME: Only commit on relevant syncRules?

    await writer.commitAll(resumeLsn);

    // FIXME: check this
    // if (flushResults?.flushed_op != null) {
    //   this.lastSnapshotOpId = flushResults.flushed_op;
    // }
    this.logger.info(`Flushed snapshot at ${this.lastSnapshotOpId}`);
  }

  private queueTable(table: storage.SourceTable) {
    // These two operations must be atomic to avoid race conditions
    this.queue.add(table);
    this.nextItemQueued?.resolve();
  }

  async queueSnapshot(writer: storage.BucketDataWriter, table: storage.SourceTable) {
    await writer.markTableSnapshotRequired(table);
    this.queueTable(table);
  }

  async estimatedCount(table: storage.SourceTable): Promise<string> {
    const count = await this.estimatedCountNumber(table);
    return `~${count}`;
  }

  async estimatedCountNumber(table: storage.SourceTable): Promise<number> {
    const db = this.client.db(table.schema);
    return await db.collection(table.name).estimatedDocumentCount();
  }

  private async resolveQualifiedTableNames(
    writer: storage.BucketDataWriter,
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
      this.logger.warn(`Collection ${schema}.${tablePattern.name} not found`);
    }

    for (let collection of collections) {
      await this.checkPostImages(schema, collection);
      const sourceTables = await writer.resolveTables({
        connection_id: this.connection_id,
        connection_tag: this.connections.connectionTag,
        entity_descriptor: getMongoRelation({ db: schema, coll: collection.name }),
        pattern: tablePattern
      });
      // TODO: dropTables?
      result.push(...sourceTables.tables);
    }

    return result;
  }

  private async snapshotTable(writer: storage.BucketDataWriter, table: storage.SourceTable) {
    const totalEstimatedCount = await this.estimatedCountNumber(table);
    let at = table.snapshotStatus?.replicatedCount ?? 0;
    const db = this.client.db(table.schema);
    const collection = db.collection(table.name);
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
      const { docs: docBatch, lastKey } = await nextChunkPromise;
      if (docBatch.length == 0) {
        // No more data - stop iterating
        break;
      }

      if (this.abortSignal.aborted) {
        throw new ReplicationAbortedError(`Aborted initial replication`, this.abortSignal.reason);
      }

      // Pre-fetch next batch, so that we can read and write concurrently
      nextChunkPromise = query.nextChunk();
      for (let document of docBatch) {
        const record = this.constructAfterRecord(writer.rowProcessor, document);

        // This auto-flushes when the batch reaches its size limit
        await writer.save({
          tag: SaveOperationTag.INSERT,
          sourceTable: table,
          before: undefined,
          beforeReplicaId: undefined,
          after: record,
          afterReplicaId: document._id
        });
      }

      // Important: flush before marking progress
      const flushResult = await writer.flush();
      if (flushResult != null) {
        this.lastSnapshotOpId = flushResult.flushed_op;
      }
      at += docBatch.length;
      this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(docBatch.length);

      table = await writer.updateTableProgress(table, {
        lastKey,
        replicatedCount: at,
        totalEstimatedCount: totalEstimatedCount
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

  private constructAfterRecord(rowProcessor: RowProcessor, document: mongo.Document): SqliteRow {
    const inputRow = constructAfterRecord(document);
    return rowProcessor.applyRowContext<never>(inputRow);
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
      this.logger.info(`Enabled postImages on ${db}.${collectionInfo.name}`);
    } else if (!enabled) {
      throw new ServiceError(ErrorCode.PSYNC_S1343, `postImages not enabled on ${db}.${collectionInfo.name}`);
    }
  }

  private async getSnapshotLsn(writer: storage.BucketDataWriter): Promise<string> {
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
    // If we just use the first resumeToken from the stream, we get two potential issues:
    // 1. The resumeToken may just be a wrapped clusterTime, which does not detect changes
    //    in source db or other stream issues.
    // 2. The first actual change we get may have the same clusterTime, causing us to incorrect
    //    skip that event.
    // Instead, we create a new checkpoint document, and wait until we get that document back in the stream.
    // To avoid potential race conditions with the checkpoint creation, we create a new checkpoint document
    // periodically until the timeout is reached.

    const LSN_TIMEOUT_SECONDS = 60;
    const LSN_CREATE_INTERVAL_SECONDS = 1;

    await using streamManager = this.openChangeStream(writer, { lsn: null, maxAwaitTimeMs: 0 });
    const { stream } = streamManager;
    const startTime = performance.now();
    let lastCheckpointCreated = -10_000;
    let eventsSeen = 0;

    while (performance.now() - startTime < LSN_TIMEOUT_SECONDS * 1000) {
      if (performance.now() - lastCheckpointCreated >= LSN_CREATE_INTERVAL_SECONDS * 1000) {
        await createCheckpoint(this.client, this.defaultDb, this.checkpointStreamId);
        lastCheckpointCreated = performance.now();
      }

      // tryNext() doesn't block, while next() / hasNext() does block until there is data on the stream
      const changeDocument = await stream.tryNext().catch((e) => {
        throw mapChangeStreamError(e);
      });
      if (changeDocument == null) {
        continue;
      }

      const ns = 'ns' in changeDocument && 'coll' in changeDocument.ns ? changeDocument.ns : undefined;

      if (ns?.coll == CHECKPOINTS_COLLECTION && 'documentKey' in changeDocument) {
        const checkpointId = changeDocument.documentKey._id as string | mongo.ObjectId;
        if (!this.checkpointStreamId.equals(checkpointId)) {
          continue;
        }
        const { comparable: lsn } = new MongoLSN({
          timestamp: changeDocument.clusterTime!,
          resume_token: changeDocument._id
        });
        return lsn;
      }

      eventsSeen += 1;
    }

    // Could happen if there is a very large replication lag?
    throw new ServiceError(
      ErrorCode.PSYNC_S1301,
      `Timeout after while waiting for checkpoint document for ${LSN_TIMEOUT_SECONDS}s. Streamed events = ${eventsSeen}`
    );
  }

  /**
   * Given a snapshot LSN, validate that we can read from it, by opening a change stream.
   */
  private async validateSnapshotLsn(writer: storage.BucketDataWriter, lsn: string) {
    await using streamManager = this.openChangeStream(writer, { lsn: lsn, maxAwaitTimeMs: 0 });
    const { stream } = streamManager;
    try {
      // tryNext() doesn't block, while next() / hasNext() does block until there is data on the stream
      await stream.tryNext();
    } catch (e) {
      // Note: A timeout here is not handled as a ChangeStreamInvalidatedError, even though
      // we possibly cannot recover from it.
      throw mapChangeStreamError(e);
    }
  }

  private getSourceNamespaceFilters(rowProcessor: RowProcessor): { $match: any; multipleDatabases: boolean } {
    const sourceTables = rowProcessor.getSourceTables();

    let $inFilters: { db: string; coll: string }[] = [
      { db: this.defaultDb.databaseName, coll: CHECKPOINTS_COLLECTION }
    ];
    let $refilters: { 'ns.db': string; 'ns.coll': RegExp }[] = [];
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

    const nsFilter = multipleDatabases
      ? { ns: { $in: $inFilters } }
      : { 'ns.coll': { $in: $inFilters.map((ns) => ns.coll) } };
    if ($refilters.length > 0) {
      return { $match: { $or: [nsFilter, ...$refilters] }, multipleDatabases };
    }
    return { $match: nsFilter, multipleDatabases };
  }

  static *getQueryData(results: Iterable<DatabaseInputRow>): Generator<SqliteInputRow> {
    for (let row of results) {
      yield constructAfterRecord(row);
    }
  }

  private openChangeStream(writer: storage.BucketDataWriter, options: { lsn: string | null; maxAwaitTimeMs?: number }) {
    const lastLsn = options.lsn ? MongoLSN.fromSerialized(options.lsn) : null;
    const startAfter = lastLsn?.timestamp;
    const resumeAfter = lastLsn?.resumeToken;

    const filters = this.getSourceNamespaceFilters(writer.rowProcessor);

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
      fullDocument: fullDocument,
      maxTimeMS: this.changeStreamTimeout
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

    this.abortSignal.addEventListener('abort', () => {
      stream.close();
    });

    return {
      stream,
      filters,
      [Symbol.asyncDispose]: async () => {
        return stream.close();
      }
    };
  }

  private lastTouchedAt = performance.now();

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
