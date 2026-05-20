import { mongo } from '@powersync/lib-service-mongodb';
import {
  container,
  DatabaseConnectionError,
  ErrorCode,
  Logger,
  ReplicationAbortedError,
  ReplicationAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import {
  MetricsEngine,
  PerformanceTracer,
  RelationCache,
  ReplicationLagTracker,
  SaveOperationTag,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import { HydratedSyncRules } from '@powersync/service-sync-rules';
import { ReplicationMetric } from '@powersync/service-types';
import { performance } from 'node:perf_hooks';
import { MongoLSN } from '../common/MongoLSN.js';
import { PostImagesOption } from '../types/types.js';
import { escapeRegExp } from '../utils.js';
import { MongoManager } from './MongoManager.js';
import { createCheckpoint, getCacheIdentifier, getMongoRelation, STANDALONE_CHECKPOINT_ID } from './MongoRelation.js';
import { MongoSnapshotter } from './MongoSnapshotter.js';
import {
  ChangeStreamBatch,
  parseChangeDocument,
  ProjectedChangeStreamDocument,
  rawChangeStream
} from './RawChangeStream.js';
import { CHECKPOINTS_COLLECTION, timestampToDate } from './replication-utils.js';
import { DirectSourceRowConverter, SourceRowConverter } from './SourceRowConverter.js';
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

  /**
   * Override snapshotChunkLength for testing.
   */
  snapshotChunkLength?: number;

  storageHooks?: storage.StorageHooks;

  logger?: Logger;
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
  sync_rules: HydratedSyncRules;
  group_id: number;

  connection_id = 1;

  private readonly storage: storage.SyncRulesBucketStorage;

  private connections: MongoManager;
  private readonly client: mongo.MongoClient;
  private readonly defaultDb: mongo.Db;
  private readonly metrics: MetricsEngine;

  private readonly maxAwaitTimeMS: number;

  private abortController = new AbortController();
  private abortSignal: AbortSignal = this.abortController.signal;

  private initPromise: Promise<void> | null = null;
  private snapshotter: MongoSnapshotter;

  /**
   * We use the relationCache _only_ for caching static SourceTable info, not for snapshot status.
   */
  private relationCache = new RelationCache(getCacheIdentifier);

  private replicationLag = new ReplicationLagTracker();

  private checkpointStreamId = new mongo.ObjectId();

  private logger: Logger;

  private snapshotChunkLength: number;

  private changeStreamTimeout: number;

  private storageHooks: storage.StorageHooks | undefined;

  private readonly sourceRowConverter: SourceRowConverter;

  constructor(options: ChangeStreamOptions) {
    this.storage = options.storage;
    this.metrics = options.metrics;
    this.group_id = options.storage.group_id;
    this.connections = options.connections;
    this.maxAwaitTimeMS = options.maxAwaitTimeMS ?? 10_000;
    this.snapshotChunkLength = options.snapshotChunkLength ?? 6_000;
    this.storageHooks = options.storageHooks;
    this.client = this.connections.client;
    this.defaultDb = this.connections.db;
    this.sync_rules = options.storage.getParsedSyncRules({
      defaultSchema: this.defaultDb.databaseName
    });
    this.sourceRowConverter = new DirectSourceRowConverter(this.sync_rules.compatibility);

    // The change stream aggregation command should timeout before the socket times out,
    // so we use 90% of the socket timeout value.
    this.changeStreamTimeout = Math.ceil(this.client.options.socketTimeoutMS * 0.9);

    this.logger = options.logger ?? this.storage.logger;
    this.snapshotter = new MongoSnapshotter({
      ...options,
      abortSignal: this.abortSignal,
      logger: this.logger,
      checkpointStreamId: this.checkpointStreamId
    });

    options.abort_signal.addEventListener(
      'abort',
      () => {
        this.abortController.abort(options.abort_signal.reason);
      },
      { once: true }
    );
    if (options.abort_signal.aborted) {
      this.abortController.abort(options.abort_signal.reason);
    }
  }

  get stopped() {
    return this.abortSignal.aborted;
  }

  private get usePostImages() {
    return this.connections.options.postImages != PostImagesOption.OFF;
  }

  private get configurePostImages() {
    return this.connections.options.postImages == PostImagesOption.AUTO_CONFIGURE;
  }

  private get supportsConcurrentSnapshots() {
    return this.storage.storageConfig.softDeleteCurrentData;
  }

  private getSourceNamespaceFilters(): { $match: any; multipleDatabases: boolean } {
    const sourceTables = this.sync_rules.getSourceTables();

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

    // When we have a large number of collections, the performance of the pipeline
    // depends a lot on how the filters here are specified.
    // Currently, only the multipleDatabases == false case is optimized, and the
    // wildcard matching version is not tested (but we assume that will be more
    // limited in the number of them).
    // Specifically, the `ns: {$in: [...]}` version can lead to PSYNC_S1345 timeouts in
    // some cases when we have a large number of collections.
    // For details, see:
    // https://github.com/powersync-ja/powersync-service/pull/417
    // https://jira.mongodb.org/browse/SERVER-114532
    const nsFilter = multipleDatabases
      ? // cluster-level: filter on the entire namespace
        { ns: { $in: $inFilters } }
      : // collection-level: filter on coll only
        { 'ns.coll': { $in: $inFilters.map((ns) => ns.coll) } };
    if ($refilters.length > 0) {
      return { $match: { $or: [nsFilter, ...$refilters] }, multipleDatabases };
    }
    return { $match: nsFilter, multipleDatabases };
  }

  private async getRelations(
    batch: storage.BucketStorageBatch,
    descriptor: SourceEntityDescriptor,
    options: { snapshot: boolean }
  ): Promise<SourceTable[]> {
    const existing = this.relationCache.getAll(descriptor);
    if (existing != null) {
      // We do this even when it's an empty result: Empty means nothing to sync, and we don't need to re-resolve.
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
      this.logger.info(`Enabled postImages on ${db}.${collectionInfo.name}`);
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
    const result = await batch.resolveTables({
      connection_id: this.connection_id,
      source: descriptor
    });
    this.relationCache.updateAll(descriptor, result.tables);

    // Drop conflicting collections.
    // This is generally not expected for MongoDB source dbs, so we log an error.
    if (result.dropTables.length > 0) {
      this.logger.error(
        `Conflicting collections found for ${JSON.stringify(descriptor)}. Dropping: ${result.dropTables.map((t) => t.id).join(', ')}`
      );
      await batch.drop(result.dropTables);
    }

    // Snapshot if:
    // 1. Snapshot is requested (false for initial snapshot, since that process handles it elsewhere)
    // 2. Snapshot is not already done, AND:
    // 3. The table is used in sync config.
    const snapshotCandidates = result.tables.filter((table) => snapshot && !table.snapshotComplete && table.syncAny);
    if (snapshotCandidates.length > 0) {
      this.logger.info(`New collection: ${descriptor.schema}.${descriptor.name}`);
      if (this.supportsConcurrentSnapshots) {
        for (const tableToSnapshot of snapshotCandidates) {
          await this.snapshotter.queueSnapshot(batch, tableToSnapshot);
        }
      } else {
        // Truncate in case a previous inline snapshot was interrupted after flushing rows, but before
        // recording snapshot progress. Without this, resuming can replay already-flushed rows on v1/v2 storage.
        await batch.truncate(snapshotCandidates);
        await this.snapshotter.snapshotTables(batch, snapshotCandidates);
      }
    }

    return result.tables;
  }

  async writeChange(
    batch: storage.BucketStorageBatch,
    table: storage.SourceTable,
    change: ProjectedChangeStreamDocument
  ): Promise<storage.FlushedResult | null> {
    if (!table.syncAny) {
      this.logger.debug(`Collection ${table.qualifiedName} not used in sync config - skipping`);
      return null;
    }

    this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
    if (change.operationType == 'insert') {
      const { row: baseRecord, replicaId: _replicaId } = this.rawToSqliteRow(change.fullDocument);
      return await batch.save({
        tag: SaveOperationTag.INSERT,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: baseRecord,
        // Same as _replicaId
        // We specifically need to use the source _id, not the converted one in baseRecord,
        // to preserve _id uniqueness properties.
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
      const { row: after, replicaId: _replicaId } = this.rawToSqliteRow(change.fullDocument!);
      return await batch.save({
        tag: SaveOperationTag.UPDATE,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: after,
        afterReplicaId: change.documentKey._id // Same as _replicaId
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
    let streamPromise: Promise<void> | null = null;
    let loopPromise: Promise<void> | null = null;
    try {
      // If anything errors here, the entire replication process is halted, and
      // all connections automatically closed, including this one.
      this.initPromise = this.initReplication();
      await this.initPromise;
      loopPromise = this.snapshotter
        .replicationLoop()
        .then(() => {
          throw new ReplicationAssertionError(`Replication snapshotter exited unexpectedly`);
        })
        .catch((e) => {
          this.abortController.abort(e);
          throw e;
        });
      if (!this.supportsConcurrentSnapshots) {
        await Promise.race([this.snapshotter.waitForInitialSnapshot(), loopPromise]);
      }
      streamPromise = this.streamChanges()
        .then(() => {
          throw new ReplicationAssertionError(`Replication stream exited unexpectedly`);
        })
        .catch((e) => {
          this.abortController.abort(e);
          throw e;
        });

      const results = await Promise.allSettled([loopPromise, streamPromise]);
      for (const result of results) {
        if (result.status == 'rejected' && !(result.reason instanceof ReplicationAbortedError)) {
          throw result.reason;
        }
      }
      for (const result of results) {
        if (result.status == 'rejected') {
          throw result.reason;
        }
      }
      throw new ReplicationAssertionError(`Replication loop exited unexpectedly`);
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    } finally {
      this.abortController.abort();
    }
  }

  public async waitForInitialSnapshot() {
    if (this.initPromise == null) {
      throw new ReplicationAssertionError('replicate() must be called before waitForInitialSnapshot()');
    }
    await this.initPromise;
    await this.snapshotter.waitForInitialSnapshot();
  }

  private async initReplication() {
    const result = await this.snapshotter.checkSlot();
    await this.snapshotter.setupCheckpointsCollection();
    if (result.needsInitialSync) {
      if (result.snapshotLsn == null) {
        // Snapshot LSN is not present, so we need to start replication from scratch.
        await this.storage.clear({ signal: this.abortSignal });
      }
      await this.snapshotter.queueSnapshotTables(result.snapshotLsn);
    }
  }

  private async streamChanges() {
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

    const filters = options.filters;

    let fullDocument: 'required' | 'updateLookup';

    if (this.usePostImages) {
      // 'read_only' or 'auto_configure'
      // Configuration happens during snapshot, or when we see new
      // collections.
      fullDocument = 'required';
    } else {
      fullDocument = 'updateLookup';
    }
    const streamOptions: mongo.ChangeStreamOptions & mongo.Document = {
      showExpandedEvents: true,
      fullDocument: fullDocument
    };
    const pipeline: mongo.Document[] = [
      {
        $changeStream: streamOptions
      },
      {
        $match: filters.$match
      },
      { $changeStreamSplitLargeEvent: {} }
    ];

    /**
     * Only one of these options can be supplied at a time.
     */
    if (resumeAfter) {
      streamOptions.resumeAfter = resumeAfter;
    } else {
      // Legacy: We don't persist lsns without resumeTokens anymore, but we do still handle the
      // case if we have an old one.
      // This is also relevant for getSnapshotLSN().
      streamOptions.startAtOperationTime = startAfter;
    }

    let watchDb: mongo.Db;
    if (filters.multipleDatabases) {
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

  private rawToSqliteRow(row: Buffer) {
    return this.sourceRowConverter.rawToSqliteRow(row);
  }

  async streamChangesInternal() {
    const transactionsReplicatedMetric = this.metrics.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED);
    const bytesReplicatedMetric = this.metrics.getCounter(ReplicationMetric.DATA_REPLICATED_BYTES);
    const chunksReplicatedMetric = this.metrics.getCounter(ReplicationMetric.CHUNKS_REPLICATED);

    const tracer = new PerformanceTracer('MongoDB streaming replication');
    await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: MongoLSN.ZERO.comparable,
        defaultSchema: this.defaultDb.databaseName,
        // We get a complete postimage for every change, so we don't need to store the current data.
        storeCurrentData: false,
        hooks: this.storageHooks,
        tracer
      },
      async (batch) => {
        const { resumeFromLsn } = batch;
        if (resumeFromLsn == null) {
          throw new ReplicationAssertionError(`No LSN found to resume from`);
        }
        const lastLsn = MongoLSN.fromSerialized(resumeFromLsn);
        const startAfter = lastLsn?.timestamp;
        let outerSpan = tracer.span('batch');

        // It is normal for this to be a minute or two old when there is a low volume
        // of ChangeStream events.
        const tokenAgeSeconds = Math.round((Date.now() - timestampToDate(startAfter).getTime()) / 1000);

        this.logger.info(`Resume streaming at ${startAfter?.inspect()} / ${lastLsn}  | Token age: ${tokenAgeSeconds}s`);

        const filters = this.getSourceNamespaceFilters();
        // This is closed when the for loop below returns/breaks/throws
        const batchStream = this.rawChangeStreamBatches({
          lsn: resumeFromLsn,
          filters,
          signal: this.abortSignal,
          tracer
        });

        // Always start with a checkpoint.
        // This helps us to clear errors when restarting, even if there is
        // no data to replicate.
        let waitForCheckpointLsn: string | null = await createCheckpoint(
          this.client,
          this.defaultDb,
          this.checkpointStreamId
        );

        let splitDocument: ProjectedChangeStreamDocument | null = null;

        let flexDbNameWorkaroundLogged = false;

        let lastEmptyResume = performance.now();
        let lastTxnKey: string | null = null;

        for await (let eventBatch of batchStream) {
          const { events, resumeToken } = eventBatch;
          using batchSpan = tracer.span('processing');

          bytesReplicatedMetric.add(eventBatch.byteSize);
          chunksReplicatedMetric.add(1);
          if (this.abortSignal.aborted) {
            break;
          }
          this.touch();
          if (events.length == 0) {
            // No changes in this batch, but we still want to keep the connection alive.
            // We do this by persisting a keepalive checkpoint.
            // If we don't update it on empty events, we do keep consistency, but resuming the stream
            // with old tokens may cause connection timeouts.
            if (waitForCheckpointLsn == null && performance.now() - lastEmptyResume > 60_000) {
              const { comparable: lsn, timestamp } = MongoLSN.fromResumeToken(resumeToken);
              await batch.keepalive(lsn);
              this.touch();
              lastEmptyResume = performance.now();
              // Log the token update. This helps as a general "replication is still active" message in the logs.
              // This token would typically be around 10s behind.
              this.logger.info(
                `Idle change stream. Persisted resumeToken for ${timestampToDate(timestamp).toISOString()}`
              );
              this.replicationLag.markStarted();
            }

            // If we have no changes, we can just persist the keepalive.
            // This is throttled to once per minute.
            if (performance.now() - lastEmptyResume < 60_000) {
              continue;
            }
          }

          this.touch();

          for (let eventIndex = 0; eventIndex < events.length; eventIndex++) {
            const rawChangeDocument = events[eventIndex];
            const originalChangeDocument = parseChangeDocument(rawChangeDocument);
            if (this.abortSignal.aborted) {
              break;
            }

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
                this.logger.warn(
                  `Incorrect DB name in change stream: ${changeDocument.ns.db}. Changed to ${this.defaultDb.databaseName}.`
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
               * This prevents the `startAfter` or `resumeToken` from advancing past the drop events.
               * The stream also closes after the drop events.
               * This causes an infinite loop of processing the collection drop events.
               *
               * This check here invalidates the change stream if our `_powersync_checkpoints` collection
               * is dropped. This allows for detecting when the DB is dropped.
               */
              if (changeDocument.operationType == 'drop') {
                throw new ChangeStreamInvalidatedError(
                  'Internal collections have been dropped',
                  new Error('_powersync_checkpoints collection was dropped')
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

              if (checkpointId == STANDALONE_CHECKPOINT_ID) {
                // Standalone / write checkpoint received.
                // When we are caught up, commit immediately to keep write checkpoint latency low.
                // Once there is already a batch checkpoint pending, or the driver has buffered more
                // change stream events, collapse standalone checkpoints into the normal batch
                // checkpoint flow to avoid commit churn under sustained load.
                const hasBufferedChanges = eventIndex < events.length - 1;
                if (waitForCheckpointLsn != null || hasBufferedChanges) {
                  if (waitForCheckpointLsn == null) {
                    waitForCheckpointLsn = await createCheckpoint(this.client, this.defaultDb, this.checkpointStreamId);
                  }
                  continue;
                }
              } else if (!this.checkpointStreamId.equals(checkpointId)) {
                continue;
              }
              const { comparable: lsn } = new MongoLSN({
                timestamp: changeDocument.clusterTime!,
                resume_token: changeDocument._id
              });
              if (batch.lastCheckpointLsn != null && lsn < batch.lastCheckpointLsn) {
                // Checkpoint out of order - should never happen with MongoDB.
                // If it does happen, we throw an error to stop the replication - restarting should recover.
                // Since we use batch.lastCheckpointLsn for the next resumeAfter, this should not result in an infinite loop.
                // Originally a workaround for https://jira.mongodb.org/browse/NODE-7042.
                // This has been fixed in the driver in the meantime, but we still keep this as a safety-check.
                throw new ReplicationAssertionError(
                  `Change resumeToken ${(changeDocument._id as any)._data} (${timestampToDate(changeDocument.clusterTime!).toISOString()}) is less than last checkpoint LSN ${batch.lastCheckpointLsn}. Restarting replication.`
                );
              }

              if (waitForCheckpointLsn != null && lsn >= waitForCheckpointLsn) {
                waitForCheckpointLsn = null;
              }
              const { checkpointBlocked } = await batch.commit(lsn, {
                oldestUncommittedChange: this.replicationLag.oldestUncommittedChange
              });

              if (!checkpointBlocked) {
                this.replicationLag.markCommitted();
              }
            } else if (
              changeDocument.operationType == 'insert' ||
              changeDocument.operationType == 'update' ||
              changeDocument.operationType == 'replace' ||
              changeDocument.operationType == 'delete'
            ) {
              if (waitForCheckpointLsn == null) {
                waitForCheckpointLsn = await createCheckpoint(this.client, this.defaultDb, this.checkpointStreamId);
              }

              const rel = getMongoRelation(changeDocument.ns, this.connections.connectionTag);
              const tables = await this.getRelations(batch, rel, {
                // In most cases, we should not need to snapshot this. But if this is the first time we see the collection
                // for whatever reason, then we do need to snapshot it.
                // This may result in some duplicate operations when a collection is created for the first time after
                // sync config was deployed.
                snapshot: true
              });
              const tablesToReplicate = tables.filter((table) => table.syncAny);
              if (tablesToReplicate.length > 0) {
                this.replicationLag.trackUncommittedChange(
                  changeDocument.clusterTime == null ? null : timestampToDate(changeDocument.clusterTime)
                );

                const transactionKeyValue = transactionKey(changeDocument);

                if (transactionKeyValue == null || lastTxnKey != transactionKeyValue) {
                  // Very crude metric for counting transactions replicated.
                  // We ignore operations other than basic CRUD, and ignore changes to _powersync_checkpoints.
                  // Individual writes may not have a txnNumber, in which case we count them as separate transactions.
                  lastTxnKey = transactionKeyValue;
                  transactionsReplicatedMetric.add(1);
                }

                for (const table of tablesToReplicate) {
                  await this.writeChange(batch, table, changeDocument);
                }
              }
            } else if (changeDocument.operationType == 'drop') {
              const rel = getMongoRelation(changeDocument.ns, this.connections.connectionTag);
              const tables = await this.getRelations(batch, rel, {
                // We're "dropping" this collection, so never snapshot it.
                snapshot: false
              });
              const tablesToDrop = tables.filter((table) => table.syncAny);
              if (tablesToDrop.length > 0) {
                await batch.drop(tablesToDrop);
              }
              this.relationCache.delete(rel);
            } else if (changeDocument.operationType == 'rename') {
              const relFrom = getMongoRelation(changeDocument.ns, this.connections.connectionTag);
              const relTo = getMongoRelation(changeDocument.to, this.connections.connectionTag);
              const tablesFrom = await this.getRelations(batch, relFrom, {
                // We're "dropping" this collection, so never snapshot it.
                snapshot: false
              });
              const tablesToDrop = tablesFrom.filter((table) => table.syncAny);
              if (tablesToDrop.length > 0) {
                await batch.drop(tablesToDrop);
              }
              this.relationCache.delete(relFrom);
              // Here we do need to snapshot the new table
              const collection = await this.getCollectionInfo(relTo.schema, relTo.name);
              await this.handleRelation(batch, relTo, {
                // This is a new (renamed) collection, so always snapshot it.
                snapshot: true,
                collectionInfo: collection
              });
            }
          }

          if (splitDocument == null) {
            // We flush and mark progress on every batch of data we receive.
            // Batches are generally large (64MB or 6000 events, whichever comes first),
            // so this is a good natural point to flush and mark progress.
            // We avoid this when splitDocument is set, since we cannot resume in the middle of a split event.
            const { comparable: lsn } = MongoLSN.fromResumeToken(resumeToken);
            await batch.flush({ oldestUncommittedChange: this.replicationLag.oldestUncommittedChange });
            // TODO: We should consider making this standard behavior of flush().
            await batch.setResumeLsn(lsn);
          }

          batchSpan.end();
          const durations = outerSpan.end();
          const duration = batchSpan.endAt - batchSpan.startAt;

          this.logger.info(
            `Processed batch of ${events.length} changes / ${eventBatch.byteSize} bytes in ${duration}ms`,
            {
              count: events.length,
              bytes: eventBatch.byteSize,
              duration,
              t: durations
            }
          );
          outerSpan = tracer.span('batch');
        }
      }
    );

    throw new ReplicationAbortedError(`Replication stream aborted`, this.abortSignal.reason);
  }

  getReplicationLagMillis(): number | undefined {
    return this.replicationLag.getLagMillis();
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

/**
 * Transaction key for a change stream event, used to detect transaction boundaries. Returns null if the event is not part of a transaction.
 */
function transactionKey(doc: Pick<mongo.ChangeStreamDocument, 'lsid' | 'txnNumber'>): string | null {
  if (doc.txnNumber == null || doc.lsid == null) {
    return null;
  }
  return `${doc.lsid.id.toString('hex')}:${doc.txnNumber}`;
}
