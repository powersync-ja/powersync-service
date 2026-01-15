import { mongo } from '@powersync/lib-service-mongodb';
import {
  container,
  logger as defaultLogger,
  ErrorCode,
  Logger,
  ReplicationAbortedError,
  ReplicationAssertionError,
  ServiceError
} from '@powersync/lib-services-framework';
import {
  BucketDataWriter,
  BucketStorageFactory,
  MetricsEngine,
  SaveOperationTag,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import { HydratedSyncRules, SqliteRow } from '@powersync/service-sync-rules';
import { ReplicationMetric } from '@powersync/service-types';
import { MongoLSN, ZERO_LSN } from '../common/MongoLSN.js';
import { PostImagesOption } from '../types/types.js';
import { escapeRegExp } from '../utils.js';
import { ChangeStreamInvalidatedError, mapChangeStreamError } from './ChangeStreamErrors.js';
import { ReplicationStreamConfig } from './ChangeStreamReplicationJob.js';
import { MongoManager } from './MongoManager.js';
import {
  constructAfterRecord,
  createCheckpoint,
  getCacheIdentifier,
  getMongoRelation,
  STANDALONE_CHECKPOINT_ID
} from './MongoRelation.js';
import { MongoSnapshotter } from './MongoSnapshotter.js';
import { CHECKPOINTS_COLLECTION, timestampToDate } from './replication-utils.js';

export interface ChangeStreamOptions {
  connections: MongoManager;
  factory: BucketStorageFactory;
  streams: Pick<ReplicationStreamConfig, 'storage'>[];
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

  logger?: Logger;
}

interface SubStreamOptions {
  connections: MongoManager;
  storage: storage.SyncRulesBucketStorage;
  logger: Logger;
  abortSignal: AbortSignal;
  checkpointStreamId: mongo.ObjectId;
  maxAwaitTimeMS: number;
}

interface InitResult {
  needsInitialSync: boolean;
  snapshotLsn: string | null;
}

class SubStream {
  private readonly connections: MongoManager;
  public readonly storage: storage.SyncRulesBucketStorage;
  public readonly syncRules: HydratedSyncRules;
  private readonly logger: Logger;

  constructor(options: SubStreamOptions) {
    this.connections = options.connections;
    this.storage = options.storage;
    this.logger = options.logger;
    this.syncRules = this.storage.getHydratedSyncRules({
      defaultSchema: this.connections.db.databaseName
    });
  }

  async checkSlot(): Promise<InitResult> {
    const status = await this.storage.getStatus();
    if (status.snapshot_done && status.checkpoint_lsn) {
      this.logger.info(`Initial replication already done`);
      return { needsInitialSync: false, snapshotLsn: null };
    }

    return { needsInitialSync: true, snapshotLsn: status.snapshot_lsn };
  }
}

export class ChangeStream {
  substreams: SubStream[] = [];

  connection_id = 1;

  private connections: MongoManager;
  private readonly client: mongo.MongoClient;
  private readonly defaultDb: mongo.Db;
  private readonly metrics: MetricsEngine;
  private readonly factory: BucketStorageFactory;

  private readonly maxAwaitTimeMS: number;

  private abortController = new AbortController();
  private abortSignal: AbortSignal = this.abortController.signal;

  private initPromise: Promise<void> | null = null;

  /**
   * Time of the oldest uncommitted change, according to the source db.
   * This is used to determine the replication lag.
   */
  private oldestUncommittedChange: Date | null = null;
  /**
   * Keep track of whether we have done a commit or keepalive yet.
   * We can only compute replication lag if isStartingReplication == false, or oldestUncommittedChange is present.
   */
  private isStartingReplication = true;

  private checkpointStreamId = new mongo.ObjectId();

  private logger: Logger;

  private changeStreamTimeout: number;

  public readonly relationCache = new Map<string | number, SourceTable[]>();

  private readonly snapshotter: MongoSnapshotter;

  private readonly snapshotChunkLength: number | undefined;

  constructor(options: ChangeStreamOptions) {
    this.metrics = options.metrics;
    this.connections = options.connections;
    this.maxAwaitTimeMS = options.maxAwaitTimeMS ?? 10_000;
    this.client = this.connections.client;
    this.defaultDb = this.connections.db;
    this.factory = options.factory;
    // The change stream aggregation command should timeout before the socket times out,
    // so we use 90% of the socket timeout value.
    this.changeStreamTimeout = Math.ceil(this.client.options.socketTimeoutMS * 0.9);

    this.logger = options.logger ?? defaultLogger;
    this.snapshotChunkLength = options.snapshotChunkLength;

    this.substreams = options.streams.map((config) => {
      return new SubStream({
        abortSignal: this.abortSignal,
        checkpointStreamId: this.checkpointStreamId,
        connections: this.connections,
        storage: config.storage,
        logger: this.logger.child({ prefix: `[powersync_${config.storage.group_id}] ` }),
        maxAwaitTimeMS: this.maxAwaitTimeMS
      });
    });

    const snapshotLogger = this.logger.child({ prefix: `[powersync_snapshot] ` });

    const snapshotter = new MongoSnapshotter({
      writer: async () => {
        const writer = await this.factory.createCombinedWriter(
          this.substreams.map((s) => s.storage),
          {
            defaultSchema: this.defaultDb.databaseName,
            storeCurrentData: false,
            zeroLSN: ZERO_LSN,
            logger: snapshotLogger
          }
        );
        return writer;
      },
      abort_signal: this.abortSignal,
      checkpointStreamId: this.checkpointStreamId,
      connections: this.connections,
      logger: snapshotLogger,
      snapshotChunkLength: this.snapshotChunkLength,
      metrics: this.metrics,
      maxAwaitTimeMS: this.maxAwaitTimeMS
    });
    this.snapshotter = snapshotter;

    // We wrap in our own abort controller so we can trigger abort internally.
    options.abort_signal.addEventListener('abort', () => {
      this.abortController.abort(options.abort_signal.reason);
    });
    if (options.abort_signal.aborted) {
      this.abortController.abort(options.abort_signal.reason);
    }
  }

  private get usePostImages() {
    return this.connections.options.postImages != PostImagesOption.OFF;
  }

  private get configurePostImages() {
    return this.connections.options.postImages == PostImagesOption.AUTO_CONFIGURE;
  }

  get stopped() {
    return this.abortSignal.aborted;
  }

  private getSourceNamespaceFilters(writer: BucketDataWriter): {
    $match: any;
    multipleDatabases: boolean;
  } {
    const sourceTables = writer.rowProcessor.getSourceTables();

    let $inFilters: { db: string; coll: string }[] = [
      { db: this.defaultDb.databaseName, coll: CHECKPOINTS_COLLECTION }
    ];
    let $refilters: { 'ns.db': string; 'ns.coll': RegExp }[] = [];
    let filters: any[] = [];
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
        filters.push({
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

    // FIXME: deduplicate filters

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

  private async initReplication() {
    await this.setupCheckpointsCollection();
    for (let stream of this.substreams) {
      const result = await stream.checkSlot();

      if (result.needsInitialSync) {
        if (result.snapshotLsn == null) {
          // Snapshot LSN is not present, so we need to start replication from scratch.
          await stream.storage.clear({ signal: this.abortSignal });
        }
        await this.snapshotter.queueSnapshotTables(result.snapshotLsn);
      }
    }
  }

  async replicate() {
    let streamPromise: Promise<void> | null = null;
    let loopPromise: Promise<void> | null = null;
    try {
      // If anything errors here, the entire replication process is halted, and
      // all connections automatically closed, including this one.
      this.initPromise = this.initReplication();
      // Important - need to wait for init. This sets the resumeLsn, amongst other setup
      await this.initPromise;
      streamPromise = this.streamChanges()
        .then(() => {
          throw new ReplicationAssertionError(`Replication stream exited unexpectedly`);
        })
        .catch(async (e) => {
          // Report stream errors to all substreams
          for (let substream of this.substreams) {
            await substream.storage.reportError(e);
          }

          this.abortController.abort(e);
          throw e;
        });
      loopPromise = this.snapshotter
        .replicationLoop()
        .then(() => {
          throw new ReplicationAssertionError(`Replication snapshotter exited unexpectedly`);
        })
        .catch(async (e) => {
          // Report stream errors to all substreams for now - we can't yet distinguish the errors
          for (let substream of this.substreams) {
            await substream.storage.reportError(e);
          }

          this.abortController.abort(e);
          throw e;
        });
      const results = await Promise.allSettled([loopPromise, streamPromise]);
      // First, prioritize non-aborted errors
      for (let result of results) {
        if (result.status == 'rejected' && !(result.reason instanceof ReplicationAbortedError)) {
          throw result.reason;
        }
      }
      // Then include aborted errors
      for (let result of results) {
        if (result.status == 'rejected') {
          throw result.reason;
        }
      }

      // If we get here, both Promises completed successfully, which is unexpected.
      throw new ReplicationAssertionError(`Replication loop exited unexpectedly`);
    } finally {
      // Just to make sure
      this.abortController.abort();
    }
  }

  /**
   * For tests: Wait until the initial snapshot is complete.
   */
  public async waitForInitialSnapshot() {
    if (this.initPromise == null) {
      throw new ReplicationAssertionError('replicate() must be called before waitForInitialSnapshot()');
    }
    await this.initPromise;
    await this.snapshotter?.waitForInitialSnapshot();
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

  private openChangeStream(writer: BucketDataWriter, options: { lsn: string | null; maxAwaitTimeMs?: number }) {
    const lastLsn = options.lsn ? MongoLSN.fromSerialized(options.lsn) : null;
    const startAfter = lastLsn?.timestamp;
    const resumeAfter = lastLsn?.resumeToken;

    const filters = this.getSourceNamespaceFilters(writer);

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

  async handleRelations(
    writer: storage.BucketDataWriter,
    descriptor: SourceEntityDescriptor,
    options: { snapshot: boolean; collectionInfo: mongo.CollectionInfo | undefined }
  ): Promise<SourceTable[]> {
    if (options.collectionInfo != null) {
      await this.checkPostImages(descriptor.schema, options.collectionInfo);
    } else {
      // If collectionInfo is null, the collection may have been dropped.
      // Ignore the postImages check in this case.
    }

    const result = await writer.resolveTables({
      connection_id: this.connection_id,
      connection_tag: this.connections.connectionTag,
      entity_descriptor: descriptor
    });

    const snapshot = options.snapshot;
    this.relationCache.set(getCacheIdentifier(descriptor), result.tables);

    // Drop conflicting collections.
    // This is generally not expected for MongoDB source dbs, so we log an error.
    if (result.dropTables.length > 0) {
      this.logger.error(
        `Conflicting collections found for ${JSON.stringify(descriptor)}. Dropping: ${result.dropTables.map((t) => t.id).join(', ')}`
      );
      await writer.drop(result.dropTables);
    }

    // Snapshot if:
    // 1. Snapshot is requested (false for initial snapshot, since that process handles it elsewhere)
    // 2. Snapshot is not already done, AND:
    // 3. The table is used in sync rules.
    for (let table of result.tables) {
      const shouldSnapshot = snapshot && !table.snapshotComplete && table.syncAny;
      if (shouldSnapshot) {
        this.logger.info(`New collection: ${descriptor.schema}.${descriptor.name}`);
        await this.snapshotter.queueSnapshot(writer, table);
      }
    }

    return result.tables;
  }

  private async drop(writer: storage.BucketDataWriter, entity: SourceEntityDescriptor): Promise<void> {
    const tables = await this.getRelations(writer, entity, {
      // We're "dropping" this collection, so never snapshot it.
      snapshot: false
    });
    if (tables.length > 0) {
      await writer.drop(tables);
    }
    this.relationCache.delete(getCacheIdentifier(entity));
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

  async getRelations(
    writer: storage.BucketDataWriter,
    descriptor: SourceEntityDescriptor,
    options: { snapshot: boolean }
  ): Promise<SourceTable[]> {
    const existing = this.relationCache.get(getCacheIdentifier(descriptor));
    if (existing != null) {
      return existing;
    }
    const collection = await this.getCollectionInfo(descriptor.schema, descriptor.name);

    return this.handleRelations(writer, descriptor, { snapshot: options.snapshot, collectionInfo: collection });
  }

  private async streamChangesInternal() {
    await using writer = await this.factory.createCombinedWriter(
      this.substreams.map((s) => s.storage),
      {
        defaultSchema: this.defaultDb.databaseName,
        storeCurrentData: false,
        zeroLSN: ZERO_LSN,
        logger: this.logger,
        markRecordUnavailable: undefined,
        skipExistingRows: false
      }
    );

    // Even though we use a unified stream, the resumeFromLsn is tracked separately per sync rules version.
    // This resumeFromLsn on the writer gives us the _minimum_ one.
    // When starting with the first sync rules, we need to get an LSN from the snapshot.
    // When we then start a new sync rules version, it will use the LSN from the existing sync rules version.
    const resumeFromLsn = writer.resumeFromLsn;
    if (resumeFromLsn == null) {
      throw new ReplicationAssertionError(`No LSN found to resume from`);
    }
    const lastLsn = MongoLSN.fromSerialized(resumeFromLsn);
    const startAfter = lastLsn?.timestamp;

    // It is normal for this to be a minute or two old when there is a low volume
    // of ChangeStream events.
    const tokenAgeSeconds = Math.round((Date.now() - timestampToDate(startAfter).getTime()) / 1000);

    this.logger.info(`Resume streaming at ${startAfter?.inspect()} / ${lastLsn}  | Token age: ${tokenAgeSeconds}s`);

    await using streamManager = this.openChangeStream(writer, { lsn: resumeFromLsn });
    const { stream, filters } = streamManager;
    if (this.abortSignal.aborted) {
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
    let changesSinceLastCheckpoint = 0;

    let lastEmptyResume = performance.now();

    while (true) {
      if (this.abortSignal.aborted) {
        break;
      }

      const originalChangeDocument = await stream.tryNext().catch((e) => {
        throw mapChangeStreamError(e);
      });
      // The stream was closed, we will only ever receive `null` from it
      if (!originalChangeDocument && stream.closed) {
        break;
      }

      if (this.abortSignal.aborted) {
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
          const { comparable: lsn, timestamp } = MongoLSN.fromResumeToken(stream.resumeToken);
          await writer.keepaliveAll(lsn);
          this.touch();
          lastEmptyResume = performance.now();
          // Log the token update. This helps as a general "replication is still active" message in the logs.
          // This token would typically be around 10s behind.
          this.logger.info(`Idle change stream. Persisted resumeToken for ${timestampToDate(timestamp).toISOString()}`);
          this.isStartingReplication = false;
        }
        continue;
      }

      this.touch();

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
        // FIXME: Implement this check again. We can't rely on batch.lastCheckpointLsn anymore.
        // if (batch.lastCheckpointLsn != null && lsn < batch.lastCheckpointLsn) {
        //   // Checkpoint out of order - should never happen with MongoDB.
        //   // If it does happen, we throw an error to stop the replication - restarting should recover.
        //   // Since we use batch.lastCheckpointLsn for the next resumeAfter, this should not result in an infinite loop.
        //   // Originally a workaround for https://jira.mongodb.org/browse/NODE-7042.
        //   // This has been fixed in the driver in the meantime, but we still keep this as a safety-check.
        //   throw new ReplicationAssertionError(
        //     `Change resumeToken ${(changeDocument._id as any)._data} (${timestampToDate(changeDocument.clusterTime!).toISOString()}) is less than last checkpoint LSN ${batch.lastCheckpointLsn}. Restarting replication.`
        //   );
        // }

        if (waitForCheckpointLsn != null && lsn >= waitForCheckpointLsn) {
          waitForCheckpointLsn = null;
        }
        const didCommit = await writer.commitAll(lsn, { oldestUncommittedChange: this.oldestUncommittedChange });

        if (didCommit) {
          // TODO: Re-check this logic
          this.oldestUncommittedChange = null;
          this.isStartingReplication = false;
          changesSinceLastCheckpoint = 0;
        }

        continue;
      }

      if (
        changeDocument.operationType == 'insert' ||
        changeDocument.operationType == 'update' ||
        changeDocument.operationType == 'replace' ||
        changeDocument.operationType == 'delete'
      ) {
        if (waitForCheckpointLsn == null) {
          waitForCheckpointLsn = await createCheckpoint(this.client, this.defaultDb, this.checkpointStreamId);
        }
        const rel = getMongoRelation(changeDocument.ns);
        const tables = await this.getRelations(writer, rel, {
          // In most cases, we should not need to snapshot this. But if this is the first time we see the collection
          // for whatever reason, then we do need to snapshot it.
          // This may result in some duplicate operations when a collection is created for the first time after
          // sync rules was deployed.
          snapshot: true
        });
        const filtered = tables.filter((t) => t.syncAny);

        for (let table of filtered) {
          if (this.oldestUncommittedChange == null && changeDocument.clusterTime != null) {
            this.oldestUncommittedChange = timestampToDate(changeDocument.clusterTime);
          }
          const flushResult = await this.writeChange(writer, table, changeDocument);
          changesSinceLastCheckpoint += 1;
          if (flushResult != null && changesSinceLastCheckpoint >= 20_000) {
            // When we are catching up replication after an initial snapshot, there may be a very long delay
            // before we do a commit(). In that case, we need to periodically persist the resume LSN, so
            // we don't restart from scratch if we restart replication.
            // The same could apply if we need to catch up on replication after some downtime.
            const { comparable: lsn } = new MongoLSN({
              timestamp: changeDocument.clusterTime!,
              resume_token: changeDocument._id
            });
            this.logger.info(`Updating resume LSN to ${lsn} after ${changesSinceLastCheckpoint} changes`);
            await writer.setAllResumeLsn(lsn);
            changesSinceLastCheckpoint = 0;
          }
        }
      } else if (changeDocument.operationType == 'drop') {
        const rel = getMongoRelation(changeDocument.ns);
        await this.drop(writer, rel);
      } else if (changeDocument.operationType == 'rename') {
        const relFrom = getMongoRelation(changeDocument.ns);
        const relTo = getMongoRelation(changeDocument.to);
        await this.drop(writer, relFrom);

        // Here we do need to snapshot the new table
        const collection = await this.getCollectionInfo(relTo.schema, relTo.name);
        await this.handleRelations(writer, relTo, {
          // This is a new (renamed) collection, so always snapshot it.
          snapshot: true,
          collectionInfo: collection
        });
      }
    }

    throw new ReplicationAbortedError(`Replication stream aborted`, this.abortSignal.reason);
  }

  private constructAfterRecord(writer: storage.BucketDataWriter, document: mongo.Document): SqliteRow {
    const inputRow = constructAfterRecord(document);
    return writer.rowProcessor.applyRowContext<never>(inputRow);
  }

  async writeChange(
    writer: storage.BucketDataWriter,
    table: storage.SourceTable,
    change: mongo.ChangeStreamDocument
  ): Promise<storage.FlushedResult | null> {
    if (!table.syncAny) {
      this.logger.debug(`Collection ${table.qualifiedName} not used in sync rules - skipping`);
      return null;
    }

    this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
    if (change.operationType == 'insert') {
      const baseRecord = this.constructAfterRecord(writer, change.fullDocument);
      return await writer.save({
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
        return await writer.save({
          tag: SaveOperationTag.DELETE,
          sourceTable: table,
          before: undefined,
          beforeReplicaId: change.documentKey._id
        });
      }
      const after = this.constructAfterRecord(writer, change.fullDocument!);
      return await writer.save({
        tag: SaveOperationTag.UPDATE,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: undefined,
        after: after,
        afterReplicaId: change.documentKey._id
      });
    } else if (change.operationType == 'delete') {
      return await writer.save({
        tag: SaveOperationTag.DELETE,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: change.documentKey._id
      });
    } else {
      throw new ReplicationAssertionError(`Unsupported operation: ${change.operationType}`);
    }
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    if (this.oldestUncommittedChange == null) {
      if (this.isStartingReplication) {
        // We don't have anything to compute replication lag with yet.
        return undefined;
      } else {
        // We don't have any uncommitted changes, so replication is up-to-date.
        return 0;
      }
    }
    return Date.now() - this.oldestUncommittedChange.getTime();
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

export { ChangeStreamInvalidatedError };
