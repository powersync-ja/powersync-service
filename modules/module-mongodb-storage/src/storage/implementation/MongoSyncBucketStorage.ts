import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import {
  BaseObserver,
  DO_NOT_LOG,
  Logger,
  ReplicationAbortedError,
  ServiceAssertionError
} from '@powersync/lib-services-framework';
import {
  BroadcastIterable,
  CHECKPOINT_INVALIDATE_ALL,
  CheckpointChanges,
  GetCheckpointChangesOptions,
  InternalOpId,
  mergeAsyncIterables,
  PopulateChecksumCacheOptions,
  PopulateChecksumCacheResults,
  ReplicationCheckpoint,
  storage,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import { HydratedSyncConfig, ParameterLookupRows, ScopedParameterLookup } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { LRUCache } from 'lru-cache';
import * as timers from 'timers/promises';
import { retryOnMongoMaxTimeMSExpired } from '../../utils/util.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { MongoSyncBucketStorageContext } from './common/MongoSyncBucketStorageContext.js';
import type { VersionedPowerSyncMongo } from './db.js';
import { StorageConfig } from './models.js';
import { MongoBucketBatchOptions } from './MongoBucketBatch.js';
import { MongoChecksumOptions, MongoChecksums } from './MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from './MongoCompactor.js';
import { MongoParameterCompactor } from './MongoParameterCompactor.js';
import { MongoPersistedSyncRulesContentV1 } from './MongoPersistedSyncRulesContent.js';
import { MongoWriteCheckpointAPI } from './MongoWriteCheckpointAPI.js';

export interface MongoSyncBucketStorageOptions {
  checksumOptions?: Omit<MongoChecksumOptions, 'storageConfig' | 'mapping'>;
  storageConfig: StorageConfig;
}

interface InternalCheckpointChanges extends CheckpointChanges {
  updatedWriteCheckpoints: Map<string, bigint>;
  invalidateWriteCheckpoints: boolean;
}

interface WriterSyncState {
  lastCheckpointLsn: string | null;
  resumeFromLsn: string | null;
  keepaliveOp: InternalOpId | null;
  syncConfigId?: bson.ObjectId | null;
}

/**
 * Only keep checkpoints around for a minute, before fetching a fresh one.
 *
 * The reason is that we keep a MongoDB snapshot reference (clusterTime) with the checkpoint,
 * and they expire after 5 minutes by default. This is an issue if the checkpoint stream is idle,
 * but new clients connect and use an outdated checkpoint snapshot for parameter queries.
 *
 * These will be filtered out for existing clients, so should not create significant overhead.
 */
const CHECKPOINT_TIMEOUT_MS = 60_000;

export abstract class MongoSyncBucketStorage
  extends BaseObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  readonly db: VersionedPowerSyncMongo;
  [DO_NOT_LOG] = true;

  readonly checksums: MongoChecksums;

  private parsedSyncRulesCache: { parsed: HydratedSyncConfig; options: storage.ParseSyncRulesOptions } | undefined;
  private writeCheckpointAPI: MongoWriteCheckpointAPI;
  public readonly logger: Logger;
  public readonly storageConfig: StorageConfig;
  #storageInitialized = false;

  constructor(
    public readonly factory: MongoBucketStorage,
    public readonly group_id: number,
    protected readonly sync_rules: MongoPersistedSyncRulesContentV1,
    public readonly slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    super();
    this.storageConfig = options.storageConfig;
    this.db = factory.db.versioned(this.storageConfig);
    this.checksums = this.createMongoChecksums(options);
    this.writeCheckpointAPI = new MongoWriteCheckpointAPI({
      db: this.db,
      mode: writeCheckpointMode ?? storage.WriteCheckpointMode.MANAGED,
      sync_rules_id: group_id
    });
    this.logger = sync_rules.logger;
  }

  /**
   * Not for external use - public here for tests only.
   *
   * @internal
   */
  abstract createMongoCompactor(options: MongoCompactOptions): MongoCompactor;

  protected abstract createMongoChecksums(options: MongoSyncBucketStorageOptions): MongoChecksums;
  protected abstract createMongoParameterCompactor(
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ): MongoParameterCompactor;

  get writeCheckpointMode() {
    return this.writeCheckpointAPI.writeCheckpointMode;
  }

  get mapping() {
    return this.sync_rules.mapping;
  }

  protected get versionContext(): MongoSyncBucketStorageContext {
    return {
      db: this.db,
      group_id: this.group_id,
      mapping: this.mapping
    };
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this.writeCheckpointAPI.setWriteCheckpointMode(mode);
  }

  createManagedWriteCheckpoint(checkpoint: storage.ManagedWriteCheckpointOptions): Promise<bigint> {
    return this.writeCheckpointAPI.createManagedWriteCheckpoint(checkpoint);
  }

  lastWriteCheckpoint(filters: storage.SyncStorageLastWriteCheckpointFilters): Promise<bigint | null> {
    return this.writeCheckpointAPI.lastWriteCheckpoint({
      ...filters,
      sync_rules_id: this.group_id
    });
  }

  getParsedSyncRules(options: storage.ParseSyncRulesOptions): HydratedSyncConfig {
    const { parsed, options: cachedOptions } = this.parsedSyncRulesCache ?? {};
    if (!parsed || options.defaultSchema != cachedOptions?.defaultSchema) {
      this.parsedSyncRulesCache = { parsed: this.sync_rules.parsed(options).hydratedSyncConfig(), options };
    }

    return this.parsedSyncRulesCache!.parsed;
  }

  async getCheckpoint(): Promise<storage.ReplicationCheckpoint> {
    return (await this.getCheckpointInternal()) ?? new EmptyReplicationCheckpoint();
  }

  protected abstract fetchCheckpointState(
    session: mongo.ClientSession
  ): Promise<{ checkpoint: bigint; lsn: string | null } | null>;

  async getCheckpointInternal(): Promise<storage.ReplicationCheckpoint | null> {
    return await this.db.client.withSession({ snapshot: true }, async (session) => {
      const state = await this.fetchCheckpointState(session);
      if (state == null) {
        return null;
      }

      const snapshotTime = (session as any).snapshotTime as bson.Timestamp | undefined;
      if (snapshotTime == null) {
        throw new ServiceAssertionError('Missing snapshotTime in getCheckpoint()');
      }
      return new MongoReplicationCheckpoint(this, state.checkpoint, state.lsn, snapshotTime);
    });
  }

  protected abstract initializeVersionStorage(): Promise<void>;

  private async initializeStorage() {
    if (this.#storageInitialized) {
      return;
    }

    await this.db.initializeStreamStorage(this.group_id);
    await this.initializeVersionStorage();
    this.#storageInitialized = true;
  }

  protected abstract createWriterImpl(batchOptions: MongoBucketBatchOptions): storage.BucketStorageBatch;
  protected abstract getWriterSyncState(): Promise<WriterSyncState>;

  async createWriter(options: storage.CreateWriterOptions): Promise<storage.BucketStorageBatch> {
    await this.initializeStorage();

    const state = await this.getWriterSyncState();

    const batchOptions: MongoBucketBatchOptions = {
      logger: options.logger ?? this.logger,
      db: this.db,
      syncRules: this.sync_rules.parsed(options).hydratedSyncConfig(),
      mapping: this.sync_rules.mapping,
      groupId: this.group_id,
      slotName: this.slot_name,
      lastCheckpointLsn: state.lastCheckpointLsn,
      resumeFromLsn: state.resumeFromLsn,
      keepaliveOp: state.keepaliveOp,
      storeCurrentData: options.storeCurrentData,
      skipExistingRows: options.skipExistingRows ?? false,
      markRecordUnavailable: options.markRecordUnavailable,
      hooks: options.hooks,
      syncConfigId: state.syncConfigId,
      tracer: options.tracer
    };
    const writer = this.createWriterImpl(batchOptions);
    this.iterateListeners((cb) => cb.batchStarted?.(writer));
    return writer;
  }

  async startBatch(
    options: storage.CreateWriterOptions,
    callback: (batch: storage.BucketStorageBatch) => Promise<void>
  ): Promise<storage.FlushedResult | null> {
    await using writer = await this.createWriter(options);
    await callback(writer);
    await writer.flush();
    return writer.last_flushed_op != null ? { flushed_op: writer.last_flushed_op } : null;
  }

  protected abstract getParameterSetsImpl(
    checkpoint: MongoReplicationCheckpoint,
    lookups: ScopedParameterLookup[],
    limit: number
  ): Promise<ParameterLookupRows[]>;

  async getParameterSets(
    checkpoint: MongoReplicationCheckpoint,
    lookups: ScopedParameterLookup[],
    limit: number
  ): Promise<ParameterLookupRows[]> {
    return this.getParameterSetsImpl(checkpoint, lookups, limit);
  }

  protected abstract getBucketDataBatchImpl(
    checkpoint: utils.InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk>;

  async *getBucketDataBatch(
    checkpoint: utils.InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    yield* this.getBucketDataBatchImpl(checkpoint, dataBuckets, options);
  }

  async getChecksums(
    checkpoint: utils.InternalOpId,
    buckets: storage.BucketChecksumRequest[]
  ): Promise<utils.ChecksumMap> {
    return this.checksums.getChecksums(checkpoint, buckets);
  }

  clearChecksumCache() {
    this.checksums.clearCache();
  }

  protected abstract terminateSyncRuleState(): Promise<void>;

  async terminate(options?: storage.TerminateOptions) {
    if (!options || options?.clearStorage) {
      await this.clear(options);
    }
    await this.terminateSyncRuleState();
    await this.db.notifyCheckpoint();
  }

  protected abstract getStatusImpl(): Promise<storage.SyncRuleStatus>;

  async getStatus(): Promise<storage.SyncRuleStatus> {
    return this.getStatusImpl();
  }

  protected abstract clearBucketData(signal?: AbortSignal): Promise<void>;

  protected abstract clearParameterIndexes(signal?: AbortSignal): Promise<void>;

  protected abstract clearSourceRecords(signal?: AbortSignal): Promise<void>;

  protected abstract clearBucketState(signal?: AbortSignal): Promise<void>;

  protected abstract clearSourceTables(signal?: AbortSignal): Promise<void>;
  protected abstract clearSyncRuleState(): Promise<void>;

  async clear(options?: storage.ClearStorageOptions): Promise<void> {
    const signal = options?.signal;

    if (signal?.aborted) {
      throw new ReplicationAbortedError('Aborted clearing data', signal.reason);
    }

    await this.clearSyncRuleState();

    await this.clearBucketData(signal);
    await this.clearParameterIndexes(signal);
    await this.clearSourceRecords(signal);
    await this.clearBucketState(signal);
    await this.clearSourceTables(signal);

    this.#storageInitialized = false;
  }

  protected async clearDeleteMany(
    label: string,
    operation: () => Promise<mongo.DeleteResult>,
    signal?: AbortSignal
  ): Promise<void> {
    await retryOnMongoMaxTimeMSExpired(operation, {
      signal,
      abortMessage: 'Aborted clearing data',
      retryDelayMs: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS / 5,
      onRetry: () => {
        this.logger.info(
          `Cleared batch of ${label} in ${lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS}ms, continuing...`
        );
      }
    });
  }

  async reportError(e: any): Promise<void> {
    const message = String(e.message ?? 'Replication failure');
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id
      },
      {
        $set: {
          last_fatal_error: message,
          last_fatal_error_ts: new Date()
        }
      }
    );
  }

  async compact(options?: storage.CompactOptions) {
    let maxOpId = options?.maxOpId;
    if (maxOpId == null) {
      const checkpoint = await this.getCheckpointInternal();
      maxOpId = checkpoint?.checkpoint ?? undefined;
    }
    await this.createMongoCompactor({ ...options, maxOpId, logger: this.logger }).compact();

    if (maxOpId != null && options?.compactParameterData) {
      await this.createMongoParameterCompactor(maxOpId, options).compact();
    }
  }

  async populatePersistentChecksumCache(options: PopulateChecksumCacheOptions): Promise<PopulateChecksumCacheResults> {
    this.logger.info(`Populating persistent checksum cache...`);
    const start = Date.now();
    const compactor = this.createMongoCompactor({
      ...options,
      memoryLimitMB: 0,
      logger: this.logger
    });

    const result = await compactor.populateChecksums({
      minBucketChanges: options.minBucketChanges ?? 10
    });
    const duration = Date.now() - start;
    this.logger.info(`Populated persistent checksum cache in ${(duration / 1000).toFixed(1)}s`);
    return result;
  }

  private async *watchActiveCheckpoint(signal: AbortSignal): AsyncIterable<ReplicationCheckpoint> {
    if (signal.aborted) {
      return;
    }

    const stream = mergeAsyncIterables(
      [this.checkpointChangesStream(signal), this.checkpointTimeoutStream(signal)],
      signal
    );

    for await (const _ of stream) {
      if (signal.aborted) {
        break;
      }

      const op = await this.getCheckpointInternal();
      if (op == null) {
        break;
      }

      yield op;
    }
  }

  private readonly sharedIter = new BroadcastIterable((signal) => {
    return this.watchActiveCheckpoint(signal);
  });

  async *watchCheckpointChanges(options: WatchWriteCheckpointOptions): AsyncIterable<storage.StorageCheckpointUpdate> {
    let lastCheckpoint: ReplicationCheckpoint | null = null;

    const iter = this.sharedIter[Symbol.asyncIterator](options.signal);

    let writeCheckpoint: bigint | null = null;
    let queriedInitialWriteCheckpoint = false;

    for await (const nextCheckpoint of iter) {
      if (nextCheckpoint.lsn != null && !queriedInitialWriteCheckpoint) {
        writeCheckpoint = await this.writeCheckpointAPI.lastWriteCheckpoint({
          sync_rules_id: this.group_id,
          user_id: options.user_id,
          heads: {
            '1': nextCheckpoint.lsn
          }
        });
        queriedInitialWriteCheckpoint = true;
      }

      if (
        lastCheckpoint != null &&
        lastCheckpoint.checkpoint == nextCheckpoint.checkpoint &&
        lastCheckpoint.lsn == nextCheckpoint.lsn
      ) {
        await timers.setTimeout(20 + 10 * Math.random());
        continue;
      }

      if (lastCheckpoint == null) {
        yield {
          base: nextCheckpoint,
          writeCheckpoint,
          update: CHECKPOINT_INVALIDATE_ALL
        };
      } else {
        const updates = await this.getCheckpointChanges({
          lastCheckpoint,
          nextCheckpoint
        });

        let updatedWriteCheckpoint = updates.updatedWriteCheckpoints.get(options.user_id) ?? null;
        if (updates.invalidateWriteCheckpoints) {
          updatedWriteCheckpoint = await this.writeCheckpointAPI.lastWriteCheckpoint({
            sync_rules_id: this.group_id,
            user_id: options.user_id,
            heads: {
              '1': nextCheckpoint.lsn!
            }
          });
        }
        if (updatedWriteCheckpoint != null && (writeCheckpoint == null || updatedWriteCheckpoint > writeCheckpoint)) {
          writeCheckpoint = updatedWriteCheckpoint;
          queriedInitialWriteCheckpoint = true;
        }

        yield {
          base: nextCheckpoint,
          writeCheckpoint,
          update: {
            updatedDataBuckets: updates.updatedDataBuckets,
            invalidateDataBuckets: updates.invalidateDataBuckets,
            updatedParameterLookups: updates.updatedParameterLookups,
            invalidateParameterBuckets: updates.invalidateParameterBuckets
          }
        };
      }

      lastCheckpoint = nextCheckpoint;
    }
  }

  private async *checkpointChangesStream(signal: AbortSignal): AsyncGenerator<void> {
    if (signal.aborted) {
      return;
    }

    const query = () => {
      return this.db.checkpoint_events.find(
        {},
        { tailable: true, awaitData: true, maxAwaitTimeMS: 10_000, batchSize: 1000 }
      );
    };

    let cursor = query();

    signal.addEventListener('abort', () => {
      cursor.close().catch(() => {});
    });

    yield;

    try {
      while (!signal.aborted) {
        const doc = await cursor.tryNext().catch((e) => {
          if (lib_mongo.isMongoServerError(e) && e.codeName === 'CappedPositionLost') {
            cursor = query();
            return {};
          } else {
            return Promise.reject(e);
          }
        });
        if (cursor.closed) {
          return;
        }
        cursor.readBufferedDocuments();
        if (doc != null) {
          yield;
        }
      }
    } catch (e) {
      if (signal.aborted) {
        return;
      }
      throw e;
    } finally {
      await cursor.close();
    }
  }

  private async *checkpointTimeoutStream(signal: AbortSignal): AsyncGenerator<void> {
    while (!signal.aborted) {
      try {
        await timers.setTimeout(CHECKPOINT_TIMEOUT_MS, undefined, { signal });
      } catch (e) {
        if (e.name == 'AbortError') {
          return;
        }
        throw e;
      }

      if (!signal.aborted) {
        yield;
      }
    }
  }

  protected abstract getDataBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>>;

  private async getDataBucketChanges(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
    return this.getDataBucketChangesImpl(options);
  }

  protected abstract getParameterBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>>;

  private async getParameterBucketChanges(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    return this.getParameterBucketChangesImpl(options);
  }

  private checkpointChangesCache = new LRUCache<
    string,
    InternalCheckpointChanges,
    { options: GetCheckpointChangesOptions }
  >({
    max: 50,
    maxSize: 12 * 1024 * 1024,
    sizeCalculation: (value: InternalCheckpointChanges) => {
      const paramSize = [...value.updatedParameterLookups].reduce<number>((a, b) => a + b.length, 0);
      const bucketSize = [...value.updatedDataBuckets].reduce<number>((a, b) => a + b.length, 0);
      const writeCheckpointSize = value.updatedWriteCheckpoints.size * 30;
      return 100 + paramSize + bucketSize + writeCheckpointSize;
    },
    fetchMethod: async (_key, _staleValue, options) => {
      return this.getCheckpointChangesInternal(options.context.options);
    }
  });

  async getCheckpointChanges(options: GetCheckpointChangesOptions): Promise<InternalCheckpointChanges> {
    const key = `${options.lastCheckpoint.checkpoint}_${options.lastCheckpoint.lsn}__${options.nextCheckpoint.checkpoint}_${options.nextCheckpoint.lsn}`;
    const result = await this.checkpointChangesCache.fetch(key, { context: { options } });
    return result!;
  }

  private async getCheckpointChangesInternal(options: GetCheckpointChangesOptions): Promise<InternalCheckpointChanges> {
    const dataUpdates = await this.getDataBucketChanges(options);
    const parameterUpdates = await this.getParameterBucketChanges(options);
    const writeCheckpointUpdates = await this.writeCheckpointAPI.getWriteCheckpointChanges(options);

    return {
      ...dataUpdates,
      ...parameterUpdates,
      ...writeCheckpointUpdates
    };
  }
}

class MongoReplicationCheckpoint implements ReplicationCheckpoint {
  #storage: MongoSyncBucketStorage;

  constructor(
    storage: MongoSyncBucketStorage,
    public readonly checkpoint: InternalOpId,
    public readonly lsn: string | null,
    public snapshotTime: mongo.Timestamp
  ) {
    this.#storage = storage;
  }

  async getParameterSets(lookups: ScopedParameterLookup[], limit: number): Promise<ParameterLookupRows[]> {
    return this.#storage.getParameterSets(this, lookups, limit);
  }
}

class EmptyReplicationCheckpoint implements ReplicationCheckpoint {
  readonly checkpoint: InternalOpId = 0n;
  readonly lsn: string | null = null;

  async getParameterSets(_lookups: ScopedParameterLookup[]): Promise<ParameterLookupRows[]> {
    return [];
  }
}
