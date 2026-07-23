import { MikroORM } from '@mikro-orm/core';
import { BaseObserver, DO_NOT_LOG, errors, Logger } from '@powersync/lib-services-framework';
import {
  BucketChecksumRequest,
  BucketDataBatchOptions,
  BucketDataRequest,
  CHECKPOINT_INVALIDATE_ALL,
  CheckpointChanges,
  GetCheckpointChangesOptions,
  PopulateChecksumCacheOptions,
  PopulateChecksumCacheResults,
  ReplicationCheckpoint,
  storage,
  StorageCheckpointUpdate,
  SyncBucketDataChunk,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import * as sync_rules from '@powersync/service-sync-rules';
import * as uuid from 'uuid';
import type { BucketData } from '../entities/entities-index.js';
import { MikroOrmBucketBatch } from './MikroOrmBucketBatch.js';
import { MikroOrmBucketStorageFactory } from './MikroOrmBucketStorageFactory.js';
import { MikroOrmCompactor } from './MikroOrmCompactor.js';
import { MikroOrmStorageDialect } from './MikroOrmStorageDialect.js';

export interface MikroOrmSyncRulesStorageOptions {
  factory: MikroOrmBucketStorageFactory;
  orm: MikroORM;
  dialect: MikroOrmStorageDialect;
  replicationStream: storage.PersistedReplicationStream;
}

export class MikroOrmSyncRulesStorage
  extends BaseObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  [DO_NOT_LOG] = true;

  readonly replicationStreamId: number;
  readonly replicationStreamName: string;
  readonly storageConfig: storage.StorageVersionConfig;
  readonly factory: MikroOrmBucketStorageFactory;
  readonly logger: Logger;

  private readonly parsedSyncConfigSets = new Map<string, storage.ParsedSyncConfigSet>();

  private writeCheckpointModeValue = storage.WriteCheckpointMode.MANAGED;

  constructor(private readonly options: MikroOrmSyncRulesStorageOptions) {
    super();
    this.replicationStreamId = options.replicationStream.replicationStreamId;
    this.replicationStreamName = options.replicationStream.replicationStreamName;
    this.storageConfig = options.replicationStream.getStorageConfig();
    this.factory = options.factory;
    this.logger = options.replicationStream.logger;
  }

  get writeCheckpointMode(): storage.WriteCheckpointMode {
    return this.writeCheckpointModeValue;
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this.writeCheckpointModeValue = mode;
  }

  async createManagedWriteCheckpoints(
    checkpoints: storage.ManagedWriteCheckpointOptions[]
  ): Promise<Map<string, bigint>> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.MANAGED) {
      throw new errors.ValidationError(
        `Attempting to create a managed Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const uniqueCheckpoints = [...new Map(checkpoints.map((checkpoint) => [checkpoint.user_id, checkpoint])).values()];
    if (uniqueCheckpoints.length == 0) {
      return new Map();
    }
    const createdCheckpoints = new Map<string, bigint>();
    const em = this.options.orm.em.fork();
    await em.transactional(async (transactionalEntityManager) => {
      for (const checkpoint of uniqueCheckpoints) {
        const [latest] = await transactionalEntityManager.find(
          this.options.dialect.writeCheckpointEntity,
          {
            userId: checkpoint.user_id,
            syncRulesId: null
          },
          {
            orderBy: { checkpoint: 'DESC' },
            limit: 1
          }
        );
        const value = (latest?.checkpoint ?? 0n) + 1n;
        const row = transactionalEntityManager.create(this.options.dialect.writeCheckpointEntity, {
          id: uuid.v4(),
          syncRulesId: null,
          userId: checkpoint.user_id,
          checkpoint: value,
          heads: checkpoint.heads,
          createdAt: new Date()
        });
        transactionalEntityManager.persist(row);
        createdCheckpoints.set(checkpoint.user_id, value);
      }
      await transactionalEntityManager.flush();
    });

    this.factory.checkpointWatcher.notify();
    return createdCheckpoints;
  }

  async lastWriteCheckpoint(filters: storage.SyncStorageLastWriteCheckpointFilters): Promise<bigint | null> {
    switch (this.writeCheckpointMode) {
      case storage.WriteCheckpointMode.CUSTOM:
        return this.lastCustomWriteCheckpoint({
          user_id: filters.user_id,
          sync_rules_id: this.replicationStreamId
        });
      case storage.WriteCheckpointMode.MANAGED:
        if (!('heads' in filters)) {
          throw new errors.ValidationError(`Replication HEAD is required for managed Write Checkpoint filtering`);
        }
        return this.lastManagedWriteCheckpoint(filters);
    }
  }

  async createWriter(options: storage.CreateWriterOptions): Promise<storage.BucketStorageBatch> {
    const em = this.options.orm.em.fork();
    const syncRules = await em.findOne(this.options.dialect.syncRulesEntity, {
      id: this.replicationStreamId
    });

    const checkpointLsn = syncRules?.lastCheckpointLsn ?? null;
    const writer = new MikroOrmBucketBatch({
      factory: this.factory,
      orm: this.options.orm,
      dialect: this.options.dialect,
      logger: options.logger ?? this.logger,
      syncRules: this.getParsedSyncRules(options),
      replicationStreamId: this.replicationStreamId,
      replicationStreamName: this.replicationStreamName,
      lastCheckpointLsn: checkpointLsn,
      keepaliveOp: syncRules?.keepaliveOp ?? null,
      resumeFromLsn: utils.maxLsn(syncRules?.snapshotLsn, checkpointLsn),
      storeCurrentData: options.storeCurrentData,
      skipExistingRows: options.skipExistingRows ?? false,
      markRecordUnavailable: options.markRecordUnavailable,
      hooks: options.hooks
    });
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

  getParsedSyncConfigSet(options: storage.ParseSyncConfigOptions): storage.ParsedSyncConfigSet {
    let parsed = this.parsedSyncConfigSets.get(options.defaultSchema);
    if (parsed == null) {
      parsed = this.options.replicationStream.parsed(options);
      this.parsedSyncConfigSets.set(options.defaultSchema, parsed);
    }
    return parsed;
  }

  getParsedSyncRules(options: storage.ParseSyncConfigOptions): sync_rules.HydratedSyncConfig {
    return this.getParsedSyncConfigSet(options).hydratedSyncConfig;
  }

  async terminate(options?: storage.TerminateOptions): Promise<void> {
    if (!options || options.clearStorage) {
      await this.clear(options);
    }

    const em = this.options.orm.em.fork();
    const row = await em.findOne(this.options.dialect.syncRulesEntity, { id: this.replicationStreamId });
    if (row != null) {
      em.assign(row, {
        state: storage.SyncRuleState.TERMINATED,
        snapshotDone: false
      });
      await em.flush();
    }
    this.factory.checkpointWatcher.notify();
  }

  async getStatus(): Promise<storage.ReplicationStreamStatus> {
    const em = this.options.orm.em.fork();
    const row = await em.findOne(this.options.dialect.syncRulesEntity, {
      id: this.replicationStreamId
    });

    if (row == null) {
      throw new Error('Cannot find replication stream status');
    }
    return {
      snapshotDone: row.snapshotDone && row.lastCheckpointLsn != null,
      resumeLsn: utils.maxLsn(row.snapshotLsn, row.lastCheckpointLsn)
    };
  }

  async clear(_options?: storage.ClearStorageOptions): Promise<void> {
    const em = this.options.orm.em.fork();
    await em.transactional(async (transactionalEntityManager) => {
      const row = await transactionalEntityManager.findOne(this.options.dialect.syncRulesEntity, {
        id: this.replicationStreamId
      });
      if (row != null) {
        transactionalEntityManager.assign(row, {
          snapshotDone: false,
          lastCheckpointLsn: null,
          lastCheckpoint: null,
          noCheckpointBefore: null
        });
      }

      await transactionalEntityManager.nativeDelete(this.options.dialect.bucketDataEntity, {
        groupId: this.replicationStreamId
      });
      await transactionalEntityManager.nativeDelete(this.options.dialect.bucketParametersEntity, {
        groupId: this.replicationStreamId
      });
      await transactionalEntityManager.nativeDelete(this.options.dialect.currentDataEntity, {
        groupId: this.replicationStreamId
      });
      await transactionalEntityManager.nativeDelete(this.options.dialect.sourceTableEntity, {
        groupId: this.replicationStreamId
      });
    });

    this.factory.checkpointWatcher.notify();
  }

  async reportError(e: any): Promise<void> {
    const em = this.options.orm.em.fork();
    const row = await em.findOne(this.options.dialect.syncRulesEntity, { id: this.replicationStreamId });
    if (row != null) {
      em.assign(row, {
        lastFatalError: String(e.message ?? 'Replication failure'),
        lastFatalErrorTs: new Date()
      });
      await em.flush();
    }
  }

  async compact(options?: storage.CompactOptions): Promise<void> {
    let maxOpId = options?.maxOpId;
    if (maxOpId == null) {
      const checkpoint = await this.getCheckpoint();
      maxOpId = checkpoint.checkpoint;
    }

    const compactor = new MikroOrmCompactor(this.options.orm, this.options.dialect, this.replicationStreamId, {
      ...options,
      maxOpId,
      logger: this.logger
    });
    await compactor.compact();

    if (options?.compactParameterData) {
      await compactor.compactParameterData(options);
    }
  }

  async populatePersistentChecksumCache(_options: PopulateChecksumCacheOptions): Promise<PopulateChecksumCacheResults> {
    return { buckets: 0 };
  }

  async getCheckpoint(): Promise<ReplicationCheckpoint> {
    const em = this.options.orm.em.fork();
    const row = await em.findOne(this.options.dialect.syncRulesEntity, {
      id: this.replicationStreamId
    });

    return {
      checkpoint: row?.lastCheckpoint ?? 0n,
      lsn: row?.lastCheckpointLsn ?? null,
      getParameterSets: (lookups, limit) => this.getParameterSets(row?.lastCheckpoint ?? 0n, lookups, limit)
    };
  }

  async getCheckpointChanges(_options: GetCheckpointChangesOptions): Promise<CheckpointChanges> {
    return CHECKPOINT_INVALIDATE_ALL;
  }

  async *watchCheckpointChanges(options: WatchWriteCheckpointOptions): AsyncIterable<StorageCheckpointUpdate> {
    let lastCheckpoint: bigint | null = null;
    let lastCheckpointLsn: string | null = null;
    let lastWriteCheckpoint: bigint | null = null;
    const { signal, user_id } = options;

    const watcher = this.factory.checkpointWatcher.watch(signal)[Symbol.asyncIterator]();
    let nextNotification: Promise<IteratorResult<void>> | null = null;
    let readImmediately = true;

    try {
      while (!signal.aborted) {
        if (!readImmediately) {
          nextNotification ??= watcher.next();
          const result = await nextNotification;
          nextNotification = null;
          if (result.done) {
            return;
          }
        }
        readImmediately = false;

        if (signal.aborted) {
          return;
        }

        const base = await this.getCheckpoint();
        const currentWriteCheckpoint = await this.lastWriteCheckpoint({
          user_id,
          heads: base.lsn == null ? {} : { '1': base.lsn }
        });

        if (
          currentWriteCheckpoint == lastWriteCheckpoint &&
          base.checkpoint == lastCheckpoint &&
          base.lsn == lastCheckpointLsn
        ) {
          continue;
        }

        lastWriteCheckpoint = currentWriteCheckpoint;
        lastCheckpoint = base.checkpoint;
        lastCheckpointLsn = base.lsn;
        nextNotification = watcher.next();

        yield {
          base,
          writeCheckpoint: currentWriteCheckpoint,
          update: CHECKPOINT_INVALIDATE_ALL
        };
      }
    } finally {
      await watcher.return?.();
    }
  }

  async *getBucketDataBatch(
    checkpoint: ReplicationCheckpoint,
    dataBuckets: BucketDataRequest[],
    options?: BucketDataBatchOptions
  ): AsyncIterable<SyncBucketDataChunk> {
    if (dataBuckets.length == 0) {
      return;
    }

    const batchRowLimit = options?.limit ?? storage.DEFAULT_DOCUMENT_BATCH_LIMIT;
    const chunkSizeLimitBytes = options?.chunkLimitBytes ?? storage.DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES;
    const startOpByBucket = new Map(dataBuckets.map((request) => [request.bucket, request.start]));
    const rows = this.options.dialect.streamBucketDataRows({
      em: this.options.orm.em.fork(),
      groupId: this.replicationStreamId,
      checkpoint: checkpoint.checkpoint,
      dataBuckets,
      limit: batchRowLimit
    });

    let chunkSizeBytes = 0;
    let currentChunk: utils.SyncBucketData | null = null;
    let targetOp: bigint | null = null;
    let batchRowCount = 0;

    for await (const row of rows) {
      const rowSizeBytes = row.data?.length ?? 0;
      const sizeExceeded =
        chunkSizeBytes >= chunkSizeLimitBytes ||
        ((currentChunk?.data.length ?? 0) > 0 && chunkSizeBytes + rowSizeBytes > chunkSizeLimitBytes) ||
        (currentChunk?.data.length ?? 0) >= batchRowLimit;

      if (currentChunk == null || currentChunk.bucket != row.bucketName || sizeExceeded) {
        let start: string | undefined = undefined;
        if (currentChunk != null) {
          if (currentChunk.bucket == row.bucketName) {
            currentChunk.has_more = true;
            start = currentChunk.next_after;
          }

          const yieldChunk = currentChunk;
          currentChunk = null;
          chunkSizeBytes = 0;
          yield { chunkData: yieldChunk, targetOp };
          targetOp = null;
          if (batchRowCount >= batchRowLimit) {
            break;
          }
        }

        if (start == null) {
          const startOpId = startOpByBucket.get(row.bucketName);
          if (startOpId == null) {
            throw new Error(`data for unexpected bucket: ${row.bucketName}`);
          }
          start = utils.internalToExternalOpId(startOpId);
        }
        currentChunk = {
          bucket: row.bucketName,
          after: start,
          has_more: false,
          data: [],
          next_after: start
        };
      }

      const entry = bucketDataRowToOpEntry(row);
      currentChunk.data.push(entry);
      currentChunk.next_after = entry.op_id;
      if (row.targetOp != null && (targetOp == null || row.targetOp > targetOp)) {
        targetOp = row.targetOp;
      }

      chunkSizeBytes += rowSizeBytes;
      batchRowCount++;
    }

    if (currentChunk != null) {
      currentChunk.has_more = batchRowCount >= batchRowLimit;
      yield { chunkData: currentChunk, targetOp };
    }
  }

  async getChecksums(checkpoint: ReplicationCheckpoint, buckets: BucketChecksumRequest[]): Promise<utils.ChecksumMap> {
    const result: utils.ChecksumMap = new Map();
    for (const bucket of buckets) {
      const rows = await this.options.orm.em.fork().find(
        this.options.dialect.bucketDataEntity,
        {
          groupId: this.replicationStreamId,
          bucketName: bucket.bucket,
          opId: { $lte: checkpoint.checkpoint }
        },
        {
          orderBy: { opId: 'ASC' }
        }
      );
      const entries = rows.map(bucketDataRowToOpEntry);
      const checksum = entries.reduce((total, entry) => utils.addChecksums(total, Number(entry.checksum)), 0);
      result.set(bucket.bucket, {
        bucket: bucket.bucket,
        checksum,
        count: entries.length
      });
    }
    return result;
  }

  clearChecksumCache(): void {
    // No checksum cache exists in the initial MikroORM storage slice.
  }

  private async lastCustomWriteCheckpoint(filters: storage.CustomWriteCheckpointFilters): Promise<bigint | null> {
    const row = await this.options.orm.em.fork().findOne(
      this.options.dialect.writeCheckpointEntity,
      {
        userId: filters.user_id,
        syncRulesId: filters.sync_rules_id
      },
      {
        orderBy: { checkpoint: 'DESC' }
      }
    );
    return row?.checkpoint ?? null;
  }

  private async getParameterSets(
    checkpoint: bigint,
    lookups: sync_rules.ScopedParameterLookup[],
    limit: number
  ): Promise<sync_rules.ParameterLookupRows[]> {
    const resultsByLookup = new Map<sync_rules.ScopedParameterLookup, sync_rules.SqliteJsonRow[]>();
    let totalRows = 0;

    for (const lookup of lookups) {
      const serializedLookup = storage.serializeLookupBuffer(lookup);
      const rows = await this.options.orm.em.fork().find(
        this.options.dialect.bucketParametersEntity,
        {
          groupId: this.replicationStreamId,
          lookup: serializedLookup,
          id: { $lte: checkpoint }
        },
        {
          orderBy: { id: 'DESC' }
        }
      );

      const latestBySource = new Map<string, (typeof rows)[number]>();
      for (const row of rows) {
        const key = `${row.sourceTable}:${Buffer.from(row.sourceKey).toString('hex')}`;
        if (!latestBySource.has(key)) {
          latestBySource.set(key, row);
        }
      }

      for (const row of latestBySource.values()) {
        const parameterRows = parseBucketParameters(row.bucketParameters);
        if (parameterRows.length == 0) {
          continue;
        }
        totalRows += parameterRows.length;
        if (totalRows > limit) {
          throw new storage.ParameterSetLimitExceededError(limit);
        }
        const existing = resultsByLookup.get(lookup);
        if (existing != null) {
          existing.push(...parameterRows);
        } else {
          resultsByLookup.set(lookup, parameterRows);
        }
      }
    }

    const results: sync_rules.ParameterLookupRows[] = [];
    resultsByLookup.forEach((rows, lookup) => results.push({ lookup, rows }));
    return results;
  }

  private async lastManagedWriteCheckpoint(filters: storage.ManagedWriteCheckpointFilters): Promise<bigint | null> {
    const lsn = filters.heads['1'];
    if (lsn == null) {
      return null;
    }

    const rows = await this.options.orm.em.fork().find(
      this.options.dialect.writeCheckpointEntity,
      {
        userId: filters.user_id,
        syncRulesId: null
      },
      {
        orderBy: { checkpoint: 'DESC' }
      }
    );

    return (
      rows.find((row) => {
        const rowHead = getPrimaryReplicationHead(row.heads);
        return rowHead != null && rowHead <= lsn;
      })?.checkpoint ?? null
    );
  }
}

function getPrimaryReplicationHead(heads: unknown): string | null {
  if (heads == null || typeof heads != 'object' || Array.isArray(heads)) {
    return null;
  }

  const head = (heads as Record<string, unknown>)['1'];
  return typeof head == 'string' ? head : null;
}

function parseBucketParameters(value: unknown): sync_rules.SqliteJsonRow[] {
  if (typeof value == 'string') {
    return JSONBig.parse(value) as sync_rules.SqliteJsonRow[];
  }
  return Array.isArray(value) ? (value as sync_rules.SqliteJsonRow[]) : [];
}

function bucketDataRowToOpEntry(row: BucketData): utils.OplogEntry {
  if (row.op == 'PUT' || row.op == 'REMOVE') {
    return {
      op_id: utils.internalToExternalOpId(row.opId),
      op: row.op,
      object_type: row.tableName ?? undefined,
      object_id: row.rowId ?? undefined,
      checksum: Number(row.checksum),
      subkey:
        row.sourceTable != null && row.sourceKey != null
          ? replicaIdToSubkey(row.sourceTable, storage.deserializeReplicaId(Buffer.from(row.sourceKey)))
          : undefined,
      data: row.op == 'REMOVE' ? null : (row.data ?? undefined)
    };
  }

  return {
    op_id: utils.internalToExternalOpId(row.opId),
    op: row.op as 'CLEAR' | 'MOVE',
    checksum: Number(row.checksum)
  };
}

function replicaIdToSubkey(tableId: storage.SourceTableId, id: storage.ReplicaId): string {
  if (storage.isUUID(id)) {
    return `${tableId}/${id.toHexString()}`;
  }
  return uuid.v5(storage.serializeBson({ table: tableId, id }), utils.ID_NAMESPACE);
}
