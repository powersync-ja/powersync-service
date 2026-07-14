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
import { and, asc, desc, eq, isNull, lte } from 'drizzle-orm';
import * as uuid from 'uuid';
import type { BucketDataRow } from '../drivers/sqlite/schema.js';
import { DrizzleBucketBatch } from './DrizzleBucketBatch.js';
import { DrizzleBucketStorageFactory } from './DrizzleBucketStorageFactory.js';
import { DrizzleCompactor } from './DrizzleCompactor.js';
import { DrizzleStorageDialect } from './DrizzleStorageDialect.js';

export interface DrizzleSyncRulesStorageOptions {
  factory: DrizzleBucketStorageFactory;
  dialect: DrizzleStorageDialect;
  replicationStream: storage.PersistedReplicationStream;
}

export class DrizzleSyncRulesStorage
  extends BaseObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  [DO_NOT_LOG] = true;

  readonly replicationStreamId: number;
  readonly replicationStreamName: string;
  readonly storageConfig: storage.StorageVersionConfig;
  readonly factory: DrizzleBucketStorageFactory;
  readonly logger: Logger;

  private parsedSyncRulesCache:
    | { parsed: sync_rules.HydratedSyncConfig; options: storage.ParseSyncConfigOptions }
    | undefined;

  private writeCheckpointModeValue = storage.WriteCheckpointMode.MANAGED;

  constructor(private readonly options: DrizzleSyncRulesStorageOptions) {
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

  async createManagedWriteCheckpoint(checkpoint: storage.ManagedWriteCheckpointOptions): Promise<bigint> {
    if (this.writeCheckpointMode !== storage.WriteCheckpointMode.MANAGED) {
      throw new errors.ValidationError(
        `Attempting to create a managed Write Checkpoint when the current Write Checkpoint mode is set to "${this.writeCheckpointMode}"`
      );
    }

    const table = this.options.dialect.tables.writeCheckpoints;
    const createdCheckpoint = this.options.dialect.transaction((tx) => {
      const latest = tx
        .select()
        .from(table)
        .where(and(eq(table.userId, checkpoint.user_id), isNull(table.syncRulesId)))
        .orderBy(desc(table.checkpoint))
        .limit(1)
        .get();
      const value = (latest?.checkpoint ?? 0n) + 1n;
      tx.insert(table)
        .values({
          id: uuid.v4(),
          syncRulesId: null,
          userId: checkpoint.user_id,
          checkpoint: value,
          heads: checkpoint.heads,
          createdAt: new Date()
        })
        .run();
      return value;
    });

    this.factory.checkpointWatcher.notify();
    return createdCheckpoint;
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
    const { db, tables } = this.options.dialect;
    const syncRules = db.select().from(tables.syncRules).where(eq(tables.syncRules.id, this.replicationStreamId)).get();

    const checkpointLsn = syncRules?.lastCheckpointLsn ?? null;
    const writer = new DrizzleBucketBatch({
      factory: this.factory,
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

  getParsedSyncRules(options: storage.ParseSyncConfigOptions): sync_rules.HydratedSyncConfig {
    const { parsed, options: cachedOptions } = this.parsedSyncRulesCache ?? {};
    if (!parsed || options.defaultSchema != cachedOptions?.defaultSchema) {
      this.parsedSyncRulesCache = {
        parsed: this.options.replicationStream.parsed(options).hydratedSyncConfig(),
        options
      };
    }

    return this.parsedSyncRulesCache!.parsed;
  }

  async terminate(options?: storage.TerminateOptions): Promise<void> {
    if (!options || options.clearStorage) {
      await this.clear(options);
    }

    const { db, tables } = this.options.dialect;
    db.update(tables.syncRules)
      .set({
        state: storage.SyncRuleState.TERMINATED,
        snapshotDone: false
      })
      .where(eq(tables.syncRules.id, this.replicationStreamId))
      .run();
    this.factory.checkpointWatcher.notify();
  }

  async getStatus(): Promise<storage.SyncRuleStatus> {
    const { db, tables } = this.options.dialect;
    const row = db.select().from(tables.syncRules).where(eq(tables.syncRules.id, this.replicationStreamId)).get();

    return {
      checkpoint_lsn: row?.lastCheckpointLsn ?? null,
      active: row?.state == storage.SyncRuleState.ACTIVE,
      snapshot_done: row?.snapshotDone ?? false,
      snapshot_lsn: row?.snapshotLsn ?? null,
      keepalive_op: row?.keepaliveOp ?? null
    };
  }

  async clear(_options?: storage.ClearStorageOptions): Promise<void> {
    const tables = this.options.dialect.tables;
    this.options.dialect.transaction((tx) => {
      tx.update(tables.syncRules)
        .set({
          snapshotDone: false,
          lastCheckpointLsn: null,
          lastCheckpoint: null,
          noCheckpointBefore: null
        })
        .where(eq(tables.syncRules.id, this.replicationStreamId))
        .run();
      tx.delete(tables.bucketData).where(eq(tables.bucketData.groupId, this.replicationStreamId)).run();
      tx.delete(tables.bucketParameters).where(eq(tables.bucketParameters.groupId, this.replicationStreamId)).run();
      tx.delete(tables.currentData).where(eq(tables.currentData.groupId, this.replicationStreamId)).run();
      tx.delete(tables.sourceTables).where(eq(tables.sourceTables.groupId, this.replicationStreamId)).run();
    });

    this.factory.checkpointWatcher.notify();
  }

  async reportError(e: any): Promise<void> {
    const { db, tables } = this.options.dialect;
    db.update(tables.syncRules)
      .set({
        lastFatalError: String(e.message ?? 'Replication failure'),
        lastFatalErrorTs: new Date()
      })
      .where(eq(tables.syncRules.id, this.replicationStreamId))
      .run();
  }

  async compact(options?: storage.CompactOptions): Promise<void> {
    let maxOpId = options?.maxOpId;
    if (maxOpId == null) {
      const checkpoint = await this.getCheckpoint();
      maxOpId = checkpoint.checkpoint;
    }

    const compactor = new DrizzleCompactor(this.options.dialect, this.replicationStreamId, {
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
    const { db, tables } = this.options.dialect;
    const row = db.select().from(tables.syncRules).where(eq(tables.syncRules.id, this.replicationStreamId)).get();

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
    checkpoint: bigint,
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
      groupId: this.replicationStreamId,
      checkpoint,
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

  async getChecksums(checkpoint: bigint, buckets: BucketChecksumRequest[]): Promise<utils.ChecksumMap> {
    const result: utils.ChecksumMap = new Map();
    const { db, tables } = this.options.dialect;
    for (const bucket of buckets) {
      const rows = db
        .select()
        .from(tables.bucketData)
        .where(
          and(
            eq(tables.bucketData.groupId, this.replicationStreamId),
            eq(tables.bucketData.bucketName, bucket.bucket),
            lte(tables.bucketData.opId, checkpoint)
          )
        )
        .orderBy(asc(tables.bucketData.opId))
        .all();
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
    // No checksum cache exists in the initial Drizzle storage slice.
  }

  private async lastCustomWriteCheckpoint(filters: storage.CustomWriteCheckpointFilters): Promise<bigint | null> {
    const { db, tables } = this.options.dialect;
    const row = db
      .select()
      .from(tables.writeCheckpoints)
      .where(
        and(
          eq(tables.writeCheckpoints.userId, filters.user_id),
          filters.sync_rules_id == null
            ? isNull(tables.writeCheckpoints.syncRulesId)
            : eq(tables.writeCheckpoints.syncRulesId, filters.sync_rules_id)
        )
      )
      .orderBy(desc(tables.writeCheckpoints.checkpoint))
      .limit(1)
      .get();
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
      const { db, tables } = this.options.dialect;
      const rows = db
        .select()
        .from(tables.bucketParameters)
        .where(
          and(
            eq(tables.bucketParameters.groupId, this.replicationStreamId),
            eq(tables.bucketParameters.lookup, serializedLookup),
            lte(tables.bucketParameters.id, checkpoint)
          )
        )
        .orderBy(desc(tables.bucketParameters.id))
        .all();

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

    const { db, tables } = this.options.dialect;
    const rows = db
      .select()
      .from(tables.writeCheckpoints)
      .where(and(eq(tables.writeCheckpoints.userId, filters.user_id), isNull(tables.writeCheckpoints.syncRulesId)))
      .orderBy(desc(tables.writeCheckpoints.checkpoint))
      .all();

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

function bucketDataRowToOpEntry(row: BucketDataRow): utils.OplogEntry {
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
