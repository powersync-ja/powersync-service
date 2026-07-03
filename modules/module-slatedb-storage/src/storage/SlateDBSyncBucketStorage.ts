import { BaseObserver, logger as defaultLogger, Logger } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { HydratedSyncConfig } from '@powersync/service-sync-rules';
import { v5 as uuidv5 } from 'uuid';
import { SlateDBBucketStorageFactory } from './SlateDBBucketStorageFactory.js';
import { encodeOpId, SlateDBKVStore, storageKey, storagePrefix, type SlateDBWriteOperation } from './SlateDBKVStore.js';
import {
  getReplicationStreamRecord,
  putReplicationStreamRecord,
  replicationStreamKey,
  SlateDBPersistedReplicationStream,
  SlateDBReplicationStreamRecord
} from './SlateDBPersistedSyncConfigContent.js';

interface SourceTableRecord {
  id: string;
  connection_id: number;
  schema: string;
  name: string;
  connectionTag: string;
  objectId: string | number | undefined;
  replicaIdColumns: storage.ColumnDescriptor[];
  snapshotComplete: boolean;
  snapshotStatus?: storage.TableSnapshotStatus;
}

interface CurrentDataRecord {
  data: Record<string, unknown> | null;
  buckets: CurrentBucket[];
}

interface CurrentBucket {
  bucket: string;
  table: string;
  id: string;
  subkey: string;
}

interface BucketOpRecord {
  op_id: string;
  op_id_bigint: string;
  bucket: string;
  op: 'PUT' | 'REMOVE';
  object_type?: string;
  object_id?: string;
  data?: string | null;
  checksum: number;
  subkey?: string;
}

const DELETED = Symbol('deleted');

export class SlateDBSyncBucketStorage
  extends BaseObserver<storage.SyncRulesBucketStorageListener>
  implements storage.SyncRulesBucketStorage
{
  readonly replicationStreamId: number;
  readonly replicationStreamName: string;
  readonly storageConfig: storage.StorageVersionConfig;
  readonly logger: Logger;
  writeCheckpointMode = storage.WriteCheckpointMode.MANAGED;

  private parsedSyncConfigSet: storage.ParsedSyncConfigSet | undefined;

  constructor(
    readonly factory: SlateDBBucketStorageFactory,
    readonly store: SlateDBKVStore,
    readonly replicationStream: SlateDBPersistedReplicationStream
  ) {
    super();
    this.replicationStreamId = replicationStream.replicationStreamId;
    this.replicationStreamName = replicationStream.replicationStreamName;
    this.storageConfig = replicationStream.getStorageConfig();
    this.logger = defaultLogger.child({ prefix: `[${this.replicationStreamName}] ` });
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this.writeCheckpointMode = mode;
  }

  async createManagedWriteCheckpoints(
    checkpoints: storage.ManagedWriteCheckpointOptions[]
  ): Promise<Map<string, bigint>> {
    return new Map(checkpoints.map((checkpoint, index) => [checkpoint.user_id, BigInt(index + 1)]));
  }

  async lastWriteCheckpoint(): Promise<bigint | null> {
    return null;
  }

  async createWriter(options: storage.CreateWriterOptions): Promise<storage.BucketStorageBatch> {
    const batch = new SlateDBBucketBatch(this, options, await this.getRecord());
    this.iterateListeners((listener) => listener.batchStarted?.(batch));
    return batch;
  }

  async startBatch(
    options: storage.CreateWriterOptions,
    callback: (batch: storage.BucketStorageBatch) => Promise<void>
  ): Promise<storage.FlushedResult | null> {
    await using batch = await this.createWriter(options);
    await callback(batch);
    return batch.flush();
  }

  getParsedSyncConfigSet(options: storage.ParseSyncConfigOptions): storage.ParsedSyncConfigSet {
    this.parsedSyncConfigSet ??= this.replicationStream.parsed(options);
    return this.parsedSyncConfigSet;
  }

  getParsedSyncRules(options: storage.ParseSyncConfigOptions): HydratedSyncConfig {
    return this.getParsedSyncConfigSet(options).hydratedSyncConfig;
  }

  async terminate(options?: storage.TerminateOptions): Promise<void> {
    if (options?.clearStorage) {
      await this.clear(options);
    }
    await this.updateRecord({ state: storage.SyncRuleState.TERMINATED });
  }

  async getStatus(): Promise<storage.ReplicationStreamStatus> {
    const record = await this.getRecord();
    return {
      resumeLsn: record.resume_lsn ?? record.last_checkpoint_lsn,
      snapshotDone: record.snapshot_done === true && record.last_checkpoint_lsn != null
    };
  }

  async clear(_options?: storage.ClearStorageOptions): Promise<void> {
    for (const prefix of [
      storagePrefix('source-table', this.replicationStreamId),
      storagePrefix('current-data', this.replicationStreamId),
      storagePrefix('bucket-data', this.replicationStreamId),
      storagePrefix('bucket-state', this.replicationStreamId)
    ]) {
      await this.store.deletePrefix(prefix);
    }
    await this.updateRecord({ last_persisted_op: '0', last_checkpoint_lsn: null, snapshot_done: false });
  }

  async reportError(e: any): Promise<void> {
    await this.updateRecord({
      last_fatal_error: e instanceof Error ? e.message : String(e),
      last_fatal_error_ts: new Date().toISOString()
    });
  }

  async compact(): Promise<void> {
    // No compaction for the POC.
  }

  async populatePersistentChecksumCache(): Promise<storage.PopulateChecksumCacheResults> {
    return { buckets: 0 };
  }

  async getCheckpoint(): Promise<storage.ReplicationCheckpoint> {
    const record = await this.getRecord();
    const checkpoint = BigInt(record.last_checkpoint_op ?? '0');
    return new SlateDBReplicationCheckpoint(checkpoint, record.last_checkpoint_lsn ?? null);
  }

  async getCheckpointChanges(options: storage.GetCheckpointChangesOptions): Promise<storage.CheckpointChanges> {
    const updatedDataBuckets = new Set<string>();
    for await (const entry of this.store.scanPrefix<{ last_op: string }>(
      storagePrefix('bucket-state', this.replicationStreamId)
    )) {
      if (BigInt(entry.value.last_op) > options.lastCheckpoint.checkpoint) {
        updatedDataBuckets.add(decodeURIComponent(entry.keyText.split('/').at(-1)!));
        if (updatedDataBuckets.size > 1000) {
          return {
            updatedDataBuckets: new Set(),
            invalidateDataBuckets: true,
            updatedParameterLookups: new Set(),
            invalidateParameterBuckets: false
          };
        }
      }
    }
    return {
      updatedDataBuckets,
      invalidateDataBuckets: false,
      updatedParameterLookups: new Set(),
      invalidateParameterBuckets: false
    };
  }

  async *watchCheckpointChanges(
    options: storage.WatchWriteCheckpointOptions
  ): AsyncIterable<storage.StorageCheckpointUpdate> {
    let last: bigint | null = null;
    while (!options.signal.aborted) {
      const base = await this.getCheckpoint();
      if (last == null || base.checkpoint > last) {
        const previous = last == null ? null : new SlateDBReplicationCheckpoint(last, null);
        last = base.checkpoint;
        yield {
          base,
          writeCheckpoint: null,
          update:
            previous == null
              ? storage.CHECKPOINT_INVALIDATE_ALL
              : await this.getCheckpointChanges({ lastCheckpoint: previous, nextCheckpoint: base })
        };
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }

  async *getBucketDataBatch(
    checkpoint: storage.ReplicationCheckpoint,
    dataBuckets: storage.BucketDataRequest[],
    options: storage.BucketDataBatchOptions = {}
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    const limit = options.limit ?? 1000;
    const chunkLimitBytes = options.chunkLimitBytes ?? storage.DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES;
    let emitted = 0;
    for (const request of dataBuckets) {
      let start = request.start;
      while (emitted < limit) {
        const chunk: utils.SyncBucketData = {
          bucket: request.bucket,
          after: utils.internalToExternalOpId(start),
          next_after: utils.internalToExternalOpId(start),
          has_more: false,
          data: []
        };
        let chunkSizeBytes = 0;

        for await (const entry of this.store.scanPrefix<BucketOpRecord>(
          storagePrefix('bucket-data', this.replicationStreamId, request.bucket)
        )) {
          const op = BigInt(entry.value.op_id_bigint);
          if (op <= start || op > checkpoint.checkpoint) {
            continue;
          }
          const entrySizeBytes = entry.value.data?.length ?? 0;
          if (chunk.data.length > 0 && chunkSizeBytes + entrySizeBytes > chunkLimitBytes) {
            chunk.has_more = true;
            break;
          }
          chunk.data.push({
            op_id: entry.value.op_id,
            op: entry.value.op,
            object_type: entry.value.object_type,
            object_id: entry.value.object_id,
            data: entry.value.data === undefined ? undefined : entry.value.data,
            checksum: entry.value.checksum,
            subkey: entry.value.subkey
          });
          chunk.next_after = entry.value.op_id;
          chunkSizeBytes += entrySizeBytes;
          emitted++;
          if (emitted >= limit) {
            chunk.has_more = true;
            break;
          }
        }

        if (chunk.data.length == 0 && chunk.next_after == utils.internalToExternalOpId(start)) {
          break;
        }
        yield { chunkData: chunk, targetOp: null };
        if (!chunk.has_more) {
          break;
        }
        if (emitted >= limit) {
          return;
        }
        start = BigInt(chunk.next_after);
      }
    }
  }

  async getChecksums(
    checkpoint: storage.ReplicationCheckpoint,
    buckets: storage.BucketChecksumRequest[]
  ): Promise<utils.ChecksumMap> {
    const result: utils.ChecksumMap = new Map();
    for (const request of buckets) {
      let checksum = 0;
      let count = 0;
      for await (const entry of this.store.scanPrefix<BucketOpRecord>(
        storagePrefix('bucket-data', this.replicationStreamId, request.bucket)
      )) {
        if (BigInt(entry.value.op_id_bigint) <= checkpoint.checkpoint) {
          checksum = utils.addChecksums(checksum, entry.value.checksum);
          count++;
        }
      }
      result.set(request.bucket, { bucket: request.bucket, checksum, count });
    }
    return result;
  }

  clearChecksumCache(): void {}

  async getRecord(): Promise<SlateDBReplicationStreamRecord> {
    const record = await getReplicationStreamRecord(this.store, this.replicationStreamId);
    if (record == null) {
      throw new Error(`SlateDB replication stream ${this.replicationStreamId} not found`);
    }
    return record;
  }

  async updateRecord(patch: Partial<SlateDBReplicationStreamRecord>): Promise<SlateDBReplicationStreamRecord> {
    const current = await this.getRecord();
    const next = { ...current, ...patch };
    if (patch.state != null) {
      next.syncConfig = { ...next.syncConfig, state: patch.state };
    }
    await putReplicationStreamRecord(this.store, next);
    return next;
  }
}

class SlateDBReplicationCheckpoint implements storage.ReplicationCheckpoint {
  constructor(
    readonly checkpoint: bigint,
    readonly lsn: string | null
  ) {}

  async getParameterSets(): Promise<[]> {
    return [];
  }
}

class SlateDBBucketBatch
  extends BaseObserver<storage.BucketBatchStorageListener>
  implements storage.BucketStorageBatch
{
  last_flushed_op: bigint | null = null;
  resumeFromLsn: string | null;
  readonly skipExistingRows: boolean;
  private pending: storage.SaveOptions[] = [];
  private parsed: HydratedSyncConfig;
  private pendingWrites: SlateDBWriteOperation[] = [];
  private pendingValues = new Map<string, unknown | typeof DELETED>();
  private nextOpId: bigint | undefined;

  constructor(
    private readonly storage: SlateDBSyncBucketStorage,
    private readonly options: storage.CreateWriterOptions,
    record: SlateDBReplicationStreamRecord
  ) {
    super();
    this.resumeFromLsn = record.resume_lsn ?? record.last_checkpoint_lsn ?? options.zeroLSN;
    this.skipExistingRows = options.skipExistingRows ?? false;
    this.parsed = storage.getParsedSyncRules(options);
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.clearListeners();
  }

  async dispose(): Promise<void> {
    await this[Symbol.asyncDispose]();
  }

  async save(record: storage.SaveOptions): Promise<storage.FlushedResult | null> {
    this.pending.push(record);
    return null;
  }

  async truncate(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    for (const table of sourceTables) {
      for await (const entry of this.storage.store.scanPrefix<CurrentDataRecord>(
        storagePrefix('current-data', this.storage.replicationStreamId, table.id.toString())
      )) {
        await this.removeCurrentBuckets(entry.value.buckets);
        await this.storage.store.delete(entry.key);
      }
    }
    return this.last_flushed_op == null ? null : { flushed_op: this.last_flushed_op };
  }

  async drop(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    await this.truncate(sourceTables);
    for (const table of sourceTables) {
      await this.storage.store.delete(sourceTableKey(this.storage.replicationStreamId, table.id.toString()));
    }
    return this.last_flushed_op == null ? null : { flushed_op: this.last_flushed_op };
  }

  async flush(): Promise<storage.FlushedResult | null> {
    if (this.pending.length == 0) {
      return null;
    }
    if (!this.options.storeCurrentData) {
      return this.flushWithoutCurrentData();
    }
    for (const record of this.pending) {
      await this.persistRecord(record);
    }
    this.pending = [];
    await this.flushPendingWrites();
    return this.currentFlushResult();
  }

  async commit(lsn: string, options: storage.BucketBatchCommitOptions = {}): Promise<storage.CheckpointResult> {
    const flushed = await this.flush();
    const createEmpty = options.createEmptyCheckpoints ?? true;
    const record = await this.storage.getRecord();
    const persistedOp = this.last_flushed_op ?? BigInt(record.last_persisted_op ?? '0');
    const lastCheckpoint = BigInt(record.last_checkpoint_op ?? '0');
    const keepaliveOp = record.keepalive_op == null ? null : BigInt(record.keepalive_op);
    const canCheckpoint =
      record.snapshot_done === true &&
      (record.last_checkpoint_lsn == null || record.last_checkpoint_lsn <= lsn) &&
      (record.no_checkpoint_before_lsn == null || record.no_checkpoint_before_lsn <= lsn);

    if (!canCheckpoint) {
      await this.storage.updateRecord({
        resume_lsn: lsn,
        keepalive_op: maxOpId(keepaliveOp, persistedOp).toString(),
        last_keepalive_ts: new Date().toISOString()
      });
      return { checkpointBlocked: true, checkpointCreated: false };
    }

    const checkpoint = maxOpId(lastCheckpoint, persistedOp, keepaliveOp);
    const activatesStream = record.state == storage.SyncRuleState.PROCESSING;
    const checkpointCreated = createEmpty || checkpoint != lastCheckpoint || activatesStream;
    const update: Partial<SlateDBReplicationStreamRecord> = {
      state: activatesStream ? storage.SyncRuleState.ACTIVE : record.state,
      resume_lsn: lsn,
      snapshot_done: true,
      last_keepalive_ts: new Date().toISOString(),
      last_fatal_error: null,
      last_fatal_error_ts: null,
      keepalive_op: null
    };
    if (checkpointCreated) {
      update.last_checkpoint_op = checkpoint.toString();
      update.last_checkpoint_lsn = lsn;
      update.last_checkpoint_ts = new Date().toISOString();
    }
    await this.storage.updateRecord(update);
    return { checkpointBlocked: false, checkpointCreated };
  }

  async keepalive(lsn: string): Promise<storage.CheckpointResult> {
    return this.commit(lsn, { createEmptyCheckpoints: true });
  }

  async setResumeLsn(lsn: string): Promise<void> {
    this.resumeFromLsn = lsn;
    await this.storage.updateRecord({ resume_lsn: lsn });
  }

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    for (const table of tables) {
      table.snapshotComplete = true;
      await this.saveSourceTable(table);
    }
    if (no_checkpoint_before_lsn != null) {
      await this.storage.updateRecord({ no_checkpoint_before_lsn });
    }
    return tables;
  }

  async markTableSnapshotRequired(table: storage.SourceTable): Promise<void> {
    table.snapshotComplete = false;
    await this.saveSourceTable(table);
    await this.storage.updateRecord({ snapshot_done: false });
  }

  async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    this.resumeFromLsn = this.resumeFromLsn ?? no_checkpoint_before_lsn;
    await this.storage.updateRecord({
      snapshot_done: true,
      no_checkpoint_before_lsn,
      resume_lsn: this.resumeFromLsn
    });
  }

  async markSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    await this.markAllSnapshotDone(no_checkpoint_before_lsn);
  }

  async updateTableProgress(
    table: storage.SourceTable,
    progress: Partial<storage.TableSnapshotStatus>
  ): Promise<storage.SourceTable> {
    table.snapshotStatus = {
      ...(table.snapshotStatus ?? { totalEstimatedCount: -1, replicatedCount: 0, lastKey: null }),
      ...progress
    };
    await this.saveSourceTable(table);
    return table;
  }

  async getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null> {
    const record = await this.storage.store.get<SourceTableRecord>(
      sourceTableKey(this.storage.replicationStreamId, table.id.toString())
    );
    return record == null ? null : this.sourceTableFromRecord(record);
  }

  async resolveTables(options: storage.ResolveTablesOptions): Promise<storage.ResolveTablesResult> {
    const source = options.source;
    const id = String(
      options.idGenerator?.() ?? `${options.connection_id}:${source.schema}:${source.name}:${source.objectId ?? ''}`
    );
    const key = sourceTableKey(this.storage.replicationStreamId, id);
    let record = await this.storage.store.get<SourceTableRecord>(key);
    if (record == null) {
      record = {
        id,
        connection_id: options.connection_id,
        schema: source.schema,
        name: source.name,
        objectId: source.objectId,
        connectionTag: source.connectionTag,
        replicaIdColumns: source.replicaIdColumns,
        snapshotComplete: false
      };
      await this.storage.store.put(key, record);
    }
    return { tables: [this.sourceTableFromRecord(record)], dropTables: [] };
  }

  addCustomWriteCheckpoint(): void {
    // Ignored for the POC.
  }

  private async flushWithoutCurrentData(): Promise<storage.FlushedResult | null> {
    const bucketOps: Array<Omit<BucketOpRecord, 'op_id' | 'op_id_bigint'>> = [];
    for (const record of this.pending) {
      if (record.tag == storage.SaveOperationTag.DELETE) {
        continue;
      }
      const { results } = this.parsed.evaluateRowWithErrors({
        record: record.after as any,
        sourceTable: record.sourceTable.ref,
        bucketDataSources: record.sourceTable.bucketDataSources
      });
      for (const row of results) {
        const data = JSONBig.stringify(row.data);
        bucketOps.push({
          bucket: row.bucket,
          op: 'PUT',
          object_type: row.table,
          object_id: row.id,
          data,
          checksum: utils.hashData(row.table, row.id, data)
        });
      }
    }

    this.pending = [];
    if (bucketOps.length == 0) {
      return null;
    }

    const firstOp = await this.reserveOps(bucketOps.length);
    const writes: SlateDBWriteOperation[] = [];
    const bucketStates = new Map<string, string>();
    for (const [index, operation] of bucketOps.entries()) {
      const op = firstOp + BigInt(index);
      const record: BucketOpRecord = {
        ...operation,
        op_id: utils.internalToExternalOpId(op),
        op_id_bigint: op.toString()
      };
      writes.push({
        type: 'put',
        key: bucketDataKey(this.storage.replicationStreamId, operation.bucket, op),
        value: record
      });
      bucketStates.set(operation.bucket, op.toString());
      this.last_flushed_op = op;
    }
    for (const [bucket, last_op] of bucketStates) {
      writes.push({
        type: 'put',
        key: storageKey('bucket-state', this.storage.replicationStreamId, bucket),
        value: { bucket, last_op }
      });
    }
    await this.storage.store.write(writes);
    return this.last_flushed_op == null ? null : { flushed_op: this.last_flushed_op };
  }

  private async persistRecord(record: storage.SaveOptions): Promise<void> {
    const sourceKey = sourceRecordId(record);
    const currentKey = currentDataKey(this.storage.replicationStreamId, record.sourceTable.id.toString(), sourceKey);
    const existing = await this.getPendingAware<CurrentDataRecord>(currentKey);
    const previousKey = previousCurrentDataKey(this.storage.replicationStreamId, record);
    const previous =
      previousKey == null || previousKey == currentKey
        ? undefined
        : await this.getPendingAware<CurrentDataRecord>(previousKey);
    let nextBuckets: CurrentBucket[] = [];
    let nextData: Record<string, unknown> | null = null;
    let nextBucketOps: Array<Omit<BucketOpRecord, 'op_id' | 'op_id_bigint'>> = [];

    if (record.tag != storage.SaveOperationTag.DELETE) {
      nextData = { ...(previous?.data ?? existing?.data ?? {}), ...record.after };
      const { results } = this.parsed.evaluateRowWithErrors({
        record: nextData as any,
        sourceTable: record.sourceTable.ref,
        bucketDataSources: record.sourceTable.bucketDataSources
      });
      const subkey = replicaIdToSubkey(record.sourceTable.id, record.afterReplicaId);
      nextBuckets = results.map((row) => ({ bucket: row.bucket, table: row.table, id: row.id, subkey }));
      nextBucketOps = results.map((row) => {
        const data = JSONBig.stringify(row.data);
        return {
          bucket: row.bucket,
          op: 'PUT',
          object_type: row.table,
          object_id: row.id,
          data,
          checksum: utils.hashData(row.table, row.id, data),
          subkey
        };
      });
    }

    const remaining = new Map(
      [...(existing?.buckets ?? []), ...(previous?.buckets ?? [])].map((bucket) => [bucketKey(bucket), bucket])
    );
    for (const bucket of nextBuckets) {
      remaining.delete(bucketKey(bucket));
    }
    await this.removeCurrentBuckets([...remaining.values()]);
    for (const operation of nextBucketOps) {
      await this.putBucketOp(operation);
    }

    if (record.tag == storage.SaveOperationTag.DELETE) {
      this.deletePending(currentKey);
    } else {
      if (previousKey != null && previousKey != currentKey) {
        this.deletePending(previousKey);
      }
      this.putPending(currentKey, { data: nextData, buckets: nextBuckets } satisfies CurrentDataRecord);
    }
  }

  private async currentFlushResult(): Promise<storage.FlushedResult> {
    if (this.last_flushed_op != null) {
      return { flushed_op: this.last_flushed_op };
    }
    const record = await this.storage.getRecord();
    return { flushed_op: BigInt(record.last_persisted_op ?? '0') };
  }

  private async removeCurrentBuckets(buckets: CurrentBucket[]): Promise<void> {
    for (const bucket of buckets) {
      await this.putBucketOp({
        bucket: bucket.bucket,
        op: 'REMOVE',
        object_type: bucket.table,
        object_id: bucket.id,
        data: null,
        checksum: utils.hashDelete(bucket.subkey),
        subkey: bucket.subkey
      });
    }
  }

  private async putBucketOp(operation: Omit<BucketOpRecord, 'op_id' | 'op_id_bigint'>): Promise<void> {
    const op = await this.nextOp();
    const opId = utils.internalToExternalOpId(op);
    const record: BucketOpRecord = { ...operation, op_id: opId, op_id_bigint: op.toString() };
    this.putPending(bucketDataKey(this.storage.replicationStreamId, operation.bucket, op), record);
    this.putPending(storageKey('bucket-state', this.storage.replicationStreamId, operation.bucket), {
      bucket: operation.bucket,
      last_op: op.toString()
    });
    this.last_flushed_op = op;
  }

  private async nextOp(): Promise<bigint> {
    this.nextOpId ??= BigInt((await this.storage.getRecord()).next_op_id ?? '1');
    const next = this.nextOpId;
    this.nextOpId++;
    return next;
  }

  private async flushPendingWrites(): Promise<void> {
    if (this.pendingWrites.length == 0 && this.nextOpId == null) {
      return;
    }

    if (this.nextOpId != null) {
      const record = await this.storage.getRecord();
      this.putPending(replicationStreamKey(this.storage.replicationStreamId), {
        ...record,
        next_op_id: this.nextOpId.toString()
      } satisfies SlateDBReplicationStreamRecord);
    }

    await this.storage.store.write(this.pendingWrites);
    this.pendingWrites = [];
    this.pendingValues.clear();
    this.nextOpId = undefined;
  }

  private async getPendingAware<T>(key: string): Promise<T | undefined> {
    if (this.pendingValues.has(key)) {
      const value = this.pendingValues.get(key);
      return value == DELETED ? undefined : (value as T);
    }
    return this.storage.store.get<T>(key);
  }

  private putPending(key: string, value: unknown): void {
    this.pendingWrites.push({ type: 'put', key, value });
    this.pendingValues.set(key, value);
  }

  private deletePending(key: string): void {
    this.pendingWrites.push({ type: 'delete', key });
    this.pendingValues.set(key, DELETED);
  }

  private async reserveOps(count: number): Promise<bigint> {
    const record = await this.storage.getRecord();
    const next = BigInt(record.next_op_id ?? '1');
    await this.storage.updateRecord({ next_op_id: (next + BigInt(count)).toString() });
    return next;
  }

  private async saveSourceTable(table: storage.SourceTable): Promise<void> {
    await this.storage.store.put(sourceTableKey(this.storage.replicationStreamId, table.id.toString()), {
      id: table.id.toString(),
      connection_id: 0,
      schema: table.schema,
      name: table.name,
      objectId: table.objectId,
      connectionTag: table.ref.connectionTag,
      replicaIdColumns: table.replicaIdColumns,
      snapshotComplete: table.snapshotComplete,
      snapshotStatus: table.snapshotStatus
    } satisfies SourceTableRecord);
  }

  private sourceTableFromRecord(record: SourceTableRecord): storage.SourceTable {
    const ref = { connectionTag: record.connectionTag, schema: record.schema, name: record.name };
    const table = new storage.SourceTable({
      id: record.id,
      ref,
      objectId: record.objectId,
      replicaIdColumns: record.replicaIdColumns,
      snapshotComplete: record.snapshotComplete,
      ...this.parsed.getMatchingSources(ref)
    });
    table.snapshotStatus = record.snapshotStatus;
    table.syncData = table.bucketDataSources.length > 0;
    table.syncParameters = false;
    table.syncEvent = this.parsed.tableTriggersEvent(ref);
    return table;
  }
}

function sourceTableKey(streamId: number, tableId: string): string {
  return storageKey('source-table', streamId, tableId);
}

function currentDataKey(streamId: number, tableId: string, sourceKey: string): string {
  return storageKey('current-data', streamId, tableId, sourceKey);
}

function bucketDataKey(streamId: number, bucket: string, op: bigint): string {
  return storageKey('bucket-data', streamId, bucket, encodeOpId(op));
}

function sourceRecordId(record: storage.SaveOptions): string {
  const replicaId = record.tag == storage.SaveOperationTag.DELETE ? record.beforeReplicaId : record.afterReplicaId;
  return storage.serializeReplicaId(replicaId).toString('base64url');
}

function previousCurrentDataKey(streamId: number, record: storage.SaveOptions): string | null {
  if (record.tag != storage.SaveOperationTag.UPDATE || record.beforeReplicaId == null) {
    return null;
  }
  const sourceKey = storage.serializeReplicaId(record.beforeReplicaId).toString('base64url');
  return currentDataKey(streamId, record.sourceTable.id.toString(), sourceKey);
}

function maxOpId(...values: (bigint | null | undefined)[]): bigint {
  let max = 0n;
  for (const value of values) {
    if (value != null && value > max) {
      max = value;
    }
  }
  return max;
}

function replicaIdToSubkey(tableId: storage.SourceTableId, id: storage.ReplicaId): string {
  if (storage.isUUID(id)) {
    return `${tableId}/${id.toHexString()}`;
  }
  const repr = storage.serializeBson({ table: tableId, id });
  return uuidv5(repr, utils.ID_NAMESPACE);
}

function bucketKey(bucket: CurrentBucket): string {
  return `${bucket.bucket}/${bucket.table}/${bucket.id}/${bucket.subkey}`;
}
