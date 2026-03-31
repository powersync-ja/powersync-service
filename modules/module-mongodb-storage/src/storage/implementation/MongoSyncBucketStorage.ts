import { HydratedSyncRules, ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import {
  GetCheckpointChangesOptions,
  PopulateChecksumCacheOptions,
  PopulateChecksumCacheResults,
  storage,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import * as bson from 'bson';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { VersionedPowerSyncMongo } from './db.js';
import { MongoChecksums } from './MongoChecksums.js';
import { MongoPersistedSyncRulesContent } from './MongoPersistedSyncRulesContent.js';
import { BaseMongoSyncBucketStorage, MongoSyncBucketStorageOptions } from './common/MongoSyncBucketStorageBase.js';
import { MongoSyncBucketStorageV1 } from './v1/MongoSyncBucketStorageV1.js';
import { MongoSyncBucketStorageV3 } from './v3/MongoSyncBucketStorageV3.js';

export { MongoSyncBucketStorageOptions } from './common/MongoSyncBucketStorageBase.js';

export class MongoSyncBucketStorage implements storage.SyncRulesBucketStorage {
  private readonly impl: BaseMongoSyncBucketStorage;

  constructor(
    factory: MongoBucketStorage,
    group_id: number,
    sync_rules: MongoPersistedSyncRulesContent,
    slot_name: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    if (sync_rules.getStorageConfig().incrementalReprocessing) {
      this.impl = new MongoSyncBucketStorageV3(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
    } else {
      this.impl = new MongoSyncBucketStorageV1(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
    }
  }

  get factory(): MongoBucketStorage {
    return this.impl.factory;
  }

  get group_id(): number {
    return this.impl.group_id;
  }

  get slot_name(): string {
    return this.impl.slot_name;
  }

  get db(): VersionedPowerSyncMongo {
    return this.impl.db;
  }

  get checksums(): MongoChecksums {
    return this.impl.checksums;
  }

  get mapping() {
    return this.impl.mapping;
  }

  get writeCheckpointMode() {
    return this.impl.writeCheckpointMode;
  }

  registerListener(listener: Partial<storage.SyncRulesBucketStorageListener>): () => void {
    return this.impl.registerListener(listener);
  }

  setWriteCheckpointMode(mode: storage.WriteCheckpointMode): void {
    this.impl.setWriteCheckpointMode(mode);
  }

  createManagedWriteCheckpoint(checkpoint: storage.ManagedWriteCheckpointOptions): Promise<bigint> {
    return this.impl.createManagedWriteCheckpoint(checkpoint);
  }

  lastWriteCheckpoint(filters: storage.SyncStorageLastWriteCheckpointFilters): Promise<bigint | null> {
    return this.impl.lastWriteCheckpoint(filters);
  }

  getParsedSyncRules(options: storage.ParseSyncRulesOptions): HydratedSyncRules {
    return this.impl.getParsedSyncRules(options);
  }

  getCheckpoint(): Promise<storage.ReplicationCheckpoint> {
    return this.impl.getCheckpoint();
  }

  getCheckpointInternal(): Promise<storage.ReplicationCheckpoint | null> {
    return this.impl.getCheckpointInternal();
  }

  createWriter(options: storage.CreateWriterOptions): Promise<storage.BucketStorageBatch> {
    return this.impl.createWriter(options);
  }

  startBatch(
    options: storage.CreateWriterOptions,
    callback: (batch: storage.BucketStorageBatch) => Promise<void>
  ): Promise<storage.FlushedResult | null> {
    return this.impl.startBatch(options, callback);
  }

  resolveTable(options: storage.ResolveTableOptions): Promise<storage.ResolveTableResult> {
    return this.impl.resolveTable(options);
  }

  getParameterSets(
    checkpoint: storage.ReplicationCheckpoint & { snapshotTime: bson.Timestamp },
    lookups: ScopedParameterLookup[]
  ): Promise<SqliteJsonRow[]> {
    return this.impl.getParameterSets(checkpoint as any, lookups);
  }

  getBucketDataBatch(
    checkpoint: utils.InternalOpId,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    return this.impl.getBucketDataBatch(checkpoint, dataBuckets, options);
  }

  getChecksums(checkpoint: utils.InternalOpId, buckets: storage.BucketChecksumRequest[]): Promise<utils.ChecksumMap> {
    return this.impl.getChecksums(checkpoint, buckets);
  }

  clearChecksumCache(): void {
    this.impl.clearChecksumCache();
  }

  terminate(options?: storage.TerminateOptions): Promise<void> {
    return this.impl.terminate(options);
  }

  getStatus(): Promise<storage.SyncRuleStatus> {
    return this.impl.getStatus();
  }

  clear(options?: storage.ClearStorageOptions): Promise<void> {
    return this.impl.clear(options);
  }

  reportError(e: any): Promise<void> {
    return this.impl.reportError(e);
  }

  compact(options?: storage.CompactOptions): Promise<void> {
    return this.impl.compact(options);
  }

  populatePersistentChecksumCache(options: PopulateChecksumCacheOptions): Promise<PopulateChecksumCacheResults> {
    return this.impl.populatePersistentChecksumCache(options);
  }

  watchCheckpointChanges(options: WatchWriteCheckpointOptions): AsyncIterable<storage.StorageCheckpointUpdate> {
    return this.impl.watchCheckpointChanges(options);
  }

  getCheckpointChanges(options: GetCheckpointChangesOptions): Promise<storage.CheckpointChanges> {
    return this.impl.getCheckpointChanges(options);
  }
}
