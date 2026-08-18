import { DO_NOT_LOG } from '@powersync/lib-services-framework';
import { GetIntanceOptions, storage } from '@powersync/service-core';
import crypto from 'crypto';
import { desc, eq, inArray } from 'drizzle-orm';
import * as uuid from 'uuid';
import type { BucketDataRow, BucketParametersRow, CurrentDataRow, SyncRulesRow } from '../drivers/sqlite/schema.js';
import type { SqliteDrizzleRuntime } from '../drivers/sqlite/sqlite-config.js';
import { DrizzlePersistedReplicationStream } from './DrizzlePersistedReplicationStream.js';
import type { DrizzleCheckpointWatcher, DrizzleStorageDialect } from './DrizzleStorageDialect.js';
import { DrizzleSyncRulesStorage } from './DrizzleSyncRulesStorage.js';

export interface DrizzleBucketStorageFactoryOptions {
  runtime: SqliteDrizzleRuntime;
  dialect: DrizzleStorageDialect;
  slotNamePrefix: string;
}

export class DrizzleBucketStorageFactory extends storage.BucketStorageFactory {
  [DO_NOT_LOG] = true;

  readonly runtime: SqliteDrizzleRuntime;
  readonly dialect: DrizzleStorageDialect;
  readonly slotNamePrefix: string;
  readonly checkpointWatcher: DrizzleCheckpointWatcher;

  constructor(options: DrizzleBucketStorageFactoryOptions) {
    super();
    this.runtime = options.runtime;
    this.dialect = options.dialect;
    this.slotNamePrefix = options.slotNamePrefix;
    this.checkpointWatcher = options.dialect.createCheckpointWatcher();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.runtime.close();
  }

  getInstance(
    replicationStream: storage.PersistedReplicationStream,
    options?: GetIntanceOptions
  ): storage.SyncRulesBucketStorage {
    const syncRuleStorage = new DrizzleSyncRulesStorage({
      factory: this,
      dialect: this.dialect,
      replicationStream
    });

    if (!options?.skipLifecycleHooks) {
      this.iterateListeners((cb) => cb.syncStorageCreated?.(syncRuleStorage));
    }

    syncRuleStorage.registerListener({
      batchStarted: (batch) => {
        batch.registerListener({
          replicationEvent: (payload) => this.iterateListeners((cb) => cb.replicationEvent?.(payload))
        });
      }
    });

    return syncRuleStorage;
  }

  async updateSyncRules(options: storage.UpdateSyncRulesOptions): Promise<DrizzlePersistedReplicationStream> {
    const storageVersion =
      options.storageVersion ?? options.config.parsed.config.storageVersion ?? storage.CURRENT_STORAGE_VERSION;
    if (storage.STORAGE_VERSION_CONFIG[storageVersion] == null) {
      throw new Error(`Unsupported storage version ${storageVersion}`);
    }

    const tables = this.dialect.tables;
    const row = this.dialect.transaction((tx) => {
      tx.update(tables.syncRules)
        .set({ state: storage.SyncRuleState.STOP })
        .where(eq(tables.syncRules.state, storage.SyncRuleState.PROCESSING))
        .run();

      return tx
        .insert(tables.syncRules)
        .values({
          state: storage.SyncRuleState.PROCESSING,
          snapshotDone: false,
          snapshotLsn: null,
          lastCheckpoint: null,
          lastCheckpointLsn: null,
          noCheckpointBefore: null,
          slotName: this.generateReplicationStreamName(),
          lastCheckpointTs: null,
          lastKeepaliveTs: null,
          lastFatalError: null,
          lastFatalErrorTs: null,
          keepaliveOp: null,
          storageVersion,
          content: options.config.yaml,
          syncPlan: options.config.plan
        })
        .returning()
        .get();
    });

    return new DrizzlePersistedReplicationStream(this.dialect, row);
  }

  async restartReplication(replicationStreamId: number): Promise<void> {
    const active = await this.getActiveSyncConfig();
    const deploying = await this.getDeployingSyncConfig();
    const stream =
      deploying?.replicationStream.replicationStreamId == replicationStreamId
        ? deploying
        : active?.replicationStream.replicationStreamId == replicationStreamId
          ? active
          : null;
    if (stream != null) {
      await this.updateSyncRules(stream.content.asUpdateOptions());
    }
  }

  async getActiveSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    return this.getSyncConfigForStates([storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED]);
  }

  async getDeployingSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    return this.getSyncConfigForStates([storage.SyncRuleState.PROCESSING]);
  }

  async getReplicatingReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    return this.findSyncRulesRows([storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE]).map(
      (row) => new DrizzlePersistedReplicationStream(this.dialect, row)
    );
  }

  async getStoppedReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    return this.findSyncRulesRows([storage.SyncRuleState.STOP]).map(
      (row) => new DrizzlePersistedReplicationStream(this.dialect, row)
    );
  }

  async getStorageMetrics(): Promise<storage.StorageMetrics> {
    const { db, tables } = this.dialect;
    const operations = db.select().from(tables.bucketData).all();
    const parameters = db.select().from(tables.bucketParameters).all();
    const currentData = db.select().from(tables.currentData).all();
    return {
      operations_size_bytes: operations.reduce((total, row) => total + estimateBucketDataSize(row), 0),
      parameters_size_bytes: parameters.reduce((total, row) => total + estimateBucketParametersSize(row), 0),
      replication_size_bytes: currentData.reduce((total, row) => total + estimateCurrentDataSize(row), 0)
    };
  }

  async getPowerSyncInstanceId(): Promise<string> {
    const { db, tables } = this.dialect;
    const existing = db.select().from(tables.instance).limit(1).get();
    if (existing != null) {
      return existing.id;
    }
    const id = uuid.v4();
    db.insert(tables.instance).values({ id }).onConflictDoNothing().run();
    return db.select().from(tables.instance).limit(1).get()!.id;
  }

  async getSystemIdentifier(): Promise<storage.BucketStorageSystemIdentifier> {
    return {
      id: `${this.dialect.type}:${await this.getPowerSyncInstanceId()}`,
      type: this.dialect.type
    };
  }

  private async getSyncConfigForStates(states: storage.SyncRuleState[]): Promise<storage.ResolvedSyncConfig | null> {
    const [row] = this.findSyncRulesRows(states);
    if (row == null) {
      return null;
    }
    const replicationStream = new DrizzlePersistedReplicationStream(this.dialect, row);
    return {
      content: replicationStream.syncConfigContent[0],
      replicationStream,
      storage: this.getInstance(replicationStream, { skipLifecycleHooks: true })
    };
  }

  private findSyncRulesRows(states: storage.SyncRuleState[]): SyncRulesRow[] {
    const { db, tables } = this.dialect;
    if (states.length == 0) {
      return [];
    }
    return db
      .select()
      .from(tables.syncRules)
      .where(inArray(tables.syncRules.state, states))
      .orderBy(desc(tables.syncRules.id))
      .all();
  }

  private generateReplicationStreamName(): string {
    return `${this.slotNamePrefix}${Date.now()}_${crypto.randomBytes(2).toString('hex')}`;
  }
}

function estimateString(value: string | null | undefined): number {
  return value == null ? 0 : value.length;
}
function estimateBuffer(value: Buffer | Uint8Array | null | undefined): number {
  return value == null ? 0 : value.byteLength;
}
function estimateJson(value: unknown): number {
  return typeof value == 'string' ? value.length : JSON.stringify(value ?? null).length;
}
function estimateBucketDataSize(row: BucketDataRow): number {
  return (
    80 +
    estimateString(row.id) +
    estimateString(row.bucketName) +
    estimateString(row.op) +
    estimateString(row.sourceTable) +
    estimateBuffer(row.sourceKey) +
    estimateString(row.tableName) +
    estimateString(row.rowId) +
    estimateString(row.data)
  );
}
function estimateBucketParametersSize(row: BucketParametersRow): number {
  return (
    80 +
    estimateString(row.sourceTable) +
    estimateBuffer(row.sourceKey) +
    estimateBuffer(row.lookup) +
    estimateJson(row.bucketParameters)
  );
}
function estimateCurrentDataSize(row: CurrentDataRow): number {
  return (
    80 +
    estimateString(row.id) +
    estimateString(row.sourceTable) +
    estimateBuffer(row.sourceKey) +
    estimateJson(row.buckets) +
    estimateJson(row.lookups) +
    estimateBuffer(row.data)
  );
}
