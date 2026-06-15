import { MikroORM } from '@mikro-orm/core';
import { DO_NOT_LOG } from '@powersync/lib-services-framework';
import { GetIntanceOptions, storage } from '@powersync/service-core';
import crypto from 'crypto';
import * as uuid from 'uuid';
import type { BucketData, BucketParameters, CurrentData, SyncRules } from '../entities/entities-index.js';
import { MikroOrmPersistedReplicationStream } from './MikroOrmPersistedReplicationStream.js';
import { MikroOrmCheckpointWatcher, MikroOrmStorageDialect } from './MikroOrmStorageDialect.js';
import { MikroOrmSyncRulesStorage } from './MikroOrmSyncRulesStorage.js';

export interface MikroOrmBucketStorageFactoryOptions {
  orm: MikroORM;
  dialect: MikroOrmStorageDialect;
  slotNamePrefix: string;
}

export class MikroOrmBucketStorageFactory extends storage.BucketStorageFactory {
  [DO_NOT_LOG] = true;

  readonly orm: MikroORM;
  readonly dialect: MikroOrmStorageDialect;
  readonly slotNamePrefix: string;
  readonly checkpointWatcher: MikroOrmCheckpointWatcher;

  constructor(options: MikroOrmBucketStorageFactoryOptions) {
    super();
    this.orm = options.orm;
    this.dialect = options.dialect;
    this.slotNamePrefix = options.slotNamePrefix;
    this.checkpointWatcher = options.dialect.createCheckpointWatcher();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.orm.close(true);
  }

  getInstance(
    replicationStream: storage.PersistedReplicationStream,
    options?: GetIntanceOptions
  ): storage.SyncRulesBucketStorage {
    const syncRuleStorage = new MikroOrmSyncRulesStorage({
      factory: this,
      orm: this.orm,
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

  async updateSyncRules(options: storage.UpdateSyncRulesOptions): Promise<MikroOrmPersistedReplicationStream> {
    const storageVersion =
      options.storageVersion ?? options.config.parsed.config.storageVersion ?? storage.CURRENT_STORAGE_VERSION;
    const storageConfig = storage.STORAGE_VERSION_CONFIG[storageVersion];
    if (storageConfig == null) {
      throw new Error(`Unsupported storage version ${storageVersion}`);
    }

    const em = this.orm.em.fork();
    let row: SyncRules;
    await em.transactional(async (transactionalEntityManager) => {
      const processingRows = await transactionalEntityManager.find(this.dialect.syncRulesEntity, {
        state: storage.SyncRuleState.PROCESSING
      });
      for (const processingRow of processingRows) {
        transactionalEntityManager.assign(processingRow, { state: storage.SyncRuleState.STOP });
      }

      const syncRules = transactionalEntityManager.create(this.dialect.syncRulesEntity, {
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
      });

      transactionalEntityManager.persist(syncRules);
      await transactionalEntityManager.flush();
      row = syncRules;
    });

    return new MikroOrmPersistedReplicationStream(this.orm, this.dialect, row!);
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

    if (stream == null) {
      return;
    }

    await this.updateSyncRules(stream.content.asUpdateOptions());
  }

  async getActiveSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    return this.getSyncConfigForStates([storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED]);
  }

  async getDeployingSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    return this.getSyncConfigForStates([storage.SyncRuleState.PROCESSING]);
  }

  async getReplicatingReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    const rows = await this.findSyncRulesRows([storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE]);
    return rows.map((row) => new MikroOrmPersistedReplicationStream(this.orm, this.dialect, row));
  }

  async getStoppedReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    const rows = await this.findSyncRulesRows([storage.SyncRuleState.STOP]);
    return rows.map((row) => new MikroOrmPersistedReplicationStream(this.orm, this.dialect, row));
  }

  async getStorageMetrics(): Promise<storage.StorageMetrics> {
    const em = this.orm.em.fork();
    const [operations, parameters, currentData] = await Promise.all([
      em.find(this.dialect.bucketDataEntity, {}),
      em.find(this.dialect.bucketParametersEntity, {}),
      em.find(this.dialect.currentDataEntity, {})
    ]);

    return {
      operations_size_bytes: operations.reduce((total, row) => total + estimateBucketDataSize(row), 0),
      parameters_size_bytes: parameters.reduce((total, row) => total + estimateBucketParametersSize(row), 0),
      replication_size_bytes: currentData.reduce((total, row) => total + estimateCurrentDataSize(row), 0)
    };
  }

  async getPowerSyncInstanceId(): Promise<string> {
    const em = this.orm.em.fork();
    const instanceEntity = this.dialect.instanceEntity;
    const [existingRow] = await em.find(instanceEntity, {}, { limit: 1 });
    let row = existingRow ?? null;
    if (row == null) {
      row = em.create(instanceEntity, { id: uuid.v4() });
      await em.persist(row).flush();
    }
    return row.id;
  }

  async getSystemIdentifier(): Promise<storage.BucketStorageSystemIdentifier> {
    return {
      id: `${this.dialect.type}:${await this.getPowerSyncInstanceId()}`,
      type: this.dialect.type
    };
  }

  private async getSyncConfigForStates(states: storage.SyncRuleState[]): Promise<storage.ResolvedSyncConfig | null> {
    const [row] = await this.findSyncRulesRows(states);
    if (row == null) {
      return null;
    }

    const replicationStream = new MikroOrmPersistedReplicationStream(this.orm, this.dialect, row);
    const storageInstance = this.getInstance(replicationStream, { skipLifecycleHooks: true });
    return {
      content: replicationStream.syncConfigContent[0],
      replicationStream,
      storage: storageInstance
    };
  }

  private async findSyncRulesRows(states: storage.SyncRuleState[]): Promise<SyncRules[]> {
    const em = this.orm.em.fork();
    return await em.find(this.dialect.syncRulesEntity, { state: { $in: states } }, { orderBy: { id: 'DESC' } });
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

function estimateBucketDataSize(row: BucketData): number {
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

function estimateBucketParametersSize(row: BucketParameters): number {
  return (
    80 +
    estimateString(row.sourceTable) +
    estimateBuffer(row.sourceKey) +
    estimateBuffer(row.lookup) +
    estimateJson(row.bucketParameters)
  );
}

function estimateCurrentDataSize(row: CurrentData): number {
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
