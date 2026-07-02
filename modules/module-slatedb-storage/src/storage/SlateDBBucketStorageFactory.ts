import { framework, storage } from '@powersync/service-core';
import { v5 as uuidv5 } from 'uuid';

import { SlateDBStorageConfigDecoded } from '../types/types.js';
import { SlateDBKVStore, storageKey } from './SlateDBKVStore.js';
import {
  getReplicationStreamRecord,
  listReplicationStreamRecords,
  putReplicationStreamRecord,
  SlateDBPersistedReplicationStream,
  SlateDBReplicationStreamRecord
} from './SlateDBPersistedSyncConfigContent.js';
import { SlateDBSyncBucketStorage } from './SlateDBSyncBucketStorage.js';

export type SlateDBBucketStorageOptions = {
  config: SlateDBStorageConfigDecoded;
  replicationStreamNamePrefix: string;
};

export class SlateDBBucketStorageFactory extends storage.BucketStorageFactory {
  [framework.DO_NOT_LOG] = true;

  readonly replicationStreamNamePrefix: string;
  private store: Promise<SlateDBKVStore> | undefined;
  private resolvedStore: SlateDBKVStore | undefined;
  private activeStorageCache: SlateDBSyncBucketStorage | undefined;

  constructor(readonly options: SlateDBBucketStorageOptions) {
    super();
    this.replicationStreamNamePrefix = options.replicationStreamNamePrefix;
  }

  async getKVStore(): Promise<SlateDBKVStore> {
    this.store ??= SlateDBKVStore.open({
      path: this.options.config.path
    });
    this.resolvedStore ??= await this.store;
    return this.resolvedStore;
  }

  getInstance(
    replicationStream: storage.PersistedReplicationStream,
    _options?: storage.GetIntanceOptions
  ): storage.SyncRulesBucketStorage {
    if (!(replicationStream instanceof SlateDBPersistedReplicationStream)) {
      throw new Error(`Expected SlateDBPersistedReplicationStream`);
    }
    if (this.activeStorageCache?.replicationStreamId == replicationStream.replicationStreamId) {
      return this.activeStorageCache;
    }
    const store = this.resolvedStore;
    if (store == null) {
      throw new Error(`SlateDB store has not been initialized`);
    }
    const instance = new SlateDBSyncBucketStorage(this, store, replicationStream);
    this.activeStorageCache = instance;
    return instance;
  }

  async updateSyncRules(_options: storage.UpdateSyncRulesOptions): Promise<storage.PersistedReplicationStream> {
    const store = await this.getKVStore();
    const options = _options;
    const storageVersion =
      options.storageVersion ?? options.config.parsed.config.storageVersion ?? storage.CURRENT_STORAGE_VERSION;
    if (storage.STORAGE_VERSION_CONFIG[storageVersion] == null) {
      throw new framework.ServiceError(
        framework.ErrorCode.PSYNC_S1005,
        `Unsupported storage version ${storageVersion}`
      );
    }

    const existing = await listReplicationStreamRecords(store);
    for (const record of existing) {
      if (record.state == storage.SyncRuleState.PROCESSING) {
        await putReplicationStreamRecord(store, {
          ...record,
          state: storage.SyncRuleState.STOP,
          syncConfig: {
            ...record.syncConfig,
            state: storage.SyncRuleState.STOP
          }
        });
      }
    }

    const replicationStreamId = await this.nextReplicationStreamId(store);
    const replicationStreamName = `${this.replicationStreamNamePrefix}${replicationStreamId}`;
    const record: SlateDBReplicationStreamRecord = {
      replicationStreamId,
      replicationStreamName,
      state: storage.SyncRuleState.PROCESSING,
      storageVersion,
      syncConfig: {
        id: String(replicationStreamId),
        content: options.config.yaml,
        compiledPlan: options.config.plan,
        state: storage.SyncRuleState.PROCESSING
      },
      snapshot_done: false,
      last_checkpoint_lsn: null
    };

    await putReplicationStreamRecord(store, record);
    return new SlateDBPersistedReplicationStream(store, record);
  }

  async restartReplication(_replicationStreamId: number): Promise<void> {
    const store = await this.getKVStore();
    const next = await this.getDeployingSyncConfig();
    const active = await this.getActiveSyncConfig();

    if (next != null && next.content.replicationStreamId == _replicationStreamId) {
      await this.updateSyncRules(next.content.asUpdateOptions());
      await this.updateStreamState(store, _replicationStreamId, storage.SyncRuleState.STOP, [
        storage.SyncRuleState.PROCESSING
      ]);
    } else if (next == null && active?.content.replicationStreamId == _replicationStreamId) {
      await this.updateSyncRules(active.content.asUpdateOptions());
      await this.updateStreamState(store, _replicationStreamId, storage.SyncRuleState.ERRORED, [
        storage.SyncRuleState.ACTIVE
      ]);
    } else if (next != null && active?.content.replicationStreamId == _replicationStreamId) {
      await this.updateStreamState(store, _replicationStreamId, storage.SyncRuleState.ERRORED, [
        storage.SyncRuleState.ACTIVE
      ]);
    }
  }

  async getActiveSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    const store = await this.getKVStore();
    const record = latestMatching(await listReplicationStreamRecords(store), [
      storage.SyncRuleState.ACTIVE,
      storage.SyncRuleState.ERRORED
    ]);
    return record == null ? null : this.resolvedSyncConfigFromRecord(store, record);
  }

  async getDeployingSyncConfig(): Promise<storage.ResolvedSyncConfig | null> {
    const store = await this.getKVStore();
    const record = latestMatching(await listReplicationStreamRecords(store), [storage.SyncRuleState.PROCESSING]);
    return record == null ? null : this.resolvedSyncConfigFromRecord(store, record);
  }

  async getReplicatingReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    const store = await this.getKVStore();
    return (await listReplicationStreamRecords(store))
      .filter(
        (record) => record.state == storage.SyncRuleState.ACTIVE || record.state == storage.SyncRuleState.PROCESSING
      )
      .map((record) => new SlateDBPersistedReplicationStream(store, record));
  }

  async getStoppedReplicationStreams(): Promise<storage.PersistedReplicationStream[]> {
    const store = await this.getKVStore();
    return (await listReplicationStreamRecords(store))
      .filter((record) => record.state == storage.SyncRuleState.STOP)
      .map((record) => new SlateDBPersistedReplicationStream(store, record));
  }

  async getStorageMetrics(): Promise<storage.StorageMetrics> {
    return {
      operations_size_bytes: 0,
      parameters_size_bytes: 0,
      replication_size_bytes: 0
    };
  }

  async getPowerSyncInstanceId(): Promise<string> {
    const store = await this.getKVStore();
    const key = storageKey('meta', 'instance-id');
    const existing = await store.get<string>(key);
    if (existing != null) {
      return existing;
    }

    const id = uuidv5(this.options.config.path, uuidv5.URL);
    await store.put(key, id);
    return id;
  }

  async getSystemIdentifier(): Promise<storage.BucketStorageSystemIdentifier> {
    return {
      id: this.options.config.path,
      type: 'slatedb'
    };
  }

  async [Symbol.asyncDispose](): Promise<void> {
    if (this.store != null) {
      const store = await this.store;
      await store[Symbol.asyncDispose]();
      this.store = undefined;
      this.resolvedStore = undefined;
    }
  }

  private async nextReplicationStreamId(store: SlateDBKVStore): Promise<number> {
    const key = storageKey('meta', 'replication-stream-sequence');
    const current = (await store.get<number>(key)) ?? 0;
    const next = current + 1;
    await store.put(key, next);
    return next;
  }

  private async updateStreamState(
    store: SlateDBKVStore,
    replicationStreamId: number,
    state: storage.SyncRuleState,
    expectedCurrentStates?: storage.SyncRuleState[]
  ): Promise<void> {
    const record = await getReplicationStreamRecord(store, replicationStreamId);
    if (record == null) {
      return;
    }
    if (expectedCurrentStates != null && !expectedCurrentStates.includes(record.state)) {
      return;
    }
    await putReplicationStreamRecord(store, {
      ...record,
      state,
      syncConfig: {
        ...record.syncConfig,
        state
      }
    });
  }

  private resolvedSyncConfigFromRecord(
    store: SlateDBKVStore,
    record: SlateDBReplicationStreamRecord
  ): storage.ResolvedSyncConfig {
    const stream = new SlateDBPersistedReplicationStream(store, record);
    const thisFactory = this;
    return {
      content: stream.syncConfigContent[0],
      replicationStream: stream,
      get storage() {
        return thisFactory.getInstance(stream);
      }
    };
  }
}

function notImplemented(method: string): Error {
  return new Error(`SlateDB bucket storage scaffold: ${method} is not implemented yet.`);
}

function latestMatching(
  records: SlateDBReplicationStreamRecord[],
  states: storage.SyncRuleState[]
): SlateDBReplicationStreamRecord | undefined {
  return records
    .filter((record) => states.includes(record.state))
    .sort((a, b) => b.replicationStreamId - a.replicationStreamId)[0];
}
