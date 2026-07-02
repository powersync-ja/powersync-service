import { storage } from '@powersync/service-core';

import { SlateDBKVStore, storageKey, storagePrefix } from './SlateDBKVStore.js';

export interface SlateDBReplicationStreamRecord {
  replicationStreamId: number;
  replicationStreamName: string;
  state: storage.SyncRuleState;
  storageVersion: number;
  syncConfig: SlateDBSyncConfigRecord;
  snapshot_done?: boolean;
  last_checkpoint_lsn: string | null;
  last_fatal_error?: string | null;
  last_fatal_error_ts?: string | null;
  last_keepalive_ts?: string | null;
  last_checkpoint_ts?: string | null;
}

export interface SlateDBSyncConfigRecord {
  id: string;
  content: string;
  compiledPlan: storage.SerializedSyncPlan | null;
  state: storage.SyncRuleState;
}

export class SlateDBPersistedSyncConfigContent extends storage.PersistedSyncConfigContent {
  constructor(
    private readonly store: SlateDBKVStore,
    private readonly record: SlateDBReplicationStreamRecord
  ) {
    super({
      replicationStreamId: record.replicationStreamId,
      sync_rules_content: record.syncConfig.content,
      compiled_plan: record.syncConfig.compiledPlan,
      replicationStreamName: record.replicationStreamName,
      storageVersion: record.storageVersion,
      syncConfigId: record.syncConfig.id,
      syncConfigState: record.syncConfig.state
    });
  }

  async getSyncConfigStatus(): Promise<storage.PersistedSyncConfigStatus | null> {
    const record = await getReplicationStreamRecord(this.store, this.replicationStreamId);
    return record == null ? null : syncConfigStatusFromRecord(record);
  }
}

export class SlateDBPersistedReplicationStream extends storage.PersistedReplicationStream {
  current_lock: storage.ReplicationLock | null = null;
  readonly syncConfigContent: readonly SlateDBPersistedSyncConfigContent[];

  constructor(
    private readonly store: SlateDBKVStore,
    readonly record: SlateDBReplicationStreamRecord
  ) {
    super({
      replicationStreamId: record.replicationStreamId,
      replicationStreamName: record.replicationStreamName,
      state: record.state,
      storageVersion: record.storageVersion
    });
    this.syncConfigContent = [new SlateDBPersistedSyncConfigContent(store, record)];
  }

  parsed(options: storage.ParseSyncConfigOptions): storage.ParsedSyncConfigSet {
    return this.syncConfigContent[0].parsed(options);
  }

  async lock(): Promise<storage.ReplicationLock> {
    return (this.current_lock = {
      sync_rules_id: this.replicationStreamId,
      release: async () => {
        this.current_lock = null;
      }
    });
  }
}

export function replicationStreamKey(replicationStreamId: number): string {
  return storageKey('meta', 'stream', replicationStreamId, 'state');
}

export function replicationStreamsPrefix(): string {
  return storagePrefix('meta', 'stream');
}

export async function getReplicationStreamRecord(
  store: SlateDBKVStore,
  replicationStreamId: number
): Promise<SlateDBReplicationStreamRecord | undefined> {
  return store.get<SlateDBReplicationStreamRecord>(replicationStreamKey(replicationStreamId));
}

export async function putReplicationStreamRecord(
  store: SlateDBKVStore,
  record: SlateDBReplicationStreamRecord
): Promise<void> {
  await store.put(replicationStreamKey(record.replicationStreamId), record);
}

export async function listReplicationStreamRecords(store: SlateDBKVStore): Promise<SlateDBReplicationStreamRecord[]> {
  const records: SlateDBReplicationStreamRecord[] = [];
  for await (const entry of store.scanPrefix<SlateDBReplicationStreamRecord>(replicationStreamsPrefix())) {
    if (entry.keyText.endsWith('/state')) {
      records.push(entry.value);
    }
  }
  records.sort((a, b) => a.replicationStreamId - b.replicationStreamId);
  return records;
}

export function syncConfigStatusFromRecord(record: SlateDBReplicationStreamRecord): storage.PersistedSyncConfigStatus {
  return {
    id: record.syncConfig.id,
    replicationStreamId: record.replicationStreamId,
    state: record.syncConfig.state,
    snapshot_done: record.snapshot_done,
    last_checkpoint_lsn: record.last_checkpoint_lsn,
    last_fatal_error: record.last_fatal_error,
    last_fatal_error_ts: dateOrNull(record.last_fatal_error_ts),
    last_keepalive_ts: dateOrNull(record.last_keepalive_ts),
    last_checkpoint_ts: dateOrNull(record.last_checkpoint_ts)
  };
}

function dateOrNull(value: string | null | undefined): Date | null {
  return value == null ? null : new Date(value);
}
